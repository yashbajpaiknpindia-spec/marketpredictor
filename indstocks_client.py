"""
INDstocks (IndMoney) API client.

Replaces yfinance as this app's India (NSE) market-data source.
Docs: https://api-docs.indstocks.com/

Scope: equity market data only (quotes + historical OHLCV + instrument
lookup). No order placement, no F&O/options — this app is India-equity-only.

Required environment variables:
    INDSTOCKS_API_KEY     - Client ID shown on the Access Tokens page (x-api-key)
    INDSTOCKS_MPIN        - Your INDstocks account MPIN
    INDSTOCKS_TOTP_SECRET - The raw TOTP secret shown once during Setup TOTP

Token lifecycle notes (see api-docs.indstocks.com/Users/):
    - Only ONE TOTP-generated token is live at a time; generating a new one
      invalidates the previous one. If you run more than one process (web +
      background worker), only one should be minting tokens — see
      get_access_token()'s docstring below.
    - A token is valid 24h. We cache it in-process and refresh a little early.
    - Minimum 60s between /generate/token calls; this client's cache means
      you will not hit that unless multiple processes race each other.
"""
import os
import io
import csv
import time
import threading
import datetime
import requests

BASE_URL = "https://api.indstocks.com"

API_KEY = os.environ.get("INDSTOCKS_API_KEY", "").strip()
MPIN = os.environ.get("INDSTOCKS_MPIN", "").strip()
TOTP_SECRET = os.environ.get("INDSTOCKS_TOTP_SECRET", "").strip()

REQUEST_TIMEOUT = float(os.environ.get("INDSTOCKS_REQUEST_TIMEOUT", "20"))

# Stay comfortably under INDstocks' documented 10 requests/second/endpoint limit
# when we fire off many historical-data calls back to back during a scan.
_MIN_GAP_SECONDS = float(os.environ.get("INDSTOCKS_MIN_REQUEST_GAP", "0.12"))
_last_request_lock = threading.Lock()
_last_request_at = [0.0]


class INDstocksError(Exception):
    pass


def _throttle():
    with _last_request_lock:
        wait = _last_request_at[0] + _MIN_GAP_SECONDS - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        _last_request_at[0] = time.monotonic()


def credentials_configured() -> bool:
    return bool(API_KEY and MPIN and TOTP_SECRET)


def _require_credentials():
    missing = [name for name, val in (
        ("INDSTOCKS_API_KEY", API_KEY),
        ("INDSTOCKS_MPIN", MPIN),
        ("INDSTOCKS_TOTP_SECRET", TOTP_SECRET),
    ) if not val]
    if missing:
        raise INDstocksError(
            f"Missing INDstocks credentials: {', '.join(missing)}. "
            "Set them in your environment (see indstocks_client.py docstring)."
        )


def _generate_totp_code() -> str:
    try:
        import pyotp
    except ImportError as e:
        raise INDstocksError("pyotp is required for TOTP token generation — add it to requirements.txt") from e
    return pyotp.TOTP(TOTP_SECRET).now()


# ---------------------------------------------------------------------------
# Access token — 24h cache, thread-safe within this process
# ---------------------------------------------------------------------------

_token_lock = threading.Lock()
_token_cache = {"token": None, "expires_at": 0.0}


def get_access_token(force_refresh: bool = False) -> str:
    """Return a cached access token, generating a fresh one via TOTP if needed.

    NOTE: if this app runs as more than one OS process (e.g. a Render web
    dyno plus a separate `background-scan` cron/worker process), each has
    its own in-memory cache. Since INDstocks allows only one live
    TOTP-generated token at a time, two processes generating independently
    will keep invalidating each other. Prefer running background-scan as
    part of the same process, or have one process own generation and share
    the token via the database if you split them.
    """
    with _token_lock:
        now = time.time()
        if not force_refresh and _token_cache["token"] and now < _token_cache["expires_at"]:
            return _token_cache["token"]
        _require_credentials()
        _throttle()
        resp = requests.post(
            f"{BASE_URL}/generate/token",
            headers={"x-api-key": API_KEY, "Content-Type": "application/json"},
            json={"mpin": MPIN, "totp": _generate_totp_code()},
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code != 200:
            raise INDstocksError(f"Token generation failed ({resp.status_code}): {resp.text[:300]}")
        payload = resp.json()
        token = payload.get("token") or (payload.get("data") or {}).get("token")
        if not token:
            raise INDstocksError(f"Token generation response missing 'token': {payload}")
        _token_cache["token"] = token
        # Real expiry is 24h; refresh an hour early to be safe.
        _token_cache["expires_at"] = now + 23 * 3600
        return token


# A 429 is transient by nature -- the old behavior (raise immediately, no retry)
# meant one rate-limit hit permanently dropped that scrip/window's data for the
# whole run, which is silently indistinguishable downstream from "this stock
# genuinely has no data" (see market_data.py's Ticker.history() docstring).
# Retrying with backoff means a transient 429 usually recovers instead of quietly
# shrinking the candidate universe.
MAX_RATE_LIMIT_RETRIES = int(os.environ.get("INDSTOCKS_MAX_RATE_LIMIT_RETRIES", "3"))


def _get(path: str, params: dict = None, _retried: bool = False, _rate_retries: int = 0):
    _throttle()
    resp = requests.get(
        f"{BASE_URL}{path}",
        headers={"Authorization": get_access_token()},
        params=params or {},
        timeout=REQUEST_TIMEOUT,
    )
    if resp.status_code == 403 and not _retried:
        # Token may have been replaced/revoked out from under this process — refresh once.
        get_access_token(force_refresh=True)
        return _get(path, params=params, _retried=True, _rate_retries=_rate_retries)
    if resp.status_code == 429 and _rate_retries < MAX_RATE_LIMIT_RETRIES:
        retry_after = resp.headers.get("Retry-After")
        try:
            wait = float(retry_after) if retry_after else (2 ** _rate_retries) * 0.5
        except (TypeError, ValueError):
            wait = (2 ** _rate_retries) * 0.5
        time.sleep(min(max(wait, 0.1), 8.0))
        return _get(path, params=params, _retried=_retried, _rate_retries=_rate_retries + 1)
    if resp.status_code != 200:
        raise INDstocksError(f"GET {path} failed ({resp.status_code}): {resp.text[:300]}")
    return resp.json()


# ---------------------------------------------------------------------------
# Instruments master — SYMBOL -> SECURITY_ID lookup, cached in-process
# ---------------------------------------------------------------------------

_instrument_lock = threading.Lock()
_instrument_cache = {"equity": None, "index": None, "loaded_at": 0.0}
INSTRUMENT_CACHE_TTL_SECONDS = float(os.environ.get("INDSTOCKS_INSTRUMENT_CACHE_TTL", str(6 * 3600)))


def _fetch_instruments_csv(source: str) -> str:
    _throttle()
    resp = requests.get(
        f"{BASE_URL}/market/instruments",
        headers={"Authorization": get_access_token()},
        params={"source": source},
        timeout=max(REQUEST_TIMEOUT, 30),
    )
    if resp.status_code != 200:
        raise INDstocksError(f"instruments fetch failed ({resp.status_code}): {resp.text[:300]}")
    return resp.text


def _load_equity_map() -> dict:
    text = _fetch_instruments_csv("equity")
    reader = csv.DictReader(io.StringIO(text))
    out = {}
    for row in reader:
        symbol = (row.get("TRADING_SYMBOL") or row.get("SYMBOL_NAME") or "").strip().upper()
        exch = (row.get("EXCH") or "NSE").strip().upper()
        sec_id = (row.get("SECURITY_ID") or "").strip()
        if symbol and sec_id:
            out.setdefault(symbol, {})[exch] = sec_id
    return out


def _load_index_map() -> dict:
    # The index CSV is 3 columns (EXCH, index-name, SECURITY_ID) — the second
    # column is labelled SEGMENT but actually holds the index name, so we
    # read it positionally rather than by header.
    text = _fetch_instruments_csv("index")
    reader = csv.reader(io.StringIO(text))
    out = {}
    next(reader, None)  # header row
    for row in reader:
        if len(row) < 3:
            continue
        exch, name, sec_id = row[0].strip(), row[1].strip(), row[2].strip()
        if name and sec_id:
            out[name.upper()] = {"exch": exch, "security_id": sec_id}
    return out


def _ensure_instruments_loaded(force: bool = False):
    with _instrument_lock:
        fresh = (time.time() - _instrument_cache["loaded_at"]) < INSTRUMENT_CACHE_TTL_SECONDS
        if not force and _instrument_cache["equity"] is not None and fresh:
            return
        _instrument_cache["equity"] = _load_equity_map()
        _instrument_cache["index"] = _load_index_map()
        _instrument_cache["loaded_at"] = time.time()


def resolve_equity_scrip_code(symbol: str, exchange: str = "NSE") -> str:
    """Map a plain symbol ('RELIANCE', 'RELIANCE.NS', 'RELIANCE.BO') to an
    INDstocks scrip code like 'NSE_2885'. Raises INDstocksError if unknown."""
    sym = symbol.strip().upper()
    if sym.endswith(".NS"):
        sym, exchange = sym[:-3], "NSE"
    elif sym.endswith(".BO"):
        sym, exchange = sym[:-3], "BSE"

    _ensure_instruments_loaded()
    row = (_instrument_cache["equity"] or {}).get(sym)
    if not row:
        # Instrument master can go stale/renamed; retry once with a forced refresh
        # before giving up, same spirit as the app's existing ticker-alias handling.
        _ensure_instruments_loaded(force=True)
        row = (_instrument_cache["equity"] or {}).get(sym)
    if not row:
        raise INDstocksError(f"Unknown symbol '{symbol}' in INDstocks equity instrument master")
    sec_id = row.get(exchange) or next(iter(row.values()), None)
    if not sec_id:
        raise INDstocksError(f"No security_id for '{symbol}' on {exchange}")
    return f"{exchange}_{sec_id}"


def resolve_index_scrip_code(name_substring: str) -> str:
    """Look up an index (e.g. 'NIFTY 50', 'INDIA VIX') by name against the INDstocks
    index instrument list. Tries an exact case-insensitive match first -- substring-only
    matching is ambiguous ('NIFTY 50' is itself a substring of 'NIFTY 500', 'NIFTY 50 USD',
    'NIFTY 50 EQUAL WEIGHT', etc., so it can silently resolve to the wrong index depending
    on CSV row order), then falls back to substring matching for names that are genuinely
    partial (e.g. an alias that isn't the exact index name on file)."""
    _ensure_instruments_loaded()
    needle = name_substring.strip().upper()
    index_map = _instrument_cache["index"] or {}
    exact = index_map.get(needle)
    if exact:
        return f"{exact['exch']}_{exact['security_id']}"
    for name, row in index_map.items():
        if needle in name or name in needle:
            return f"{row['exch']}_{row['security_id']}"
    raise INDstocksError(f"No index found matching '{name_substring}'")


# ---------------------------------------------------------------------------
# Quotes
# ---------------------------------------------------------------------------

def get_ltp(scrip_codes) -> dict:
    """scrip_codes: list of e.g. ['NSE_2885']. Up to 1000 per call."""
    data = _get("/market/quotes/ltp", {"scrip-codes": ",".join(scrip_codes)})
    return data.get("data") or {}


def get_full_quote(scrip_codes) -> dict:
    data = _get("/market/quotes/full", {"scrip-codes": ",".join(scrip_codes)})
    return data.get("data") or {}


def get_market_depth(scrip_codes) -> dict:
    data = _get("/market/quotes/mkt", {"scrip-codes": ",".join(scrip_codes)})
    return data.get("data") or {}


# ---------------------------------------------------------------------------
# Portfolio — live holdings/positions from your actual INDstocks account.
# Docs: https://api-docs.indstocks.com/portfolio_funds/
# ---------------------------------------------------------------------------

def get_holdings() -> list:
    """Current equity holdings (stocks sitting in your Demat account).
    Each item: security_id, trading_symbol, exchange_segment, isin, quantity,
    average_price, last_traded_price, close_price, market_value,
    pnl_absolute, pnl_percent."""
    data = _get("/portfolio/holdings")
    return data.get("data") or []


def get_positions(segment: str = None, product: str = None) -> list:
    """Open positions (e.g. today's intraday trades not yet squared off).
    segment: 'equity' or 'derivative'. product: e.g. 'intraday', 'margin'."""
    params = {}
    if segment:
        params["segment"] = segment
    if product:
        params["product"] = product
    data = _get("/portfolio/positions", params)
    return data.get("data") or []


# ---------------------------------------------------------------------------
# Historical OHLCV
# ---------------------------------------------------------------------------

# yfinance-style interval strings -> INDstocks interval strings
INTERVAL_MAP = {
    "1m": "1minute", "1minute": "1minute",
    "2m": "2minute", "2minute": "2minute",
    "3m": "3minute", "3minute": "3minute",
    "5m": "5minute", "5minute": "5minute",
    "10m": "10minute", "10minute": "10minute",
    "15m": "15minute", "15minute": "15minute",
    "30m": "30minute", "30minute": "30minute",
    "60m": "60minute", "1h": "60minute", "60minute": "60minute",
    "1d": "1day", "1day": "1day",
    "1wk": "1week", "1week": "1week",
    "1mo": "1month", "1month": "1month",
}

# Max span (days) INDstocks allows per single historical call, per interval.
MAX_RANGE_DAYS = {
    "1minute": 7, "2minute": 7, "3minute": 7, "4minute": 7, "5minute": 7,
    "10minute": 7, "15minute": 7, "30minute": 7,
    "60minute": 15, "120minute": 15, "180minute": 15, "240minute": 15,
    "1day": 365, "1week": 365, "1month": 365,
}

MAX_SCRIPS_PER_HISTORICAL_CALL = 5


def get_historical(scrip_codes, interval: str, start_dt: datetime.datetime, end_dt: datetime.datetime) -> dict:
    """Fetch OHLCV candles for one or more scrip codes across a date range,
    transparently batching by the API's 5-scrip-per-call and max-range-per-
    call limits (walking backwards window by window, paging by 5 scrips).

    Returns {scrip_code: [ {ts, o, h, l, c, v}, ... ]} sorted oldest-first.
    Missing/unavailable scrips simply come back with an empty list.
    """
    ind_interval = INTERVAL_MAP.get(interval)
    if not ind_interval:
        raise INDstocksError(f"Unsupported interval '{interval}'")
    max_days = MAX_RANGE_DAYS[ind_interval]

    codes = list(dict.fromkeys(scrip_codes))  # de-dupe, keep order
    result = {code: [] for code in codes}

    for batch_start in range(0, len(codes), MAX_SCRIPS_PER_HISTORICAL_CALL):
        batch = codes[batch_start:batch_start + MAX_SCRIPS_PER_HISTORICAL_CALL]
        window_end = end_dt
        while window_end > start_dt:
            window_start = max(start_dt, window_end - datetime.timedelta(days=max_days))
            params = {
                "scrip-codes": ",".join(batch),
                "start_time": int(window_start.timestamp() * 1000),
                "end_time": int(window_end.timestamp() * 1000),
            }
            try:
                data = _get(f"/market/historical/{ind_interval}", params)
            except INDstocksError as e:
                print(f"[indstocks_client] historical fetch failed for {batch} "
                      f"[{window_start.date()} to {window_end.date()}]: {e}")
                window_end = window_start
                continue
            for code, payload in (data.get("data") or {}).items():
                candles = payload.get("candles") or []
                result.setdefault(code, [])
                result[code] = candles + result[code]  # older windows go in front
            window_end = window_start

    for code in result:
        result[code].sort(key=lambda c: c.get("ts", 0))
    return result
