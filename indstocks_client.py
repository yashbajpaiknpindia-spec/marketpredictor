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
import json
import requests
from export_streaming import response_json, response_error

BASE_URL = "https://api.indstocks.com"

API_KEY = os.environ.get("INDSTOCKS_API_KEY", "").strip()
MPIN = os.environ.get("INDSTOCKS_MPIN", "").strip()
TOTP_SECRET = os.environ.get("INDSTOCKS_TOTP_SECRET", "").strip()

REQUEST_TIMEOUT = float(os.environ.get("INDSTOCKS_REQUEST_TIMEOUT", "20"))

# Stay comfortably under INDstocks' documented 5 requests/second limit for Quote/Data APIs.
# The same global throttle is shared by the app so concurrent data paths do not
# accidentally turn a safe per-path plan into a 429 burst.
_MIN_GAP_SECONDS = float(os.environ.get("INDSTOCKS_MIN_REQUEST_GAP", "0.21"))
_last_request_lock = threading.Lock()
_last_request_at = [0.0]


class INDstocksError(Exception):
    pass


class INDstocksIndexHistoricalUnavailable(INDstocksError):
    """Index exists in instruments, but its ID is not accepted by historical REST."""



def _throttle():
    with _last_request_lock:
        wait = _last_request_at[0] + _MIN_GAP_SECONDS - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        _last_request_at[0] = time.monotonic()


def _static_token() -> str:
    """A dashboard-generated access token supplied via env (bypasses /generate/token entirely)."""
    for name in ("INDSTOCKS_ACCESS_TOKEN", "INDSTOCKS_TOKEN"):
        v = os.environ.get(name, "").strip()
        if v:
            return v
    return ""


_static_rejected = set()


def credentials_configured() -> bool:
    return bool(_static_token() or (API_KEY and MPIN and TOTP_SECRET))


def _require_credentials():
    missing = [name for name, val in (
        ("INDSTOCKS_API_KEY", API_KEY),
        ("INDSTOCKS_MPIN", MPIN),
        ("INDSTOCKS_TOTP_SECRET", TOTP_SECRET),
    ) if not val]
    if missing:
        raise INDstocksError(
            f"Missing INDstocks credentials: {', '.join(missing)}. "
            "Set them in your environment (see indstocks_client.py docstring), "
            "or set INDSTOCKS_ACCESS_TOKEN to a dashboard-generated token."
        )


def _generate_totp_code() -> str:
    try:
        import pyotp
    except ImportError as e:
        raise INDstocksError("pyotp is required for TOTP token generation — add it to requirements.txt") from e
    # Authenticator keys are often copied with spaces / lowercase; pyotp wants plain base32.
    return pyotp.TOTP(TOTP_SECRET.replace(" ", "").replace("-", "").upper()).now()


USER_AGENT = os.environ.get("INDSTOCKS_USER_AGENT", "MarketPredictor-INDstocks-client/2.0 (paper-research; python-requests)")
# Docs: "1 token per 60 seconds". Stay clear of that edge.
MIN_MINT_GAP_SECONDS = 65.0


def _headers(token: str = None) -> dict:
    h = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    if token:
        h["Authorization"] = token
    return h


# ---------------------------------------------------------------------------
# Access token
#
# INDstocks keeps exactly ONE live TOTP-issued token per account: every successful
# /generate/token call kills the previous token (api-docs.indstocks.com/Users/).
# So the token and the failure/cooldown memory must be SHARED by every process and
# must SURVIVE restarts -- otherwise each deploy/restart (and the old+new instance
# overlap during a Render deploy) re-mints, invalidates the other's token, trips the
# 1-per-60s rule and finally gets the account/IP challenged by Cloudflare.
# app.py plugs a PostgreSQL-backed store in via configure_token_store(); without a
# store the module behaves as before (per-process memory only).
# ---------------------------------------------------------------------------

_token_lock = threading.Lock()
_token_cache = {"token": None, "expires_at": 0.0, "source": None}
_token_state = {"fail_count": 0, "cooldown_until": 0.0, "last_error": None, "last_error_at": None,
                "last_attempt_at": 0.0, "last_ok_at": None, "last_mint_at": 0.0}
_token_store = None


def configure_token_store(store) -> None:
    """store needs: load() -> dict|None, save(dict), lock() -> context manager yielding True/False."""
    global _token_store
    _token_store = store


def _snapshot_state() -> dict:
    return {"token": _token_cache["token"], "expires_at": _token_cache["expires_at"], **_token_state}


def _sync_from_store() -> None:
    if _token_store is None:
        return
    try:
        st = _token_store.load()
    except Exception:
        return
    if not st:
        return
    _token_cache["token"] = st.get("token") or None
    _token_cache["expires_at"] = float(st.get("expires_at") or 0.0)
    _token_cache["source"] = "shared-store" if st.get("token") else None
    for k in ("fail_count", "cooldown_until", "last_attempt_at", "last_mint_at"):
        _token_state[k] = float(st.get(k) or 0.0) if k != "fail_count" else int(st.get(k) or 0)
    for k in ("last_error", "last_error_at", "last_ok_at"):
        _token_state[k] = st.get(k)


def _persist_state() -> None:
    if _token_store is None:
        return
    try:
        _token_store.save(_snapshot_state())
    except Exception as exc:  # the store is best-effort; never break quoting because of it
        print(f"[indstocks_client] token store save failed: {str(exc)[:160]}")


def _compact_http_error(status: int, text: str) -> str:
    t = (text or "").strip()
    low = t[:400].lower()
    if low.startswith("<!doctype") or "<html" in low:
        kind = "Cloudflare challenge page (\"Just a moment...\")" if "just a moment" in low else "HTML page instead of JSON"
        return f"HTTP {status}: provider returned a {kind} - the request was blocked/rate-limited before reaching the API"
    return f"HTTP {status}: {t[:200]}"


def _is_token_rejection(body: str) -> bool:
    low = (body or "").lower()
    if "<html" in low or "cloudflare" in low or "just a moment" in low:
        return False  # a Cloudflare 403 is NOT a revoked token; minting would only make things worse
    return "token" in low


def token_status() -> dict:
    """Safe auth health for the UI. Never contains the token."""
    now = time.time()
    static = _static_token()
    if static and static not in _static_rejected:
        return {"has_token": True, "token_expires_in_s": None, "fail_count": 0, "cooldown_remaining_s": 0,
                "last_error": None, "last_error_at": None, "last_ok_at": None,
                "source": "static INDSTOCKS_ACCESS_TOKEN (dashboard token; auto-mint disabled)", "shared_store": _token_store is not None}
    _sync_from_store()
    return {
        "has_token": bool(_token_cache["token"] and now < _token_cache["expires_at"]),
        "token_expires_in_s": int(max(0, _token_cache["expires_at"] - now)) if _token_cache["token"] else 0,
        "fail_count": int(_token_state["fail_count"]),
        "cooldown_remaining_s": int(max(0, _token_state["cooldown_until"] - now)),
        "last_error": _token_state["last_error"],
        "last_error_at": _token_state["last_error_at"],
        "last_ok_at": _token_state["last_ok_at"],
        "source": _token_cache.get("source") or "none",
        "shared_store": _token_store is not None,
    }


def _now_iso() -> str:
    return datetime.datetime.now().isoformat(timespec="seconds")


def get_access_token(force_refresh: bool = False, user_initiated: bool = False, rejected_token: str = None) -> str:
    """Return a usable access token.

    Order of preference:
      1. INDSTOCKS_ACCESS_TOKEN / INDSTOCKS_TOKEN env (dashboard token) - never calls /generate/token.
      2. The shared token (PostgreSQL store, or this process's memory) if it is still valid.
      3. Mint ONE new token via TOTP - only if (a) no cooldown from earlier failures is running,
         (b) the last mint was >65 s ago, and (c) this process wins the cross-process lock.
         Anyone who loses the lock waits for the winner to publish the token instead of minting.
    `rejected_token` is the token a caller just saw rejected (403). If the shared token is already a
    different one, another process rotated it and we simply adopt it - no new mint.
    """
    static = _static_token()
    if static and static not in _static_rejected:
        if rejected_token and rejected_token == static:
            _static_rejected.add(static)
            if not (API_KEY and MPIN and TOTP_SECRET):
                raise INDstocksError(
                    "INDSTOCKS_ACCESS_TOKEN was rejected by INDstocks (expired after 24h or revoked). "
                    "Generate a new token on the INDstocks Access Tokens page, update the env var and redeploy. "
                    "Auto-refresh is off because no TOTP credentials are configured.")
        else:
            return static

    with _token_lock:
        now = time.time()
        _sync_from_store()

        def _usable():
            return bool(_token_cache["token"]) and time.time() < _token_cache["expires_at"]

        if _usable() and not force_refresh:
            return _token_cache["token"]
        if _usable() and rejected_token and _token_cache["token"] != rejected_token:
            return _token_cache["token"]  # another process already rotated it
        _require_credentials()

        remaining = _token_state["cooldown_until"] - now
        if remaining > 0:
            bypass = user_initiated and (now - _token_state["last_attempt_at"] >= MIN_MINT_GAP_SECONDS)
            if not bypass:
                raise INDstocksError(
                    f"Token generation paused after {_token_state['fail_count']} failure(s); retrying automatically in {int(remaining)}s. "
                    f"Last error: {_token_state['last_error']}")
        since_mint = now - _token_state["last_mint_at"]
        if since_mint < MIN_MINT_GAP_SECONDS and _token_state["last_mint_at"] > 0:
            raise INDstocksError(
                f"A token was generated {int(since_mint)}s ago; INDstocks allows 1 per 60s, so the existing token is kept. Retrying shortly.")

        lock_cm = _token_store.lock() if _token_store is not None else None
        if lock_cm is None:
            from contextlib import nullcontext
            lock_cm = nullcontext(True)
        with lock_cm as got_lock:
            if not got_lock:
                deadline = time.time() + 25.0
                while time.time() < deadline:
                    time.sleep(1.0)
                    _sync_from_store()
                    if _usable() and (not rejected_token or _token_cache["token"] != rejected_token):
                        return _token_cache["token"]
                raise INDstocksError("Another instance is generating the INDstocks token right now; this poll waits for it.")
            # Re-check under the lock: the winner before us may just have published a fresh token.
            _sync_from_store()
            if _usable() and (not rejected_token or _token_cache["token"] != rejected_token) and not (force_refresh and not rejected_token):
                return _token_cache["token"]
            now = time.time()
            if _token_state["cooldown_until"] - now > 0 and not (user_initiated and now - _token_state["last_attempt_at"] >= MIN_MINT_GAP_SECONDS):
                raise INDstocksError(f"Token generation paused; retrying automatically in {int(_token_state['cooldown_until'] - now)}s.")

            _token_state["last_attempt_at"] = now
            _persist_state()  # record the attempt BEFORE the request so a crash/restart cannot loop it
            _throttle()

            def _fail(msg: str, retry_after: float = 0.0, status: int = 0):
                _token_state["fail_count"] += 1
                n = _token_state["fail_count"]
                challenge = "Cloudflare" in msg
                delay = min(600.0, 30.0 * (2 ** (n - 1)))
                if challenge:
                    delay = min(900.0, max(120.0, 60.0 * (2 ** (n - 1))))
                elif status in (400, 401, 403, 422):
                    delay = max(delay, 120.0)  # wrong MPIN/TOTP counts toward a 15-minute lockout; do not retry fast
                delay = max(delay, retry_after)
                _token_state["cooldown_until"] = time.time() + delay
                _token_state["last_error"] = msg[:300]
                _token_state["last_error_at"] = _now_iso()
                _persist_state()
                raise INDstocksError(f"Token generation failed - {msg}")

            try:
                resp = requests.post(
                    f"{BASE_URL}/generate/token",
                    headers={**_headers(), "x-api-key": API_KEY, "Content-Type": "application/json"},
                    json={"mpin": MPIN, "totp": _generate_totp_code()},
                    timeout=REQUEST_TIMEOUT,
                )
            except requests.RequestException as exc:
                _fail(f"network error: {str(exc)[:160]}")
            if resp.status_code != 200:
                try:
                    ra = float(resp.headers.get("Retry-After") or 0)
                except (TypeError, ValueError):
                    ra = 0.0
                _fail(_compact_http_error(resp.status_code, resp.text), ra, resp.status_code)
            try:
                payload = resp.json()
            except ValueError:
                _fail("HTTP 200 but the body was not JSON (provider/proxy page)")
            token = payload.get("token") or (payload.get("data") or {}).get("token")
            if not token:
                _fail(f"response missing 'token' (keys: {sorted(payload.keys()) if isinstance(payload, dict) else type(payload).__name__})")
            done = time.time()
            _token_cache["token"] = token
            _token_cache["source"] = "minted-here"
            # Real expiry is 24h; refresh an hour early to be safe.
            _token_cache["expires_at"] = done + 23 * 3600
            _token_state.update({"fail_count": 0, "cooldown_until": 0.0, "last_error": None,
                                 "last_ok_at": _now_iso(), "last_mint_at": done})
            _persist_state()
            return token


# A 429 is transient by nature -- retrying with backoff means a transient 429 usually
# recovers instead of quietly shrinking the candidate universe.
MAX_RATE_LIMIT_RETRIES = int(os.environ.get("INDSTOCKS_MAX_RATE_LIMIT_RETRIES", "3"))


def _get(path: str, params: dict = None, _retried: bool = False, _rate_retries: int = 0):
    _throttle()
    used_token = get_access_token()
    resp = requests.get(
        f"{BASE_URL}{path}",
        headers=_headers(used_token),
        params=params or {},
        timeout=REQUEST_TIMEOUT, stream=True,
    )
    if resp.status_code == 403 and not _retried:
        body = response_error(resp)
        if _is_token_rejection(body):
            # Token replaced/revoked (TokenException). Adopt the shared one, or mint at most once.
            get_access_token(force_refresh=True, rejected_token=used_token)
            return _get(path, params=params, _retried=True, _rate_retries=_rate_retries)
        raise INDstocksError(f"GET {path} failed (403): {_compact_http_error(403, body)}")
    if resp.status_code == 429 and _rate_retries < MAX_RATE_LIMIT_RETRIES:
        retry_after = resp.headers.get("Retry-After")
        try:
            wait = float(retry_after) if retry_after else (2 ** _rate_retries) * 0.5
        except (TypeError, ValueError):
            wait = (2 ** _rate_retries) * 0.5
        resp.close()
        time.sleep(min(max(wait, 0.1), 8.0))
        return _get(path, params=params, _retried=_retried, _rate_retries=_rate_retries + 1)
    if resp.status_code != 200:
        detail = _compact_http_error(resp.status_code, response_error(resp))
        raise INDstocksError(f"GET {path} failed ({resp.status_code}): {detail}")
    if path.startswith("/market/historical/"):
        return response_json(resp)
    try:
        return resp.json()
    finally:
        resp.close()


# ---------------------------------------------------------------------------
# Instruments master — SYMBOL -> SECURITY_ID lookup, cached in-process
# ---------------------------------------------------------------------------

_instrument_lock = threading.Lock()
_instrument_cache = {"equity": None, "index": None, "loaded_at": 0.0,
                     "equity_rows": None, "isin_map": None, "index_rows": None}
# Cache the historical capability of each index so an unsupported code is probed once.
_index_hist_code_cache = {}
_index_hist_code_lock = threading.Lock()
INSTRUMENT_CACHE_TTL_SECONDS = float(os.environ.get("INDSTOCKS_INSTRUMENT_CACHE_TTL", str(6 * 3600)))


def _fetch_instruments_csv(source: str, _retried: bool = False):
    """Yield CSV lines; do not duplicate a full instrument master in bytes and text."""
    _throttle()
    used_token = get_access_token()
    with requests.get(
        f"{BASE_URL}/market/instruments",
        headers=_headers(used_token),
        params={"source": source}, timeout=max(REQUEST_TIMEOUT, 30), stream=True,
    ) as resp:
        if resp.status_code == 403 and not _retried:
            body = response_error(resp)
            if _is_token_rejection(body):
                get_access_token(force_refresh=True, rejected_token=used_token)
                yield from _fetch_instruments_csv(source, _retried=True)
                return
            raise INDstocksError(f"instruments fetch failed (403): {_compact_http_error(403, body)}")
        if resp.status_code != 200:
            raise INDstocksError(f"instruments fetch failed ({resp.status_code}): {_compact_http_error(resp.status_code, response_error(resp))}")
        resp.encoding = "utf-8-sig"
        yield from resp.iter_lines(decode_unicode=True)


def _load_equity_map() -> dict:
    return _parse_equity_csv(_fetch_instruments_csv("equity"))["map"]


def _parse_equity_csv(text: str) -> dict:
    """Parse the equity instrument master. Returns
        {'map':  {SYMBOL: {EXCH: SECURITY_ID}}            (exactly what this loader always produced)
         'rows': [raw CSV row dict, NSE listings only]    (every column the provider sent, unmodified)
         'isin': {ISIN: SYMBOL}                           (NSE listings; only if the CSV has an ISIN column)}
    The extra outputs are additive -- they let the historical export publish the provider's own
    instrument metadata (tick size, lot size, status ... whatever columns exist) and fall back to an
    ISIN match when a ticker was renamed."""
    reader = csv.DictReader(io.StringIO(text) if isinstance(text, str) else text)
    out, rows, isin_map = {}, [], {}
    isin_col = None
    for row in reader:
        if isin_col is None:
            isin_col = next((k for k in row.keys() if k and "ISIN" in k.upper()), "")
        symbol = (row.get("TRADING_SYMBOL") or row.get("SYMBOL_NAME") or "").strip().upper()
        exch = (row.get("EXCH") or "NSE").strip().upper()
        sec_id = (row.get("SECURITY_ID") or "").strip()
        if symbol and sec_id:
            out.setdefault(symbol, {})[exch] = sec_id
            if exch == "NSE":
                rows.append({k: v for k, v in row.items() if k})
                isin = (row.get(isin_col) or "").strip().upper() if isin_col else ""
                if isin:
                    isin_map.setdefault(isin, symbol)
    return {"map": out, "rows": rows, "isin": isin_map}


def _load_index_map() -> dict:
    return _parse_index_csv(_fetch_instruments_csv("index"))["map"]


def _parse_index_csv(text: str) -> dict:
    # The index CSV is 3 columns (EXCH, index-name, SECURITY_ID) — the second
    # column is labelled SEGMENT but actually holds the index name, so we
    # read it positionally rather than by header.
    reader = csv.reader(io.StringIO(text) if isinstance(text, str) else text)
    out, rows = {}, []
    next(reader, None)  # header row
    for row in reader:
        if len(row) < 3:
            continue
        exch, name, sec_id = row[0].strip(), row[1].strip(), row[2].strip()
        if name and sec_id:
            out[name.upper()] = {"exch": exch, "security_id": sec_id}
            rows.append({"exch": exch, "index_name": name, "security_id": sec_id})
    return {"map": out, "rows": rows}


INSTRUMENT_FAIL_BACKOFF_SECONDS = float(os.environ.get("INDSTOCKS_INSTRUMENT_FAIL_BACKOFF", "60"))
_instrument_fail = {"at": 0.0, "msg": None}


def _ensure_instruments_loaded(force: bool = False):
    with _instrument_lock:
        fresh = (time.time() - _instrument_cache["loaded_at"]) < INSTRUMENT_CACHE_TTL_SECONDS
        if not force and _instrument_cache["equity"] is not None and fresh:
            return
        # A forced reload (triggered by every unknown/renamed symbol) is allowed at most once per
        # 10 minutes; otherwise N unmapped tickers = 2N full instrument-master downloads.
        if force and _instrument_cache["equity"] is not None and (time.time() - _instrument_cache["loaded_at"]) < 600:
            return
        # Failure memory: before this, ONE failed master download was retried for EVERY symbol
        # (250 symbols x 2 CSV downloads) - a request storm that gets the IP challenged by Cloudflare.
        since_fail = time.time() - _instrument_fail["at"]
        if _instrument_fail["at"] and since_fail < INSTRUMENT_FAIL_BACKOFF_SECONDS:
            raise INDstocksError(
                f"Instrument master unavailable (paused {int(INSTRUMENT_FAIL_BACKOFF_SECONDS - since_fail)}s after: {_instrument_fail['msg']})")
        try:
            eq = _parse_equity_csv(_fetch_instruments_csv("equity"))
            ix = _parse_index_csv(_fetch_instruments_csv("index"))
        except Exception as exc:
            _instrument_fail.update({"at": time.time(), "msg": str(exc)[:200]})
            raise
        _instrument_fail.update({"at": 0.0, "msg": None})
        _instrument_cache["equity"] = eq["map"]
        _instrument_cache["equity_rows"] = eq["rows"]
        _instrument_cache["isin_map"] = eq["isin"]
        _instrument_cache["index"] = ix["map"]
        _instrument_cache["index_rows"] = ix["rows"]
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
    # Provider naming changes punctuation/spaces across releases. Compare a compact
    # normalized form as a final exact-ish fallback (e.g. NIFTY FIN SERVICE vs
    # NIFTY FINANCIAL SERVICES, NIFTYBANK vs NIFTY BANK).
    def _compact(v):
        return ''.join(ch for ch in str(v).upper() if ch.isalnum())
    compact_needle = _compact(needle)
    synonyms = {
        'NIFTYBANK': ('NIFTY BANK', 'NIFTYBANK'),
        'NIFTYFINANCIALSERVICES': ('NIFTY FIN SERVICE', 'NIFTY FINANCIAL SERVICES'),
        'NIFTYFINSERVICE': ('NIFTY FIN SERVICE', 'NIFTY FINANCIAL SERVICES'),
    }
    candidates = synonyms.get(compact_needle, (needle,))
    candidate_compact = {_compact(x) for x in candidates}
    for name, row in index_map.items():
        cn = _compact(name)
        if cn in candidate_compact or any(c in cn or cn in c for c in candidate_compact):
            return f"{row['exch']}_{row['security_id']}"
    raise INDstocksError(f"No index found matching '{name_substring}'")


def resolve_equity_scrip_code_strict(symbol: str) -> str:
    """Like resolve_equity_scrip_code(), but ONLY accepts an NSE listing: never falls
    back to another exchange's security id. Used by the historical-data export,
    where silently pulling the wrong instrument would poison the dataset."""
    sym = symbol.strip().upper()
    if sym.endswith(".NS"):
        sym = sym[:-3]
    _ensure_instruments_loaded()
    row = (_instrument_cache["equity"] or {}).get(sym)
    if not row:
        _ensure_instruments_loaded(force=True)
        row = (_instrument_cache["equity"] or {}).get(sym)
    if not row or not row.get("NSE"):
        raise INDstocksError(f"'{symbol}' has no NSE listing in the INDstocks equity instrument master")
    return f"NSE_{row['NSE']}"


def resolve_index_scrip_code_exact(name: str) -> str:
    """Exact (case-insensitive) index-name match only -- no substring fallback.
    resolve_index_scrip_code() falls back to substring matching, which is fine
    for a live scan but could hand the export a different index than the one
    named in the file it writes."""
    _ensure_instruments_loaded()
    row = (_instrument_cache["index"] or {}).get(name.strip().upper())
    if not row:
        raise INDstocksError(f"No index named exactly '{name}' in the INDstocks index instrument list")
    return f"{row['exch']}_{row['security_id']}"


def resolve_equity_scrip_by_isin(isin: str):
    """ISIN -> (NSE symbol, 'NSE_<id>') using the instrument master, or None.
    Used only as a fallback by the historical export when a ticker on an NSE constituent
    list is not (or no longer) the trading symbol INDstocks lists it under (renames,
    e.g. after a merger/demerger). ISINs are unique, so this can never pick a wrong stock."""
    key = (isin or "").strip().upper()
    if not key:
        return None
    _ensure_instruments_loaded()
    sym = (_instrument_cache.get("isin_map") or {}).get(key)
    if not sym:
        return None
    row = (_instrument_cache["equity"] or {}).get(sym) or {}
    if not row.get("NSE"):
        return None
    return sym, f"NSE_{row['NSE']}"


def get_nse_instrument_rows(symbols) -> dict:
    """{SYMBOL: raw instrument-master row (every column the provider sent)} for NSE listings."""
    _ensure_instruments_loaded()
    wanted = {str(x).strip().upper() for x in symbols}
    out = {}
    for row in (_instrument_cache.get("equity_rows") or []):
        sym = (row.get("TRADING_SYMBOL") or row.get("SYMBOL_NAME") or "").strip().upper()
        if sym in wanted and sym not in out:
            out[sym] = row
    return out


def get_index_instrument_rows() -> list:
    """Every row of the provider's index instrument list: [{exch, index_name, security_id}]."""
    _ensure_instruments_loaded()
    return list(_instrument_cache.get("index_rows") or [])


def resolve_index_historical_scrip_code(name: str, interval: str = "1day") -> str:
    """Probe the one documented REST scrip-code shape for an index, once per process.

    The instrument master exposes index SECURITY_IDs, while the historical endpoint
    documents ``SEGMENT_TOKEN`` identifiers such as ``NSE_3045``.  The deployed
    endpoint is currently rejecting the observed index IDs with ``Invalid scrip codes``.
    We therefore try only the documented ``EXCH_SECURITY_ID`` shape, cache success or
    a controlled-unavailable state, and never spray undocumented ``IDX_``/``INDEX_``/
    bare-token variants.
    """
    key = (str(name or '').strip().upper(), str(interval or '1day').strip().lower())
    with _index_hist_code_lock:
        if key in _index_hist_code_cache:
            cached = _index_hist_code_cache[key]
            if cached:
                return cached
            raise INDstocksIndexHistoricalUnavailable(
                f"Index '{name}' is unavailable on INDstocks historical data"
            )

    compact = _norm_index_name(name)
    aliases = {
        'NIFTYBANK': ['NIFTY BANK'],
        'NIFTYFINSERVICE': ['NIFTY FIN SERVICE', 'NIFTY FINANCIAL SERVICES'],
        'NIFTYFINANCIALSERVICES': ['NIFTY FIN SERVICE', 'NIFTY FINANCIAL SERVICES'],
    }
    names = aliases.get(compact, [name])
    candidates = index_code_candidates(names)
    if not candidates:
        _ensure_instruments_loaded(force=True)
        candidates = index_code_candidates(names)
    documented = next((c for c in candidates if c.get('variant') == 'EXCH_ID'), None)
    if not documented:
        with _index_hist_code_lock:
            _index_hist_code_cache[key] = None
        raise INDstocksIndexHistoricalUnavailable(f"No index instrument found for '{name}'")

    ind_interval = INTERVAL_MAP.get(str(interval).lower()) or str(interval).lower()
    now = datetime.datetime.now(datetime.timezone.utc)
    start = now - datetime.timedelta(days=2)
    params = {
        'start_time': int(start.timestamp() * 1000),
        'end_time': int(now.timestamp() * 1000),
        'scrip-codes': documented['code'],
    }
    try:
        payload = _get(f"/market/historical/{ind_interval}", params)
        if isinstance(payload, dict) and payload.get('success') is False:
            raise INDstocksIndexHistoricalUnavailable(
                f"INDstocks rejected documented index code {documented['code']}"
            )
        with _index_hist_code_lock:
            _index_hist_code_cache[key] = documented['code']
        print(f"[indstocks_client] index historical code resolved: {name} -> {documented['code']}")
        return documented['code']
    except Exception as e:
        with _index_hist_code_lock:
            _index_hist_code_cache[key] = None
        raise INDstocksIndexHistoricalUnavailable(
            f"INDstocks historical rejected index code {documented['code']}: {str(e)[:160]}"
        ) from e


def _norm_index_name(name: str) -> str:
    return "".join(ch for ch in str(name or "").upper() if ch.isalnum())


def index_code_candidates(names) -> list:
    """Candidate provider codes for ONE index, given the names it may be listed under.
    Matching is exact-after-normalisation (case/space/punctuation-insensitive equality) --
    never substring -- so 'NIFTY 50' can never resolve to 'NIFTY 500'. For each match the
    documented '<EXCH>_<id>' form comes first, then the alternative code shapes, so a caller
    that PROBES the provider can find the shape it accepts.
    Returns [{'name': index name as listed, 'code': str, 'variant': str}]."""
    _ensure_instruments_loaded()
    wanted = []
    for n in names:
        k = _norm_index_name(n)
        if k and k not in wanted:
            wanted.append(k)
    rows = _instrument_cache.get("index_rows") or []
    out, seen = [], set()
    for k in wanted:
        for r in rows:
            if _norm_index_name(r["index_name"]) != k:
                continue
            exch, sid = r["exch"] or "NSE", r["security_id"]
            for variant, code in (("EXCH_ID", f"{exch}_{sid}"), ("NSE_ID", f"NSE_{sid}"),
                                  ("IDX_ID", f"IDX_{sid}"), ("INDEX_ID", f"INDEX_{sid}"), ("BARE_ID", str(sid))):
                if code not in seen:
                    seen.add(code)
                    out.append({"name": r["index_name"], "code": code, "variant": variant})
    return out


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
    """Return REST market-depth rows keyed by the requested scrip code.

    The documented response is data[SEGMENT_TOKEN].market_depth.depth, but the
    live adapter deliberately normalizes equivalent provider key variants
    (NSE_123 vs NSE:123, numeric token keys) so a harmless representation change
    cannot silently turn valid L5 into zeros.
    """
    data = _get("/market/quotes/mkt", {"scrip-codes": ",".join(scrip_codes)})
    raw = data.get("data") or {} if isinstance(data, dict) else {}
    if not isinstance(raw, dict):
        return {}
    out = {}
    for requested in scrip_codes:
        candidates = [str(requested), str(requested).replace('_', ':'), str(requested).split('_', 1)[-1]]
        row = None
        for key in candidates:
            if isinstance(raw.get(key), dict):
                row = raw.get(key)
                break
        if row is not None:
            out[str(requested)] = row
    # Preserve any exact provider keys too; callers that use the requested
    # canonical code are already covered above.
    for key, row in raw.items():
        if isinstance(row, dict) and key not in out:
            out[key] = row
    return out


def get_market_depth_diagnostic(scrip_code: str) -> dict:
    """Fetch one market-depth quote and expose the raw shape without secrets.

    This is deliberately independent of the worker parser: it answers whether INDstocks
    actually returned a depth ladder, and where that ladder was located.
    """
    _throttle()
    resp = requests.get(
        f"{BASE_URL}/market/quotes/mkt",
        headers=_headers(get_access_token()),
        params={"scrip-codes": str(scrip_code)},
        timeout=REQUEST_TIMEOUT,
    )
    try:
        status_code = int(resp.status_code)
        try:
            payload = resp.json() if status_code == 200 else {}
        except ValueError:
            payload = {}
        data = payload.get("data") if isinstance(payload, dict) else None
        data = data if isinstance(data, dict) else {}
        row = None
        matched_key = None
        for key in (str(scrip_code), str(scrip_code).replace('_', ':'), str(scrip_code).split('_', 1)[-1]):
            if isinstance(data.get(key), dict):
                row = data.get(key)
                matched_key = key
                break
        row = row if isinstance(row, dict) else {}
        md = row.get("market_depth") if isinstance(row.get("market_depth"), dict) else None

        def _norm(k):
            return str(k).strip().replace('-', '_').replace(' ', '_').casefold()

        side_alias = {
            'buy':'buy','bid':'buy','bids':'buy','buy_depth':'buy','bid_levels':'buy','buy_orders':'buy',
            'sell':'sell','ask':'sell','asks':'sell','sell_depth':'sell','ask_levels':'sell','sell_orders':'sell',
        }
        first_path = None
        first_levels = None
        queue = [('market_depth', md, 0)]
        seen = set()
        while queue and first_levels is None:
            path, node, hops = queue.pop(0)
            if hops > 12 or id(node) in seen:
                continue
            if isinstance(node, (dict, list)):
                seen.add(id(node))
            if isinstance(node, list):
                if node:
                    first_levels, first_path = node[:5], path
                    break
            elif isinstance(node, dict):
                # Detect buy/sell side containers directly.
                sides = {}
                for k, v in node.items():
                    alias = side_alias.get(_norm(k))
                    if alias:
                        sides[alias] = v
                if 'buy' in sides and 'sell' in sides:
                    b = sides['buy'] if isinstance(sides['buy'], list) else list(sides['buy'].values()) if isinstance(sides['buy'], dict) and not any(x in {_norm(k) for k in sides['buy'].keys()} for x in ('price','quantity','qty')) else [sides['buy']]
                    a = sides['sell'] if isinstance(sides['sell'], list) else list(sides['sell'].values()) if isinstance(sides['sell'], dict) and not any(x in {_norm(k) for k in sides['sell'].keys()} for x in ('price','quantity','qty')) else [sides['sell']]
                    if b and a:
                        first_levels = [{"buy": x if isinstance(x, dict) else {}, "sell": y if isinstance(y, dict) else {}} for x,y in zip(b[:5],a[:5])]
                        first_path = path + '.buy/sell'
                        break
                for k, v in node.items():
                    if isinstance(v, (dict, list)):
                        queue.append((f'{path}.{k}', v, hops+1))

        valid = 0
        if isinstance(first_levels, list):
            for lvl in first_levels[:5]:
                buy = lvl.get('buy') if isinstance(lvl, dict) else {}
                sell = lvl.get('sell') if isinstance(lvl, dict) else {}
                try:
                    bp = float(str((buy or {}).get('price') or 0).replace(',', ''))
                    sp = float(str((sell or {}).get('price') or 0).replace(',', ''))
                    bq = float(str((buy or {}).get('quantity', (buy or {}).get('qty')) or 0).replace(',', ''))
                    sq = float(str((sell or {}).get('quantity', (sell or {}).get('qty')) or 0).replace(',', ''))
                except (TypeError, ValueError):
                    bp = sp = bq = sq = 0.0
                if bp > 0 and sp > 0 and bq > 0 and sq > 0:
                    valid += 1

        depth_key = next((k for k in md.keys() if _norm(k) == 'depth'), None) if isinstance(md, dict) else None
        raw_depth = md.get(depth_key) if depth_key is not None else None
        return {
            "http_status": status_code,
            "top_level_keys": sorted(payload.keys()) if isinstance(payload, dict) else [],
            "data_keys_sample": list(data.keys())[:10],
            "matched_key": matched_key,
            "row_keys": sorted(row.keys()),
            "market_depth_present": isinstance(md, dict),
            "market_depth_keys": sorted(md.keys()) if isinstance(md, dict) else [],
            "depth_key": depth_key,
            "depth_container_type": type(raw_depth).__name__ if raw_depth is not None else None,
            "depth_container_count": len(raw_depth) if isinstance(raw_depth, (list, dict)) else 0,
            "depth_path": first_path,
            "depth_levels": len(first_levels or []),
            "valid_levels": valid,
            "first_level": (first_levels[0] if first_levels else None),
            # Market data only: safe excerpt for diagnosing provider-side shape.
            "row_excerpt": (json.dumps(row, default=str)[:2000] if row else None),
        }
    finally:
        resp.close()



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


def get_historical_with_report(scrip_codes, interval: str, start_dt: datetime.datetime, end_dt: datetime.datetime,
                                window_retries: int = 0, cancel_check=None):
    """Same batching/windowing as get_historical(), but ALSO returns a list of the
    (scrip batch, time window) fetches that failed, instead of only printing them.

    Why this exists: get_historical() swallows a failed window (prints, moves on),
    which is fine for a live scan but silently turns an API hiccup into a gap in
    the data. The historical-data export must never hand over a dataset with
    hidden gaps, so it uses this variant, retries each failed window
    `window_retries` extra times, and records whatever still failed.

    start_dt / end_dt may be timezone-aware (recommended for exact IST day
    boundaries) or naive (interpreted in the server's local time, as before).
    `cancel_check`, if given, is called between windows; a truthy return aborts.

    Returns (result, failures):
      result   = {scrip_code: [ {ts, o, h, l, c, v}, ... ]} sorted oldest-first
      failures = [ {'scrips': [...], 'start': iso, 'end': iso, 'error': str}, ... ]
    """
    ind_interval = INTERVAL_MAP.get(interval)
    if not ind_interval:
        raise INDstocksError(f"Unsupported interval '{interval}'")
    max_days = MAX_RANGE_DAYS[ind_interval]

    codes = list(dict.fromkeys(scrip_codes))  # de-dupe, keep order
    result = {code: [] for code in codes}
    failures = []

    for batch_start in range(0, len(codes), MAX_SCRIPS_PER_HISTORICAL_CALL):
        batch = codes[batch_start:batch_start + MAX_SCRIPS_PER_HISTORICAL_CALL]
        window_end = end_dt
        while window_end > start_dt:
            if cancel_check and cancel_check():
                return result, failures
            window_start = max(start_dt, window_end - datetime.timedelta(days=max_days))
            params = {
                "scrip-codes": ",".join(batch),
                "start_time": int(window_start.timestamp() * 1000),
                "end_time": int(window_end.timestamp() * 1000),
            }
            data = None
            last_err = None
            for attempt in range(window_retries + 1):
                try:
                    data = _get(f"/market/historical/{ind_interval}", params)
                    last_err = None
                    break
                except INDstocksError as e:
                    last_err = e
                    if attempt < window_retries:
                        time.sleep(min(2.0 * (attempt + 1), 8.0))
            if last_err is not None:
                print(f"[indstocks_client] historical fetch failed for {batch} "
                      f"[{window_start.date()} to {window_end.date()}]: {last_err}")
                failures.append({
                    "scrips": list(batch),
                    "start": window_start.isoformat(),
                    "end": window_end.isoformat(),
                    "error": str(last_err)[:300],
                })
                window_end = window_start
                continue
            for code, payload in (data.get("data") or {}).items():
                candles = payload.get("candles") or []
                result.setdefault(code, [])
                result[code] = candles + result[code]  # older windows go in front
            window_end = window_start

    for code in result:
        result[code].sort(key=lambda c: c.get("ts", 0))
    return result, failures


def plan_windows(interval: str, start_dt: datetime.datetime, end_dt: datetime.datetime) -> list:
    """The exact (window_start, window_end) list get_historical_with_report() walks, newest
    first, so a caller can fetch and consume ONE window at a time (bounded memory) instead of
    holding a whole date range for a batch of scrips in RAM."""
    ind_interval = INTERVAL_MAP.get(interval)
    if not ind_interval:
        raise INDstocksError(f"Unsupported interval '{interval}'")
    max_days = MAX_RANGE_DAYS[ind_interval]
    windows, window_end = [], end_dt
    while window_end > start_dt:
        window_start = max(start_dt, window_end - datetime.timedelta(days=max_days))
        windows.append((window_start, window_end))
        window_end = window_start
    return windows


def fetch_window(scrip_codes, interval: str, window_start: datetime.datetime, window_end: datetime.datetime,
                 retries: int = 0):
    """ONE historical API call for <=5 scrips over ONE window (no accumulation).
    Returns (data, error): data = {scrip: [candle dicts, oldest-first]} or None if it failed
    after `retries` extra attempts, and error = the last error text (or None)."""
    ind_interval = INTERVAL_MAP.get(interval)
    if not ind_interval:
        raise INDstocksError(f"Unsupported interval '{interval}'")
    codes = list(dict.fromkeys(scrip_codes))[:MAX_SCRIPS_PER_HISTORICAL_CALL]
    params = {
        "scrip-codes": ",".join(codes),
        "start_time": int(window_start.timestamp() * 1000),
        "end_time": int(window_end.timestamp() * 1000),
    }
    last_err = None
    for attempt in range(retries + 1):
        try:
            payload = _get(f"/market/historical/{ind_interval}", params)
            data = {code: [] for code in codes}
            for code, item in (payload.get("data") or {}).items():
                candles = (item or {}).get("candles") or []
                candles.sort(key=lambda c: c.get("ts", 0))
                data[code] = candles
            return data, None
        except INDstocksError as e:
            last_err = e
            if attempt < retries:
                time.sleep(min(2.0 * (attempt + 1), 8.0))
        except requests.RequestException as e:  # network hiccup: same treatment as an API error
            last_err = e
            if attempt < retries:
                time.sleep(min(2.0 * (attempt + 1), 8.0))
    return None, str(last_err)[:300]


def get_historical(scrip_codes, interval: str, start_dt: datetime.datetime, end_dt: datetime.datetime) -> dict:
    """Fetch OHLCV candles for one or more scrip codes across a date range,
    transparently batching by the API's 5-scrip-per-call and max-range-per-
    call limits (walking backwards window by window, paging by 5 scrips).

    Returns {scrip_code: [ {ts, o, h, l, c, v}, ... ]} sorted oldest-first.
    Missing/unavailable scrips simply come back with an empty list.

    Behavior is unchanged from before: failed windows are printed and skipped
    (no retry). Use get_historical_with_report() when gaps must be surfaced.
    """
    result, _failures = get_historical_with_report(scrip_codes, interval, start_dt, end_dt, window_retries=0)
    return result
