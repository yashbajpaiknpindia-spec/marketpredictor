"""Documented fallback data providers for MarketPredictor historical export.

Primary provider remains INDstocks. This module is only used when INDstocks rejects/omits
an instrument or when a dataset is not available there.

Official contracts used:
- Upstox V3 historical candles:
  /v3/historical-candle/{instrument_key}/{unit}/{interval}/{to_date}/{from_date}
- Upstox instruments JSON (NSE + Global)
- Upstox Fundamentals corporate-actions/profile/income-statement
- Upstox Market FII/DII endpoints
- Dhan V2 intraday candles (optional third fallback when a security-id mapping is configured)

No credential is embedded. Set UPSTOX_ACCESS_TOKEN and/or DHAN_ACCESS_TOKEN in the deployment
secrets. A missing credential is reported as CREDENTIAL_MISSING, never silently skipped.
"""
from __future__ import annotations

import datetime as _dt
import json
import os
import time
import threading
from export_streaming import response_file, json_master_rows, response_json, response_error
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests

IST = ZoneInfo("Asia/Kolkata")
UPSTOX_BASE = "https://api.upstox.com"
UPSTOX_NSE_MASTER = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
UPSTOX_GLOBAL_MASTER = "https://assets.upstox.com/market-quote/instruments/exchange/global.json.gz"
DHAN_BASE = "https://api.dhan.co/v2"
DHAN_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
HTTP_TIMEOUT = float(os.getenv("HISTORICAL_FALLBACK_TIMEOUT", "30"))
UPSTOX_PACE_SECONDS = float(os.getenv("HISTORICAL_UPSTOX_PACE_SECONDS", "0.14"))  # <500 standard API calls/min


class FallbackError(RuntimeError):
    pass


def _norm(v: str) -> str:
    return "".join(ch for ch in str(v or "").upper() if ch.isalnum())


def _token() -> str:
    return os.getenv("UPSTOX_ACCESS_TOKEN", "").strip()


def upstox_configured() -> bool:
    return bool(_token())


def dhan_configured() -> bool:
    return bool(os.getenv("DHAN_ACCESS_TOKEN", "").strip())


def _upstox_headers() -> Dict[str, str]:
    token = _token()
    if not token:
        raise FallbackError("CREDENTIAL_MISSING: UPSTOX_ACCESS_TOKEN is not configured")
    return {"Accept": "application/json", "Authorization": f"Bearer {token}"}


def _upstox_request(method: str, url: str, *, params=None, headers=None, max_retries: int = 3):
    """Documented Upstox request wrapper with conservative rate pacing and 429 retry.

    Official standard-API limits are 50/sec, 500/min, 2000/30min. We intentionally pace
    slower than the minute ceiling because an export can make hundreds of fundamentals calls.
    """
    hdr = headers or _upstox_headers()
    last = None
    for attempt in range(max_retries):
        r = requests.request(method, url, params=params or {}, headers=hdr, timeout=HTTP_TIMEOUT, stream=True)
        last = r
        time.sleep(UPSTOX_PACE_SECONDS)
        if r.status_code != 429:
            return r
        if attempt == max_retries - 1:
            return r
        retry = r.headers.get("Retry-After")
        r.close()
        try:
            wait = max(float(retry), 1.0) if retry else (2.0 * (attempt + 1))
        except ValueError:
            wait = 2.0 * (attempt + 1)
        time.sleep(min(wait, 10.0))
    return last


_master_lock = threading.RLock()


def load_upstox_master(kind: str) -> List[Dict[str, Any]]:
    # Lock outside the LRU wrapper: concurrent cache misses must not download twice.
    with _master_lock:
        return _load_upstox_master(kind)


@lru_cache(maxsize=2)
def _load_upstox_master(kind: str) -> List[Dict[str, Any]]:
    url = UPSTOX_GLOBAL_MASTER if kind == "global" else UPSTOX_NSE_MASTER
    r = requests.get(url, timeout=HTTP_TIMEOUT, stream=True,
                     headers={"User-Agent": "MarketPredictor/3.2.5"})
    with response_file(r) as src:
        # The NSE catalogue includes derivatives; only index entries are used here.
        return [row for row in json_master_rows(src)
                if kind == "global" or (row.get("segment") == "NSE_INDEX"
                                        and row.get("instrument_type") == "INDEX")]


def resolve_upstox_index(names: Iterable[str]) -> Optional[Dict[str, Any]]:
    wanted = {_norm(n) for n in names if n}
    for row in load_upstox_master("nse"):
        if row.get("segment") != "NSE_INDEX" or row.get("instrument_type") != "INDEX":
            continue
        vals = {row.get("name", ""), row.get("trading_symbol", ""), row.get("short_name", "")}
        if any(_norm(v) in wanted for v in vals if v):
            return row
    # India VIX has an explicitly documented key even if a master naming change occurs.
    if "INDIAVIX" in wanted or "NIFTYINDIAVIX" in wanted:
        return {"name": "India VIX", "trading_symbol": "India VIX", "instrument_key": "NSE_INDEX|India VIX",
                "segment": "NSE_INDEX", "instrument_type": "INDEX"}
    return None


def resolve_upstox_global(names: Iterable[str]) -> Optional[Dict[str, Any]]:
    wanted = {_norm(n) for n in names if n}
    for row in load_upstox_master("global"):
        vals = {row.get("name", ""), row.get("trading_symbol", "")}
        if any(_norm(v) in wanted for v in vals if v):
            return row
    return None


def _upstox_interval(interval: str) -> Tuple[str, str, int]:
    m = {"1m": ("minutes", "1", 30), "5m": ("minutes", "5", 30), "15m": ("minutes", "15", 30),
         "1d": ("days", "1", 3650)}
    if interval not in m:
        raise FallbackError(f"Upstox fallback does not support interval {interval!r}")
    return m[interval]


def _date_windows(start: _dt.date, end: _dt.date, days: int):
    cur = start
    while cur <= end:
        to = min(end, cur + _dt.timedelta(days=days - 1))
        yield cur, to
        cur = to + _dt.timedelta(days=1)


def fetch_upstox_historical(instrument_key: str, interval: str, start: _dt.date, end: _dt.date) -> List[Dict[str, Any]]:
    unit, iv, max_days = _upstox_interval(interval)
    headers = _upstox_headers()
    out: Dict[int, Dict[str, Any]] = {}
    for frm, to in _date_windows(start, end, max_days):
        key = quote(instrument_key, safe="")
        url = f"{UPSTOX_BASE}/v3/historical-candle/{key}/{unit}/{iv}/{to.isoformat()}/{frm.isoformat()}"
        r = _upstox_request("GET", url, headers=headers)
        if r.status_code != 200:
            detail = response_error(r)
            raise FallbackError(f"Upstox historical HTTP {r.status_code} for {instrument_key}: {detail}")
        payload = response_json(r)
        if payload.get("status") != "success":
            raise FallbackError(f"Upstox historical non-success for {instrument_key}: {str(payload)[:250]}")
        for row in ((payload.get("data") or {}).get("candles") or []):
            if len(row) < 6:
                continue
            dt = _dt.datetime.fromisoformat(str(row[0]).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=IST)
            ts = int(dt.timestamp())
            out[ts] = {"ts": ts, "o": row[1], "h": row[2], "l": row[3], "c": row[4], "v": row[5] or 0}
    return [out[k] for k in sorted(out)]


def fetch_upstox_equity(isin: str, interval: str, start: _dt.date, end: _dt.date):
    if not isin:
        raise FallbackError("Upstox equity fallback requires an ISIN")
    key = f"NSE_EQ|{isin}"
    candles = fetch_upstox_historical(key, interval, start, end)
    return candles, {"provider": "Upstox", "instrument_key": key, "name": isin}


def fetch_upstox_index(names: Iterable[str], interval: str, start: _dt.date, end: _dt.date):
    _upstox_headers()  # reject missing credentials before downloading any master
    row = resolve_upstox_index(names)
    if not row:
        raise FallbackError(f"Upstox instrument master has no exact NSE_INDEX match for {list(names)}")
    candles = fetch_upstox_historical(row["instrument_key"], interval, start, end)
    return candles, {"provider": "Upstox", "instrument_key": row["instrument_key"], "name": row.get("name", "")}


def fetch_upstox_global(names: Iterable[str], interval: str, start: _dt.date, end: _dt.date):
    _upstox_headers()
    row = resolve_upstox_global(names)
    if not row:
        raise FallbackError(f"Upstox global instrument master has no exact match for {list(names)}")
    candles = fetch_upstox_historical(row["instrument_key"], interval, start, end)
    return candles, {"provider": "Upstox", "instrument_key": row["instrument_key"], "name": row.get("name", "")}


def upstox_get(path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    r = _upstox_request("GET", f"{UPSTOX_BASE}{path}", params=params or {}, headers=_upstox_headers())
    if r.status_code != 200:
        detail = response_error(r)
        raise FallbackError(f"Upstox {path} HTTP {r.status_code}: {detail}")
    p = response_json(r)
    if p.get("status") != "success":
        raise FallbackError(f"Upstox {path} non-success: {str(p)[:250]}")
    return p.get("data")


def fetch_upstox_corporate_actions(isin: str) -> List[Dict[str, Any]]:
    return upstox_get(f"/v2/fundamentals/{quote(isin, safe='')}/corporate-actions") or []


def fetch_upstox_company_profile(isin: str) -> Dict[str, Any]:
    return upstox_get(f"/v2/fundamentals/{quote(isin, safe='')}/profile") or {}


def fetch_upstox_income_statement(isin: str) -> Dict[str, Any]:
    return upstox_get(f"/v2/fundamentals/{quote(isin, safe='')}/income-statement",
                      {"type": "consolidated", "time_period": "quarterly", "fs": "true"}) or {}


def fetch_upstox_news(instrument_keys: List[str], page_number: int = 1, page_size: int = 100) -> Dict[str, Any]:
    # Official endpoint only returns the past seven days; callers MUST preserve this coverage caveat.
    return upstox_get("/v2/news", {"category": "instrument_keys", "instrument_keys": ",".join(instrument_keys[:30]),
                                    "page_number": page_number, "page_size": page_size}) or {}


def _activity_rows(data: Any) -> List[Dict[str, Any]]:
    if isinstance(data, dict):
        rows = data.get("NSE_EQ|CASH") or []
        return rows if isinstance(rows, list) else []
    return data if isinstance(data, list) else []


def _activity_page(path: str, start: _dt.date) -> List[Dict[str, Any]]:
    data = upstox_get(path, {"data_type": "NSE_EQ|CASH", "interval": "1D", "from": start.isoformat()}) or {}
    return _activity_rows(data)


def fetch_upstox_fii_dii(start: _dt.date, end: Optional[_dt.date] = None) -> Dict[str, Any]:
    """Page Upstox FII/DII daily history without violating the documented 30-trading-day limit.

    The endpoint exposes `from` but no `to`; advance from the newest timestamp actually returned.
    """
    end = end or _dt.date.today()
    out = {"fii": {}, "dii": {}}
    for label, path in (("fii", "/v2/market/fii"), ("dii", "/v2/market/dii")):
        cur = start
        seen = set()
        for _ in range(24):
            if cur > end:
                break
            rows = _activity_page(path, cur)
            if not rows:
                break
            newest = None
            for row in rows:
                if not isinstance(row, dict):
                    continue
                ts = row.get("time_stamp")
                key = json.dumps(row, sort_keys=True, default=str)
                seen.add(key); out[label][key] = row
                try:
                    day = _dt.datetime.fromtimestamp(int(ts) / 1000, tz=IST).date()
                    newest = day if newest is None or day > newest else newest
                except Exception:
                    pass
            if newest is None or newest < cur:
                break
            nxt = newest + _dt.timedelta(days=1)
            if nxt <= cur:
                break
            cur = nxt
    return {k: list(v.values()) for k, v in out.items()}


def load_dhan_master() -> List[Dict[str, Any]]:
    with _master_lock:
        return _load_dhan_master()


@lru_cache(maxsize=1)
def _load_dhan_master() -> List[Dict[str, Any]]:
    import csv, io
    r = requests.get(DHAN_MASTER_URL, timeout=HTTP_TIMEOUT, stream=True,
                     headers={"User-Agent": "MarketPredictor/3.2.5"})
    with response_file(r) as src:
        with io.TextIOWrapper(src, encoding="utf-8-sig", newline="") as text:
            return [row for row in csv.DictReader(text)
                    if str(row.get("INSTRUMENT") or row.get("SEM_INSTRUMENT_NAME") or "").upper() == "INDEX"]


def resolve_dhan_index(names: Iterable[str]) -> Optional[Dict[str, Any]]:
    wanted = {_norm(n) for n in names if n}
    for row in load_dhan_master():
        instrument = str(row.get("INSTRUMENT") or row.get("SEM_INSTRUMENT_NAME") or "").upper()
        if instrument != "INDEX":
            continue
        vals = [row.get("DISPLAY_NAME"), row.get("SYMBOL_NAME"), row.get("SEM_CUSTOM_SYMBOL"),
                row.get("SM_SYMBOL_NAME"), row.get("SEM_TRADING_SYMBOL")]
        if not any(_norm(v) in wanted for v in vals if v):
            continue
        sid = row.get("SECURITY_ID") or row.get("SEM_SMST_SECURITY_ID") or row.get("SM_SECURITY_ID")
        if sid:
            out = dict(row); out["_security_id"] = str(sid); return out
    return None


def fetch_dhan_index_by_names(names: Iterable[str], interval: str, start: _dt.datetime, end: _dt.datetime):
    if not dhan_configured():
        raise FallbackError("CREDENTIAL_MISSING: DHAN_ACCESS_TOKEN is not configured")
    row = resolve_dhan_index(names)
    if not row:
        raise FallbackError(f"Dhan instrument master has no exact INDEX match for {list(names)}")
    candles = fetch_dhan_index(row["_security_id"], interval, start, end)
    return candles, {"provider": "Dhan", "instrument_key": row["_security_id"],
                     "name": row.get("DISPLAY_NAME") or row.get("SEM_CUSTOM_SYMBOL") or row.get("SYMBOL_NAME") or ""}


def fetch_dhan_index(security_id: str, interval: str, start: _dt.datetime, end: _dt.datetime) -> List[Dict[str, Any]]:
    token = os.getenv("DHAN_ACCESS_TOKEN", "").strip()
    if not token:
        raise FallbackError("CREDENTIAL_MISSING: DHAN_ACCESS_TOKEN is not configured")
    imap = {"1m": "1", "5m": "5", "15m": "15"}
    if interval not in imap:
        raise FallbackError("Dhan fallback currently supports intraday 1m/5m/15m only")
    payload = {"securityId": str(security_id), "exchangeSegment": "IDX_I", "instrument": "INDEX",
               "interval": imap[interval], "oi": False,
               "fromDate": start.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S"),
               "toDate": end.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S")}
    r = requests.post(f"{DHAN_BASE}/charts/intraday", headers={"Accept": "application/json", "Content-Type": "application/json",
                       "access-token": token}, json=payload, timeout=HTTP_TIMEOUT, stream=True)
    if r.status_code != 200:
        detail = response_error(r)
        raise FallbackError(f"Dhan historical HTTP {r.status_code}: {detail}")
    d = response_json(r); n = len(d.get("timestamp") or [])
    out = []
    for i in range(n):
        out.append({"ts": int(d["timestamp"][i]), "o": d["open"][i], "h": d["high"][i], "l": d["low"][i],
                    "c": d["close"][i], "v": (d.get("volume") or [0] * n)[i]})
    return out


def provider_capabilities() -> List[Dict[str, str]]:
    return [
        {"provider": "INDstocks", "configured": "yes", "purpose": "primary stock/index OHLCV + live execution/depth"},
        {"provider": "Upstox", "configured": "yes" if upstox_configured() else "no", "purpose": "fallback indices/VIX/global/fundamentals/FII-DII"},
        {"provider": "Dhan", "configured": "yes" if dhan_configured() else "no", "purpose": "optional index OHLCV fallback"},
    ]
