"""
Extra research datasets for the historical export (everything that is NOT an INDstocks candle).

Design rules -- these exist because the export must never be made less reliable by a dataset
that lives on somebody else's website:

  * EVERY dataset is independent and best-effort. A failure in one is caught, recorded in
    quality/dataset_status.csv (status + the actual error text) and the export carries on.
    A dataset is never silently empty: it is either written with rows, written header-only
    with status OK_EMPTY, or absent with a status that says why.
  * Nothing is invented. Where a field is not in the source it is left blank. Anything this
    module PARSES out of free text (event type, split/bonus price factor) is put in a column
    whose name says it was parsed, next to the original text.
  * Point-in-time honesty. Files that are a snapshot of TODAY's state say so in their
    `is_point_in_time` / `source` columns. News/announcement/results rows carry the source's own
    timestamps so look-ahead can be checked, never a timestamp derived by us.
  * Bounded memory. Rows are streamed straight into the ZIP entry (zipfile's streaming writer);
    nothing is accumulated, so a busy announcements season cannot grow the process.
  * Bounded time. A total time budget, a per-request timeout, request pacing, and "give up on a
    dataset after N consecutive failures" (NSE blocks many cloud IP ranges -- fail fast, say so).

NSE's public website endpoints are UNOFFICIAL and can change or block a cloud server at any time;
the status file is how you tell which datasets actually arrived.
"""
import csv
import datetime
import io
import json
import os
import re
import time
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional
from zoneinfo import ZoneInfo

import requests
from export_streaming import response_json

import historical_fallbacks as fallbacks

IST = ZoneInfo("Asia/Kolkata")

EXTRAS_TIME_BUDGET_SECONDS = float(os.environ.get("HISTORICAL_EXPORT_EXTRAS_BUDGET_SECONDS", "900"))
NSE_REQUEST_TIMEOUT = float(os.environ.get("HISTORICAL_EXPORT_NSE_TIMEOUT", "15"))
NSE_PACE_SECONDS = float(os.environ.get("HISTORICAL_EXPORT_NSE_PACE_SECONDS", "0.8"))
NSE_MAX_CONSECUTIVE_FAILURES = int(os.environ.get("HISTORICAL_EXPORT_NSE_MAX_FAILURES", "3"))
NSE_BASE = "https://www.nseindia.com"
STOOQ_URL = "https://stooq.com/q/d/l/"

# Datasets that no free provider serves historically. Listed in the status file so their absence
# is explicit rather than something you discover later.
NOT_AVAILABLE = [
    ("bid_ask_top_of_book", "Historical best bid/ask + quantities are not served by INDstocks or NSE public endpoints. "
                            "Live depth exists (indstocks_client.get_market_depth) so it can only be COLLECTED going forward."),
    ("order_book_depth", "Historical order-book depth is not served. Live only (indstocks_client.get_market_depth)."),
    ("tick_trade_data", "Historical tick/trade prints are not served by INDstocks; 1-minute candles are the finest history."),
    ("pre_open_auction", "NSE publishes pre-open (indicative open / equilibrium price / matched qty) only for the CURRENT session; "
                         "there is no historical endpoint. It can only be collected going forward by a scheduled 09:00-09:08 IST snapshot."),
    ("earnings_consensus_and_surprise", "Consensus estimates are licensed data; not available. Actuals are inside NSE XBRL filings "
                                        "(link included in results file) and are not parsed here."),
    ("point_in_time_universe_history", "Historical NSE index reconstitution is not served by an API this app can call. The universe "
                                       "files are a dated SNAPSHOT (is_point_in_time=False); keep each export's snapshot to build real "
                                       "history going forward."),
    ("point_in_time_sector_history", "Same as above for sector classification: the app's sector map is today's, applied backwards."),
]

# --------------------------------------------------------------------------------------
# Context + generic streaming writer
# --------------------------------------------------------------------------------------


class ExtrasContext:
    def __init__(self, start: datetime.date, end: datetime.date, explicit, symbols: List[str], isins: List[str],
                 cancel_check: Callable[[], bool], progress: Callable[[str], None], pace: float = None, memory_check=None):
        self.start, self.end, self.explicit = start, end, explicit
        self.symbols = {str(s).upper() for s in symbols if s}
        self.isins = {str(s).upper() for s in isins if s}
        self.cancel_check = cancel_check
        self.memory_check = memory_check
        self.last_memory_check = 0.0
        self.progress = progress
        self.deadline = time.monotonic() + EXTRAS_TIME_BUDGET_SECONDS
        self.pace = NSE_PACE_SECONDS if pace is None else pace
        self.notes: List[str] = []

    def out_of_time(self) -> bool:
        return time.monotonic() > self.deadline


class ExtrasCancelled(Exception):
    pass


class DatasetError(Exception):
    pass


def _check(ctx: ExtrasContext):
    if ctx.cancel_check():
        raise ExtrasCancelled()
    if ctx.memory_check and time.monotonic() - ctx.last_memory_check >= 1.0:
        ctx.memory_check()
        ctx.last_memory_check = time.monotonic()
    if ctx.out_of_time():
        raise DatasetError("extras time budget exhausted (HISTORICAL_EXPORT_EXTRAS_BUDGET_SECONDS)")


class NseSession:
    """Browser-like session for nseindia.com's JSON endpoints (they reject bare API calls)."""

    def __init__(self):
        self.s = requests.Session()
        self.s.headers.update({
            "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/124.0 Safari/537.36"),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate",  # no 'br': brotli isn't installed
            "Referer": NSE_BASE + "/",
        })
        self.primed = False
        self._last = 0.0

    def _prime(self):
        r = self.s.get(NSE_BASE + "/", timeout=NSE_REQUEST_TIMEOUT)
        if r.status_code >= 400:
            raise DatasetError(f"nseindia.com home page returned HTTP {r.status_code} (server IP may be blocked)")
        self.primed = True

    def get_json(self, path: str, params: Dict[str, Any], pace: float):
        if not self.primed:
            self._prime()
        for attempt in range(2):
            wait = self._last + pace - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            r = self.s.get(NSE_BASE + path, params=params, timeout=NSE_REQUEST_TIMEOUT, stream=True)
            self._last = time.monotonic()
            if r.status_code in (401, 403) and attempt == 0:
                r.close()
                self.primed = False
                self._prime()
                continue
            if r.status_code != 200:
                r.close()
                raise DatasetError(f"GET {path} -> HTTP {r.status_code}")
            try:
                return response_json(r)
            except ValueError:
                raise DatasetError(f"GET {path} returned invalid or oversized JSON")
        raise DatasetError(f"GET {path} failed")


def _nse_date(d: datetime.date) -> str:
    return d.strftime("%d-%m-%Y")


def _chunks(start: datetime.date, end: datetime.date, days: int) -> Iterator[tuple]:
    cur = start
    while cur <= end:
        e = min(end, cur + datetime.timedelta(days=days - 1))
        yield cur, e
        cur = e + datetime.timedelta(days=1)


def _wanted_day(ctx: ExtrasContext, d: Optional[datetime.date]) -> bool:
    if d is None:
        return True
    if ctx.explicit is not None:
        return d in ctx.explicit
    return ctx.start <= d <= ctx.end


def _pick(item: Dict[str, Any], *keys) -> Any:
    for k in keys:
        if k in item and item[k] not in (None, ""):
            return item[k]
    return ""


_TS_FORMATS = ("%d-%b-%Y %H:%M:%S", "%d-%b-%Y %H:%M", "%d-%m-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
               "%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d")


def parse_nse_time(value: Any) -> Optional[datetime.datetime]:
    """NSE timestamps are IST wall-clock. Returns an aware IST datetime, or None if unparseable
    (the caller keeps the original text in its own column either way)."""
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in _TS_FORMATS:
        try:
            return datetime.datetime.strptime(text, fmt).replace(tzinfo=IST)
        except ValueError:
            continue
    return None


def _iso(value: Any) -> str:
    dt = parse_nse_time(value)
    return dt.isoformat() if dt else ""


def _date_of(value: Any) -> Optional[datetime.date]:
    dt = parse_nse_time(value)
    return dt.date() if dt else None


# --------------------------------------------------------------------------------------
# Corporate-action text parsing (clearly labelled as parsed, never authoritative)
# --------------------------------------------------------------------------------------

_NUM = r"([0-9]+(?:\.[0-9]+)?)"


def classify_action(subject: str) -> str:
    s = (subject or "").lower()
    rules = (
        ("DEMERGER", ("demerger", "de-merger")),
        ("MERGER_AMALGAMATION", ("amalgamation", "merger")),
        ("SCHEME_OF_ARRANGEMENT", ("scheme of arrangement", "arrangement")),
        ("BUYBACK", ("buy back", "buyback", "buy-back")),
        ("BONUS", ("bonus",)),
        ("SPLIT_SUBDIVISION", ("split", "sub-division", "sub division", "subdivision")),
        ("CONSOLIDATION", ("consolidation",)),
        ("RIGHTS", ("rights",)),
        ("DIVIDEND", ("dividend",)),
        ("FACE_VALUE_CHANGE", ("face value",)),
    )
    for label, needles in rules:
        if any(n in s for n in needles):
            return label
    return "OTHER"


def parse_price_factor(event_type: str, subject: str):
    """Returns (factor, note). `factor` multiplies PRE-ex-date raw prices to make them comparable
    with post-ex-date prices (split 10->2 gives 0.2). Blank when the text is not a clean split/bonus/
    consolidation pattern. This is parsed from free text: VERIFY before adjusting anything."""
    s = (subject or "")
    if event_type in ("SPLIT_SUBDIVISION", "CONSOLIDATION", "FACE_VALUE_CHANGE"):
        m = re.search(r"from\s*(?:rs\.?|re\.?|inr)?\s*" + _NUM + r".*?to\s*(?:rs\.?|re\.?|inr)?\s*" + _NUM, s, re.I | re.S)
        if m:
            old_fv, new_fv = float(m.group(1)), float(m.group(2))
            if old_fv > 0 and new_fv > 0:
                return round(new_fv / old_fv, 8), f"face value {old_fv:g} -> {new_fv:g}"
    if event_type == "BONUS":
        m = re.search(r"(\d+)\s*:\s*(\d+)", s)
        if m:
            a, b = int(m.group(1)), int(m.group(2))  # NSE convention: a bonus shares for every b held
            if a > 0 and b > 0:
                return round(b / (a + b), 8), f"bonus {a}:{b} (a new for every b held)"
    return "", ""


# --------------------------------------------------------------------------------------
# Dataset fetchers: generators that yield row dicts. They raise DatasetError on failure.
# --------------------------------------------------------------------------------------

CA_COLUMNS = ["symbol", "isin", "company_name", "series", "event_type_parsed", "subject", "face_value",
              "ex_date", "record_date", "book_closure_start", "book_closure_end", "no_delivery_start", "no_delivery_end",
              "parsed_price_factor", "parse_note", "announcement_date", "in_universe", "source", "raw_json"]


def fetch_corporate_actions(ctx: ExtrasContext, nse: NseSession) -> Iterator[Dict[str, Any]]:
    fails = 0
    # Look a little past the window: an action announced in-range often goes ex just after it.
    for c_start, c_end in _chunks(ctx.start, ctx.end + datetime.timedelta(days=30), 31):
        _check(ctx)
        try:
            data = nse.get_json("/api/corporates-corporateActions",
                                {"index": "equities", "from_date": _nse_date(c_start), "to_date": _nse_date(c_end)}, ctx.pace)
            fails = 0
        except (DatasetError, requests.RequestException) as e:
            fails += 1
            if fails >= NSE_MAX_CONSECUTIVE_FAILURES:
                raise DatasetError(f"{fails} consecutive failures; last: {e}")
            continue
        for it in (data if isinstance(data, list) else data.get("data") or []):
            sym = str(_pick(it, "symbol")).upper()
            isin = str(_pick(it, "isin")).upper()
            in_uni = (sym in ctx.symbols) or (isin in ctx.isins)
            if not in_uni:
                continue
            subject = str(_pick(it, "subject", "purpose"))
            etype = classify_action(subject)
            factor, note = parse_price_factor(etype, subject)
            yield {
                "symbol": sym, "isin": isin, "company_name": _pick(it, "comp", "companyName"), "series": _pick(it, "series"),
                "event_type_parsed": etype, "subject": subject, "face_value": _pick(it, "faceVal"),
                "ex_date": _pick(it, "exDate"), "record_date": _pick(it, "recDate"),
                "book_closure_start": _pick(it, "bcStartDate"), "book_closure_end": _pick(it, "bcEndDate"),
                "no_delivery_start": _pick(it, "ndStartDate"), "no_delivery_end": _pick(it, "ndEndDate"),
                "parsed_price_factor": factor, "parse_note": note,
                "announcement_date": "",  # NSE's corporate-actions list does not carry it
                "in_universe": True, "source": "nseindia.com /api/corporates-corporateActions",
                "raw_json": json.dumps(it, ensure_ascii=False, separators=(",", ":")),
            }


ANN_COLUMNS = ["symbol", "isin", "company_name", "publication_timestamp", "publication_timestamp_raw",
               "exchange_received_timestamp", "exchange_received_timestamp_raw", "event_type", "headline",
               "document_reference", "sequence_id", "source"]


def fetch_announcements(ctx: ExtrasContext, nse: NseSession) -> Iterator[Dict[str, Any]]:
    fails = 0
    day = ctx.start
    while day <= ctx.end:
        _check(ctx)
        if not _wanted_day(ctx, day) or day.weekday() >= 5:
            day += datetime.timedelta(days=1)
            continue
        try:
            data = nse.get_json("/api/corporate-announcements",
                                {"index": "equities", "from_date": _nse_date(day), "to_date": _nse_date(day)}, ctx.pace)
            fails = 0
        except (DatasetError, requests.RequestException) as e:
            fails += 1
            if fails >= NSE_MAX_CONSECUTIVE_FAILURES:
                raise DatasetError(f"{fails} consecutive failures; last: {e}")
            day += datetime.timedelta(days=1)
            continue
        for it in (data if isinstance(data, list) else data.get("data") or []):
            sym = str(_pick(it, "symbol")).upper()
            isin = str(_pick(it, "sm_isin", "isin")).upper()
            if sym not in ctx.symbols and isin not in ctx.isins:
                continue
            pub_raw = _pick(it, "an_dt", "sort_date")
            rec_raw = _pick(it, "exchdisstime", "dt")
            yield {
                "symbol": sym, "isin": isin, "company_name": _pick(it, "sm_name", "comp"),
                "publication_timestamp": _iso(pub_raw), "publication_timestamp_raw": pub_raw,
                "exchange_received_timestamp": _iso(rec_raw), "exchange_received_timestamp_raw": rec_raw,
                "event_type": _pick(it, "desc"), "headline": _pick(it, "attchmntText", "subject"),
                "document_reference": _pick(it, "attchmntFile"), "sequence_id": _pick(it, "seq_id"),
                "source": "nseindia.com /api/corporate-announcements",
            }
        day += datetime.timedelta(days=1)


RES_COLUMNS = ["symbol", "isin", "company_name", "period_end", "period_type", "filing_or_broadcast_timestamp",
               "filing_or_broadcast_timestamp_raw", "audited", "consolidated", "xbrl_document", "source", "raw_json"]


def fetch_results(ctx: ExtrasContext, nse: NseSession) -> Iterator[Dict[str, Any]]:
    fails = 0
    for c_start, c_end in _chunks(ctx.start, ctx.end, 31):
        _check(ctx)
        try:
            data = nse.get_json("/api/corporates-financial-results",
                                {"index": "equities", "period": "Quarterly",
                                 "from_date": _nse_date(c_start), "to_date": _nse_date(c_end)}, ctx.pace)
            fails = 0
        except (DatasetError, requests.RequestException) as e:
            fails += 1
            if fails >= NSE_MAX_CONSECUTIVE_FAILURES:
                raise DatasetError(f"{fails} consecutive failures; last: {e}")
            continue
        for it in (data if isinstance(data, list) else data.get("data") or []):
            sym = str(_pick(it, "symbol")).upper()
            isin = str(_pick(it, "isin")).upper()
            if sym not in ctx.symbols and isin not in ctx.isins:
                continue
            ts_raw = _pick(it, "broadCastDate", "filingDate", "creation_Date")
            yield {
                "symbol": sym, "isin": isin, "company_name": _pick(it, "companyName", "comp"),
                "period_end": _pick(it, "toDate", "period_end"), "period_type": _pick(it, "period", "resultsFor"),
                "filing_or_broadcast_timestamp": _iso(ts_raw), "filing_or_broadcast_timestamp_raw": ts_raw,
                "audited": _pick(it, "audited"), "consolidated": _pick(it, "consolidated"),
                "xbrl_document": _pick(it, "xbrl", "attachment"),
                "source": "nseindia.com /api/corporates-financial-results",
                "raw_json": json.dumps(it, ensure_ascii=False, separators=(",", ":")),
            }


DEAL_COLUMNS = ["date", "symbol", "security_name", "client_name", "buy_sell", "quantity", "weighted_avg_price",
                "value_inr_computed", "remarks", "deal_type", "in_universe", "source"]


def _fetch_deals(ctx: ExtrasContext, nse: NseSession, kind: str) -> Iterator[Dict[str, Any]]:
    path = f"/api/historical/{kind}-deals"
    fails = 0
    for c_start, c_end in _chunks(ctx.start, ctx.end, 31):
        _check(ctx)
        try:
            data = nse.get_json(path, {"from": _nse_date(c_start), "to": _nse_date(c_end)}, ctx.pace)
            fails = 0
        except (DatasetError, requests.RequestException) as e:
            fails += 1
            if fails >= NSE_MAX_CONSECUTIVE_FAILURES:
                raise DatasetError(f"{fails} consecutive failures; last: {e}")
            continue
        for it in (data.get("data") if isinstance(data, dict) else data) or []:
            sym = str(_pick(it, "BD_SYMBOL", "symbol")).upper()
            d = _pick(it, "BD_DT_DATE", "date")
            if not _wanted_day(ctx, _date_of(d)):
                continue
            try:
                qty = float(str(_pick(it, "BD_QTY_TRD", "quantity")).replace(",", ""))
                px = float(str(_pick(it, "BD_TP_WATP", "price")).replace(",", ""))
                value = round(qty * px, 2)
            except ValueError:
                value = ""
            yield {
                "date": d, "symbol": sym, "security_name": _pick(it, "BD_SCRIP_NAME"),
                "client_name": _pick(it, "BD_CLIENT_NAME"), "buy_sell": _pick(it, "BD_BUY_SELL"),
                "quantity": _pick(it, "BD_QTY_TRD"), "weighted_avg_price": _pick(it, "BD_TP_WATP"),
                "value_inr_computed": value, "remarks": _pick(it, "BD_REMARKS"), "deal_type": kind.upper(),
                "in_universe": sym in ctx.symbols, "source": f"nseindia.com {path}",
            }


def fetch_bulk_deals(ctx, nse):
    return _fetch_deals(ctx, nse, "bulk")


def fetch_block_deals(ctx, nse):
    return _fetch_deals(ctx, nse, "block")


FII_COLUMNS = ["date", "category", "buy_value_cr", "sell_value_cr", "net_value_cr", "source", "note"]


def fetch_fii_dii(ctx: ExtrasContext, nse: NseSession) -> Iterator[Dict[str, Any]]:
    _check(ctx)
    try:
        data = nse.get_json("/api/fiidiiTradeReact", {}, ctx.pace)
    except (DatasetError, requests.RequestException) as e:
        raise DatasetError(str(e))
    for it in (data if isinstance(data, list) else data.get("data") or []):
        yield {
            "date": _pick(it, "date"), "category": _pick(it, "category"),
            "buy_value_cr": _pick(it, "buyValue"), "sell_value_cr": _pick(it, "sellValue"), "net_value_cr": _pick(it, "netValue"),
            "source": "nseindia.com /api/fiidiiTradeReact",
            "note": "LATEST TRADING DAY ONLY: NSE serves no history from this endpoint. Not a range series.",
        }


# --------------------------------------------------------------------------------------
# Global / overnight DAILY series (Stooq CSV). Daily resolution only -- and every row says WHEN
# it became knowable in IST, because a same-day Asian close is NOT known at India's 09:15 open.
# --------------------------------------------------------------------------------------

# stem, stooq symbol, description, exchange tz, local close time (None = ~24h market)
GLOBAL_SERIES = [
    ("SP500", "^spx", "S&P 500", "America/New_York", datetime.time(16, 0)),
    ("NASDAQ_COMPOSITE", "^ndq", "Nasdaq composite", "America/New_York", datetime.time(16, 0)),
    ("DOW_JONES", "^dji", "Dow Jones Industrial Average", "America/New_York", datetime.time(16, 0)),
    ("US_VIX", "^vix", "CBOE VIX", "America/New_York", datetime.time(16, 15)),
    ("NIKKEI_225", "^nkx", "Nikkei 225", "Asia/Tokyo", datetime.time(15, 30)),
    ("HANG_SENG", "^hsi", "Hang Seng", "Asia/Hong_Kong", datetime.time(16, 0)),
    ("SHANGHAI_COMPOSITE", "^shc", "Shanghai Composite", "Asia/Shanghai", datetime.time(15, 0)),
    ("USDINR", "usdinr", "USD/INR", None, None),
    ("CRUDE_OIL_WTI", "cl.f", "WTI crude oil futures (continuous)", None, None),
    ("GOLD", "gc.f", "Gold futures (continuous)", None, None),
    ("US_10Y_YIELD", "10usy.b", "US 10-year treasury yield", None, None),
]
GLOBAL_COLUMNS = ["date", "open", "high", "low", "close", "volume", "known_from_ist", "known_from_rule", "source"]


def _known_from_ist(day: datetime.date, tz: Optional[str], close: Optional[datetime.time]) -> tuple:
    if tz and close:
        dt = datetime.datetime.combine(day, close, tzinfo=ZoneInfo(tz)).astimezone(IST)
        return dt.isoformat(), f"exchange close {close.strftime('%H:%M')} {tz} converted to IST"
    # ~24h markets: the daily bar's close convention varies by vendor -> only treat it as known from the next IST day.
    nxt = datetime.datetime.combine(day + datetime.timedelta(days=1), datetime.time.min, tzinfo=IST)
    return nxt.isoformat(), "continuous market: conservatively known from 00:00 IST of the next day"


def fetch_global_series(ctx: ExtrasContext, stem: str, symbol: str, tz: Optional[str], close: Optional[datetime.time]) -> Iterator[Dict[str, Any]]:
    _check(ctx)
    d1 = (ctx.start - datetime.timedelta(days=7)).strftime("%Y%m%d")
    d2 = ctx.end.strftime("%Y%m%d")
    try:
        r = requests.get(STOOQ_URL, params={"s": symbol, "i": "d", "d1": d1, "d2": d2}, timeout=NSE_REQUEST_TIMEOUT,
                         headers={"User-Agent": "Mozilla/5.0"})
    except requests.RequestException as e:
        raise DatasetError(f"stooq request failed: {e}")
    text = r.text or ""
    if r.status_code != 200 or not text.lower().startswith("date"):
        raise DatasetError(f"stooq did not return CSV (HTTP {r.status_code}: {text[:80]!r})")
    for row in csv.DictReader(io.StringIO(text)):
        try:
            day = datetime.date.fromisoformat(row.get("Date", ""))
        except ValueError:
            continue
        known, rule = _known_from_ist(day, tz, close)
        yield {"date": day.isoformat(), "open": row.get("Open", ""), "high": row.get("High", ""), "low": row.get("Low", ""),
               "close": row.get("Close", ""), "volume": row.get("Volume", ""), "known_from_ist": known,
               "known_from_rule": rule, "source": f"stooq.com {symbol}"}


# --------------------------------------------------------------------------------------
# Documented fallback-provider normalizers
# --------------------------------------------------------------------------------------

UPSTOX_CA_COLUMNS = ["isin", "event_name", "expiry_date", "amount", "ratio", "announcement_date",
                     "ex_date", "record_date", "details", "source", "raw_json"]
UPSTOX_PROFILE_COLUMNS = ["isin", "sector", "industry", "company_name", "source", "raw_json"]
UPSTOX_INCOME_COLUMNS = ["isin", "source", "causal_timestamp_available", "raw_json"]
UPSTOX_FLOW_COLUMNS = ["participant", "source", "causal_timestamp_available", "raw_json"]
UPSTOX_NEWS_COLUMNS = ["coverage_note", "source", "raw_json"]


def _detail_map(event: Dict[str, Any]) -> Dict[str, Any]:
    out = {}
    for d in event.get("event_details") or []:
        if isinstance(d, dict) and d.get("name"):
            out[str(d.get("name")).strip().lower()] = d.get("value")
    return out


def fetch_upstox_actions_fallback(ctx: ExtrasContext) -> Iterator[Dict[str, Any]]:
    """Official Upstox corporate-actions fallback, one ISIN at a time.

    This is invoked only when the NSE public route failed/returned nothing. Upstox documents
    announcement/ex/record dates and event details for dividend/bonus/split/rights events.
    """
    for isin in sorted(ctx.isins):
        _check(ctx)
        try:
            rows = fallbacks.fetch_upstox_corporate_actions(isin)
        except Exception as e:
            if "CREDENTIAL_MISSING" in str(e):
                raise DatasetError(str(e))
            # One malformed/unavailable ISIN must not kill the whole universe.
            continue
        for event in rows or []:
            if not isinstance(event, dict):
                continue
            dm = _detail_map(event)
            yield {
                "isin": isin,
                "event_name": event.get("name", ""),
                "expiry_date": event.get("expiry_date", ""),
                "amount": event.get("amount", ""),
                "ratio": event.get("ratio", ""),
                "announcement_date": dm.get("announcement date", ""),
                "ex_date": dm.get("ex dividend date", dm.get("ex date", event.get("expiry_date", ""))),
                "record_date": dm.get("record date", ""),
                "details": dm.get("details", ""),
                "source": "Upstox /v2/fundamentals/{isin}/corporate-actions",
                "raw_json": json.dumps(event, ensure_ascii=False, separators=(",", ":")),
            }


def fetch_upstox_profiles_fallback(ctx: ExtrasContext) -> Iterator[Dict[str, Any]]:
    """Current company profile/sector metadata. This is NOT point-in-time history."""
    for isin in sorted(ctx.isins):
        _check(ctx)
        try:
            row = fallbacks.fetch_upstox_company_profile(isin)
        except Exception as e:
            if "CREDENTIAL_MISSING" in str(e):
                raise DatasetError(str(e))
            continue
        if not isinstance(row, dict) or not row:
            continue
        yield {
            "isin": isin,
            "sector": _pick(row, "sector", "sector_name", "sectorName"),
            "industry": _pick(row, "industry", "industry_name", "industryName"),
            "company_name": _pick(row, "company_name", "companyName", "name"),
            "source": "Upstox /v2/fundamentals/{isin}/profile (current snapshot, not point-in-time)",
            "raw_json": json.dumps(row, ensure_ascii=False, separators=(",", ":")),
        }


def fetch_upstox_income_fallback(ctx: ExtrasContext) -> Iterator[Dict[str, Any]]:
    """Quarterly financial statements. Does NOT substitute for historical filing timestamps."""
    for isin in sorted(ctx.isins):
        _check(ctx)
        try:
            row = fallbacks.fetch_upstox_income_statement(isin)
        except Exception as e:
            if "CREDENTIAL_MISSING" in str(e):
                raise DatasetError(str(e))
            continue
        if not row:
            continue
        yield {
            "isin": isin, "source": "Upstox company fundamentals income-statement",
            "causal_timestamp_available": "no — financial values are reference data; do not expose to a decision before a verified filing timestamp",
            "raw_json": json.dumps(row, ensure_ascii=False, separators=(",", ":")),
        }


def fetch_upstox_fii_dii_fallback(ctx: ExtrasContext) -> Iterator[Dict[str, Any]]:
    try:
        payload = fallbacks.fetch_upstox_fii_dii(ctx.start, ctx.end)
    except Exception as e:
        raise DatasetError(str(e))
    for participant in ("fii", "dii"):
        data = payload.get(participant)
        # Preserve the exact official response; response schemas may evolve and research code
        # should explicitly normalize a version rather than silently assuming fields here.
        rows = data if isinstance(data, list) else [data]
        for row in rows:
            if row in (None, {}, []):
                continue
            yield {"participant": participant.upper(), "source": f"Upstox /v2/market/{participant}",
                   "causal_timestamp_available": "daily/monthly market-information series",
                   "raw_json": json.dumps(row, ensure_ascii=False, separators=(",", ":"))}



def fetch_upstox_recent_news_fallback(ctx: ExtrasContext) -> Iterator[Dict[str, Any]]:
    """Recent Upstox news fallback. Official API coverage is only the past 7 days.

    This can enrich the tail of a recent export but must NEVER be interpreted as complete
    historical news for an older backtest window.
    """
    keys = [f"NSE_EQ|{isin}" for isin in sorted(ctx.isins)]
    for i in range(0, len(keys), 30):
        batch = keys[i:i + 30]
        page = 1
        while page <= 100:
            _check(ctx)
            try:
                payload = fallbacks.fetch_upstox_news(batch, page_number=page, page_size=100)
            except Exception as e:
                raise DatasetError(str(e))
            if not isinstance(payload, dict):
                break
            any_rows = False
            for instrument_key, items in payload.items():
                if not isinstance(items, list):
                    continue
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    any_rows = True
                    yield {"coverage_note": "Upstox News API returns only the past 7 days; PARTIAL historical coverage",
                           "source": f"Upstox /v2/news {instrument_key}",
                           "raw_json": json.dumps(item, ensure_ascii=False, separators=(",", ":"))}
            # upstox_get returns only `data`, not metadata. If a full page is returned we cannot
            # safely infer there is another page from data shape across instruments, so stop at one
            # page per 30-key batch rather than risk duplicate/looping requests.
            break


def fetch_upstox_global_fallback(ctx: ExtrasContext, aliases: List[str], stem: str,
                                  tz: Optional[str], close: Optional[datetime.time]) -> Iterator[Dict[str, Any]]:
    try:
        candles, meta = fallbacks.fetch_upstox_global(aliases, "1d", ctx.start - datetime.timedelta(days=7), ctx.end)
    except Exception as e:
        raise DatasetError(str(e))
    for c in candles:
        dt = datetime.datetime.fromtimestamp(int(c["ts"]), tz=IST)
        day = dt.date()
        known, rule = _known_from_ist(day, tz, close)
        yield {"date": day.isoformat(), "open": c.get("o", ""), "high": c.get("h", ""),
               "low": c.get("l", ""), "close": c.get("c", ""), "volume": c.get("v", ""),
               "known_from_ist": known, "known_from_rule": rule,
               "source": f"Upstox {meta.get('instrument_key','')} ({meta.get('name','')})"}


UPSTOX_GLOBAL_ALIASES = {
    "SP500": ["S&P", "S&P 500"],
    "DOW_JONES": ["DOW JONES", "US 30"],
    "NIKKEI_225": ["NIKKEI 225"],
    "HANG_SENG": ["HANG SENG"],
    "USDINR": ["USD INR"],
    "CRUDE_OIL_WTI": ["Oil (WTI)"],
    # Do not silently substitute US Tech 100 for Nasdaq Composite, or Brent for WTI.
}

# GIFT NIFTY is explicitly documented in Upstox Global Instruments and can be queried with
# Historical Candle Data V3. It is exported separately because it is an overnight/opening feature.
GIFT_NIFTY_ALIASES = ["GIFT NIFTY"]

# --------------------------------------------------------------------------------------
# Runner
# --------------------------------------------------------------------------------------

STATUS_COLUMNS = ["dataset", "file", "status", "rows", "resolution", "point_in_time", "source", "note"]


def _stream_dataset(zf, path: str, columns: List[str], rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    """Stream rows into ONE zip entry. Returns {'rows': n, 'error': str|None, 'wrote': bool}.
    The entry is created on the first row (or header-only if the source finished cleanly with none)."""
    n, err, entry, writer, text = 0, None, None, None, None

    def _open():
        nonlocal entry, writer, text
        entry = zf.open(path, "w", force_zip64=True)
        text = io.TextIOWrapper(entry, encoding="utf-8", newline="")
        writer = csv.DictWriter(text, fieldnames=columns, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()

    try:
        for row in rows:
            if writer is None:
                _open()
            writer.writerow(row)
            n += 1
    except ExtrasCancelled:
        if text:
            text.close()
        raise
    except (DatasetError, requests.RequestException) as e:
        err = str(e)[:300]
    except Exception as e:  # a source changing shape must never take the export down
        err = f"{type(e).__name__}: {e}"[:300]
    if writer is None and err is None:
        _open()  # finished cleanly, zero rows -> header-only file, status OK_EMPTY
    if text:
        text.close()
    return {"rows": n, "error": err, "wrote": writer is not None}


def run_extras(zf, ctx: ExtrasContext) -> List[Dict[str, Any]]:
    """Fetch every best-effort research dataset and transparently chain documented fallbacks.

    Source precedence:
      1) existing primary source (NSE public pages / Stooq),
      2) Upstox official API when configured and the primary failed/was empty,
      3) explicit NOT_AVAILABLE only where no historical route is connected.

    Every attempt receives its own dataset-status row. A fallback never hides the primary failure.
    """
    statuses: List[Dict[str, Any]] = []
    nse = NseSession()

    def add(dataset, path, columns, gen_factory, resolution, pit, source, note=""):
        ctx.progress(f"Fetching extra dataset: {dataset}...")
        if ctx.out_of_time():
            rec = {"dataset": dataset, "file": "", "status": "SKIPPED_TIME_BUDGET", "rows": 0,
                   "resolution": resolution, "point_in_time": pit, "source": source,
                   "note": "extras time budget used up"}
            statuses.append(rec); return rec
        try:
            res = _stream_dataset(zf, path, columns, gen_factory())
        except ExtrasCancelled:
            raise
        if res["error"] and res["rows"] == 0:
            status = "CREDENTIAL_MISSING" if str(res["error"]).startswith("CREDENTIAL_MISSING") else "FAILED"
        elif res["error"]:
            status = "PARTIAL"
        elif res["rows"] == 0:
            status = "OK_EMPTY"
        else:
            status = "OK"
        rec = {"dataset": dataset, "file": path if res["wrote"] else "", "status": status, "rows": res["rows"],
               "resolution": resolution, "point_in_time": pit, "source": source,
               "note": (res["error"] or note)}
        statuses.append(rec); return rec

    # ----- NSE primary routes ---------------------------------------------------------
    ca = add("corporate_actions", "events/corporate_actions.csv", CA_COLUMNS, lambda: fetch_corporate_actions(ctx, nse),
        "event", "yes: as listed by NSE (dates are the exchange's own)", "NSE website (unofficial JSON)",
        "Filtered to universe. Price factor is PARSED from the subject text -- verify before adjusting.")
    ann = add("corporate_announcements", "events/announcements.csv", ANN_COLUMNS, lambda: fetch_announcements(ctx, nse),
        "event (source timestamps)", "yes: source timestamps preserved; decision rule = usable only at/after exchange_received_timestamp",
        "NSE website (unofficial JSON)", "Filtered to universe.")
    res = add("financial_results", "events/results.csv", RES_COLUMNS, lambda: fetch_results(ctx, nse),
        "event (source timestamps)", "yes for timestamps; figures NOT included (they live in the XBRL document)",
        "NSE website (unofficial JSON)", "No revenue/EBITDA/PAT/EPS: not in this list, only in the linked XBRL filing.")
    add("bulk_deals", "events/bulk_deals.csv", DEAL_COLUMNS, lambda: fetch_bulk_deals(ctx, nse), "event", "yes",
        "NSE website (unofficial JSON)", "One row per client per deal side as NSE lists it.")
    add("block_deals", "events/block_deals.csv", DEAL_COLUMNS, lambda: fetch_block_deals(ctx, nse), "event", "yes",
        "NSE website (unofficial JSON)", "One row per client per deal side as NSE lists it.")
    fii = add("fii_dii_cash", "flows/fii_dii_latest.csv", FII_COLUMNS, lambda: fetch_fii_dii(ctx, nse), "single latest day", "n/a",
        "NSE website (unofficial JSON)", "Latest day only; primary NSE public endpoint is not a range series.")

    # ----- Upstox official fallbacks / supplements -----------------------------------
    # Corporate actions have a documented ISIN endpoint with announcement/ex/record dates.
    if ca["status"] in ("FAILED", "OK_EMPTY", "PARTIAL"):
        add("corporate_actions_upstox_fallback", "events/corporate_actions_upstox.csv", UPSTOX_CA_COLUMNS,
            lambda: fetch_upstox_actions_fallback(ctx), "event", "source event dates",
            "Upstox Company Fundamentals API",
            "FALLBACK for NSE public-route failure. Requires UPSTOX_ACCESS_TOKEN.")

    # A current profile can fill missing sector labels, but it is explicitly NOT historical point-in-time membership.
    add("company_profiles_upstox_snapshot", "reference/company_profiles_upstox.csv", UPSTOX_PROFILE_COLUMNS,
        lambda: fetch_upstox_profiles_fallback(ctx), "current snapshot", "NO — current profile only",
        "Upstox Company Fundamentals API", "Supplemental current sector/industry metadata; never apply blindly as PIT history.")

    # Financial values are useful reference features but cannot replace filing timestamps if the NSE route failed.
    if res["status"] in ("FAILED", "OK_EMPTY", "PARTIAL"):
        add("income_statements_upstox_reference", "fundamentals/income_statements_upstox.csv", UPSTOX_INCOME_COLUMNS,
            lambda: fetch_upstox_income_fallback(ctx), "quarterly statements", "NO causal filing timestamp in this export",
            "Upstox Company Fundamentals API",
            "Reference-only fallback: do not expose values to historical decisions without a verified publication timestamp.")

    if fii["status"] in ("FAILED", "OK_EMPTY", "PARTIAL"):
        add("fii_dii_cash_upstox_fallback", "flows/fii_dii_upstox.csv", UPSTOX_FLOW_COLUMNS,
            lambda: fetch_upstox_fii_dii_fallback(ctx), "daily/monthly (requested daily)", "daily series",
            "Upstox Market Information APIs", "FALLBACK for blocked NSE public endpoint.")

    # Recent news can only cover the API's trailing seven days. It is useful for recent exports but is never
    # claimed as a substitute for full historical exchange announcements.
    if ann["status"] in ("FAILED", "OK_EMPTY", "PARTIAL"):
        add("news_upstox_recent_partial", "events/news_upstox_recent.csv", UPSTOX_NEWS_COLUMNS,
            lambda: fetch_upstox_recent_news_fallback(ctx), "event", "published timestamp in raw payload",
            "Upstox News API", "PARTIAL COVERAGE: official API exposes only the past 7 days.")

    # ----- Global / overnight ----------------------------------------------------------
    global_primary = {}
    for stem, symbol, desc, tz, close in GLOBAL_SERIES:
        global_primary[stem] = add(f"global_{stem}", f"global/{stem}.csv", GLOBAL_COLUMNS,
            (lambda st=stem, sy=symbol, z=tz, c=close: fetch_global_series(ctx, st, sy, z, c)),
            "daily close only (no intraday)", "yes: `known_from_ist` says when each value became knowable in India",
            "stooq.com", f"{desc}. Use known_from_ist, not date, to decide what was known before the 09:15 IST open.")

    # If Stooq fails, use only exact officially listed Upstox global instruments. We intentionally do not
    # substitute similar-but-different benchmarks (e.g. US Tech 100 for Nasdaq Composite).
    global_meta = {stem: (tz, close) for stem, _symbol, _desc, tz, close in GLOBAL_SERIES}
    for stem, primary in global_primary.items():
        if primary["status"] not in ("FAILED", "OK_EMPTY", "PARTIAL"):
            continue
        aliases = UPSTOX_GLOBAL_ALIASES.get(stem)
        if not aliases:
            statuses.append({"dataset": f"global_{stem}_upstox_fallback", "file": "", "status": "NOT_SUPPORTED_EXACT_MATCH",
                             "rows": 0, "resolution": "daily", "point_in_time": "", "source": "Upstox Global Instruments",
                             "note": "No exact documented Upstox global instrument mapping; similar instruments are not substituted."})
            continue
        tz, close = global_meta[stem]
        add(f"global_{stem}_upstox_fallback", f"global/{stem}_UPSTOX.csv", GLOBAL_COLUMNS,
            lambda a=aliases, st=stem, z=tz, c=close: fetch_upstox_global_fallback(ctx, a, st, z, c),
            "daily", "known_from_ist applied", "Upstox Global Instruments + Historical Candle V3",
            "FALLBACK for Stooq failure; exact instrument match only.")

    # GIFT NIFTY is a first-class documented Upstox global index as of May 2026.
    add("gift_nifty_overnight_upstox", "global/GIFT_NIFTY_UPSTOX.csv", GLOBAL_COLUMNS,
        lambda: fetch_upstox_global_fallback(ctx, GIFT_NIFTY_ALIASES, "GIFT_NIFTY", None, None),
        "daily in this export", "conservatively next-day known_from_ist for daily bars",
        "Upstox Global Instruments + Historical Candle V3",
        "Exact documented GIFT NIFTY instrument; requires UPSTOX_ACCESS_TOKEN.")

    # ----- Historical datasets for which no connected API currently provides the bytes -------
    for dataset, note in NOT_AVAILABLE:
        statuses.append({"dataset": dataset, "file": "", "status": "NOT_AVAILABLE", "rows": 0, "resolution": "",
                         "point_in_time": "", "source": "", "note": note})
    return statuses

