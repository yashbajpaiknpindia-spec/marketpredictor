"""
Historical market-data export ("ground truth" dataset builder).

Produces a ZIP of RAW candles straight from INDstocks -- no trades, no Replay
results, no aggregation -- so the data can be analysed independently of this
app's own decision layer:

    historical_data/<YYYY-MM-DD>/<SYMBOL>.csv   one file per stock per day
    market/NIFTY.csv, market/INDIA_VIX.csv      whole selected range, one file each
    sectors/<INDEX>.csv                         NIFTY sectoral indices, whole range
    universe/large_midcap.csv                   symbol, exchange, cap class, ...
    universe/universe_membership.csv            dated membership SNAPSHOT (not point-in-time)
    universe/sector_membership.csv              dated sector-mapping SNAPSHOT (not point-in-time)
    reference/instrument_master.csv             provider's own instrument metadata for the universe
    reference/index_instruments.csv             every index the provider lists
    reference/index_probe.csv                   which index code shape the provider accepted / rejected
    reference/trading_calendar.csv              per-date session times + holiday flags
    reference/cost_model.json                   the exact charge rates the app is configured with
    events/*.csv, flows/*.csv, global/*.csv     corporate actions, announcements, results, deals, FII/DII,
                                                overnight/global daily series (best-effort; see historical_extras.py)
    quality/data_quality.csv                    bars per symbol per day vs expected
    quality/fetch_failures.csv                  any API window that failed after retries
    quality/dataset_status.csv                  one row per extra dataset: OK / OK_EMPTY / FAILED / NOT_AVAILABLE + why
    manifest.json, README.txt

Integrity rules this module follows:
  * Candles are requested at the NATIVE interval and written as returned. There
    is no resampling / re-bucketing, so no candle is ever built from later ones.
  * Timestamps are written as IST ISO-8601 with an explicit +05:30 offset, plus
    the provider's original epoch seconds in `epoch`, so nothing is re-derived.
  * Duplicate timestamps (same candle returned twice) are de-duplicated keeping the
    last one -- the same rule the Replay engine's own loader (market_data._candles_to_df)
    applies -- and counted in the quality file.
  * A failed API window is retried, then RECORDED. A dataset never silently has a
    hole that looks like "no trading that day".

Resource rules (why a 1-minute export no longer takes the site down):
  * STREAMING. The old build fetched a stock's WHOLE date range into RAM (three batches at once, each
    a list of Python dicts, ~400 bytes per candle) before writing anything. Now every provider call
    covers ONE 7-day window for <=5 scrips, its day files are written straight into the ZIP and the
    candles are dropped. Candle buffers stay bounded; ZIP directory metadata still grows with file count.
  * BOUNDED IN-FLIGHT WORK. For 1-minute exports only ONE request is in flight and ONE scrip is requested per call by default;
    containers up to 1 GB use one request and one scrip for all intervals.
  * GENTLE BY DEFAULT. Fewer workers and a short pause between windows for 1-minute exports, so the
    web threads and the live scanner in the same process keep getting CPU and INDstocks rate-limit
    budget. Slower on purpose -- see HISTORICAL_EXPORT_* env vars below.
  * MEMORY GOVERNOR. Container working-set memory is checked before windows: above the soft limit the export
    garbage-collects and slows down; above the hard limit it WAITS for memory to come back and, if it
    doesn't, stops cleanly with a message instead of letting the host OOM-kill the whole site.
  * DISK GUARD. It refuses to start when the disk cannot hold the ZIP.

This module is deliberately independent of app.py (no circular import): the
caller resolves the universe and passes it in.

Env vars (all optional):
  HISTORICAL_EXPORT_WORKERS            parallel window fetches (always 1 on <=1 GB containers; otherwise 1 for 1m, else up to 3)
  HISTORICAL_EXPORT_PACE_SECONDS       pause after each window is written (default 0.35 for 1m, 0.1 for 5m, 0.05 for 15m)
  HISTORICAL_EXPORT_ZIP_LEVEL          deflate level 1-9 (default 3: much less CPU than 6, ~10% bigger)
  HISTORICAL_EXPORT_MEMORY_LIMIT_MB    override the detected container memory limit
  HISTORICAL_EXPORT_MEMORY_WAIT_SECONDS  how long to wait at the hard limit before stopping (default 600)
  HISTORICAL_EXPORT_1M_SCRIPS_PER_CALL    1-5, default 1 for low-RAM safety
  HISTORICAL_EXPORT_MEMORY_RESERVE_MB     RAM reserved before each provider response, default 128
  HISTORICAL_EXPORT_EXTRAS             1/0 -- include the extra datasets by default (default 1)
"""
import csv
import datetime
import gc
import ctypes
import io
import json
import os
import re
import shutil
import tempfile
import threading
import time
import uuid
import zipfile

try:
    import psycopg2
    import psycopg2.extras
except Exception:
    psycopg2 = None
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import indstocks_client as ind
import historical_extras
from export_streaming import DiskRows, write_csv_entry
import historical_fallbacks as fallbacks
import market_data
import sector_data

IST = ZoneInfo("Asia/Kolkata")

EXPORT_DIR = os.environ.get(
    "HISTORICAL_EXPORT_DIR",
    os.path.join(tempfile.gettempdir(), "marketpredictor_historical_exports"),
)
MAX_LOOKBACK_DAYS = int(os.environ.get("HISTORICAL_EXPORT_MAX_LOOKBACK_DAYS", "150"))
MAX_SPAN_DAYS = int(os.environ.get("HISTORICAL_EXPORT_MAX_SPAN_DAYS", "130"))
MAX_ROWS = int(os.environ.get("HISTORICAL_EXPORT_MAX_ROWS", "8000000"))
WINDOW_RETRIES = max(0, int(os.environ.get("HISTORICAL_EXPORT_WINDOW_RETRIES", "2")))
KEEP_EXPORTS = max(1, int(os.environ.get("HISTORICAL_EXPORT_KEEP", "3")))

_WORKERS_ENV = os.environ.get("HISTORICAL_EXPORT_WORKERS")
WORKERS = max(1, int(_WORKERS_ENV)) if _WORKERS_ENV else 3  # kept for compatibility; see _workers_for()
ZIP_LEVEL = min(9, max(1, int(os.environ.get("HISTORICAL_EXPORT_ZIP_LEVEL", "3"))))
# 1-minute exports run in deliberately low-memory mode on web-service instances.
# INDstocks allows up to 5 scrips per historical call, but a 5-scrip x 7-day JSON
# response can create a large transient Python-object spike before the memory governor
# gets control back.  Default to ONE scrip per call for 1m; this is slower but safe.
ONE_MINUTE_SCRIPS_PER_CALL = max(1, min(5, int(os.environ.get("HISTORICAL_EXPORT_1M_SCRIPS_PER_CALL", "1"))))
MEMORY_RESERVE_MB = max(64.0, float(os.environ.get("HISTORICAL_EXPORT_MEMORY_RESERVE_MB", "128")))
_PACE_ENV = os.environ.get("HISTORICAL_EXPORT_PACE_SECONDS")
MEMORY_LIMIT_ENV = os.environ.get("HISTORICAL_EXPORT_MEMORY_LIMIT_MB")
MEMORY_WAIT_SECONDS = float(os.environ.get("HISTORICAL_EXPORT_MEMORY_WAIT_SECONDS", "600"))
MEMORY_SOFT_FRACTION = 0.60   # above this: gc + slow down
MEMORY_HARD_FRACTION = 0.78   # above this: wait for memory, then stop cleanly
EXTRAS_DEFAULT = os.environ.get("HISTORICAL_EXPORT_EXTRAS", "1").strip().lower() not in ("0", "false", "no", "off")

# Only the intervals it makes sense to research on; all are native provider intervals.
INTERVAL_MINUTES = {"1m": 1, "5m": 5, "15m": 15}
SESSION_MINUTES = 375  # NSE cash session 09:15-15:30 IST
SESSION_CLOSE_SAFE = datetime.time(15, 45)  # a session is "complete" after this (IST)

# Market-context series. Index names are the INDstocks names already used by
# market_data.INDEX_ALIASES, so this resolves exactly like the rest of the app.
MARKET_SERIES = [
    ("NIFTY", "NIFTY 50", "market"),
    ("INDIA_VIX", "INDIA VIX", "market"),
]
SECTOR_SERIES = [
    ("NIFTYBANK", "NIFTY BANK"),
    ("NIFTYIT", "NIFTY IT"),
    ("NIFTYAUTO", "NIFTY AUTO"),
    ("NIFTYPHARMA", "NIFTY PHARMA"),
    ("NIFTYFMCG", "NIFTY FMCG"),
    ("NIFTYMETAL", "NIFTY METAL"),
    ("NIFTYENERGY", "NIFTY ENERGY"),
    ("NIFTYFINSERVICE", "NIFTY FIN SERVICE"),
    ("NIFTYREALTY", "NIFTY REALTY"),
    ("NIFTYMEDIA", "NIFTY MEDIA"),
    ("NIFTYPSUBANK", "NIFTY PSU BANK"),
    ("NIFTYINFRA", "NIFTY INFRA"),
]
# Other names the SAME index is known by. Matching is exact-after-normalisation, never substring,
# so none of these can pull in a different index (e.g. NIFTY 50 can never become NIFTY 500).
INDEX_NAME_ALTERNATES = {
    "NIFTY": ["NIFTY50", "NIFTY"],
    "INDIA_VIX": ["INDIAVIX", "NIFTY INDIA VIX"],
    "NIFTYBANK": ["BANKNIFTY", "NIFTY BANK INDEX"],
    "NIFTYFINSERVICE": ["NIFTY FINANCIAL SERVICES", "NIFTY FINANCIAL SERVICE", "NIFTY FINANCIAL", "FINNIFTY"],
    "NIFTYINFRA": ["NIFTY INFRASTRUCTURE"],
}
# sector_data's own sector name -> the file stem above, so the universe file can
# say which sector index file belongs to each stock.
SECTOR_NAME_TO_FILE = {
    "Bank": "NIFTYBANK", "IT": "NIFTYIT", "Auto": "NIFTYAUTO", "Pharma": "NIFTYPHARMA",
    "FMCG": "NIFTYFMCG", "Metal": "NIFTYMETAL", "Energy": "NIFTYENERGY",
    "Financial Services": "NIFTYFINSERVICE", "Realty": "NIFTYREALTY", "Media": "NIFTYMEDIA",
    "PSU Bank": "NIFTYPSUBANK", "Infrastructure": "NIFTYINFRA",
}

CANDLE_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume", "epoch"]
CANDLE_COLUMNS_COMPACT = ["epoch", "open", "high", "low", "close", "volume"]

DATABASE_URL = os.environ.get("DATABASE_URL")
PERSIST_DB = os.environ.get("HISTORICAL_EXPORT_PERSIST_DB", "1" if DATABASE_URL else "0").strip().lower() not in ("0", "false", "no", "off")
DB_ARCHIVE_KEEP = max(1, int(os.environ.get("HISTORICAL_EXPORT_DB_KEEP", "1")))
DB_ARCHIVE_MAX_MB = max(10, int(os.environ.get("HISTORICAL_EXPORT_DB_MAX_MB", "180")))
DB_CHUNK_BYTES = max(1024 * 1024, int(float(os.environ.get("HISTORICAL_EXPORT_DB_CHUNK_MB", "1")) * 1024 * 1024))
COMPACT_DEFAULT = os.environ.get("HISTORICAL_EXPORT_COMPACT", "1").strip().lower() not in ("0", "false", "no", "off")

_JOB_ID_RE = re.compile(r"[0-9a-f]{12}")


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def file_stem(symbol: str) -> str:
    """Filesystem-safe stem for a symbol ('M&M' -> 'M_AND_M', 'BAJAJ-AUTO' stays).
    The universe file lists both `symbol` and `file_stem`, so nothing is ambiguous."""
    s = str(symbol or "").strip().upper().replace("&", "_AND_")
    return re.sub(r"[^A-Z0-9._-]", "_", s)


def valid_job_id(job_id: str) -> bool:
    return bool(job_id and _JOB_ID_RE.fullmatch(str(job_id)))


def bars_per_day(interval: str) -> int:
    return SESSION_MINUTES // INTERVAL_MINUTES[interval]


def _workers_for(interval: str) -> int:
    if _memory_limit_mb() <= 1024:
        return 1
    if _WORKERS_ENV:
        return max(1, min(3, int(_WORKERS_ENV)))
    return 1 if interval == "1m" else 3


def _scrips_per_call(interval: str) -> int:
    return 1 if _memory_limit_mb() <= 1024 else (ONE_MINUTE_SCRIPS_PER_CALL if interval == "1m" else 5)


def _pace_for(interval: str) -> float:
    if _PACE_ENV is not None:
        try:
            return max(0.0, float(_PACE_ENV))
        except ValueError:
            pass
    return {"1m": 0.35, "5m": 0.1, "15m": 0.05}.get(interval, 0.1)


def _ist_now() -> datetime.datetime:
    return datetime.datetime.now(IST)


def _ist_midnight(d: datetime.date) -> datetime.datetime:
    return datetime.datetime.combine(d, datetime.time.min, tzinfo=IST)


def latest_complete_session_date(now: Optional[datetime.datetime] = None) -> datetime.date:
    """Most recent calendar date whose session can be complete: today after
    15:45 IST, otherwise yesterday. (Weekends/holidays simply have no candles.)"""
    now = now or _ist_now()
    return now.date() if now.time() >= SESSION_CLOSE_SAFE else now.date() - datetime.timedelta(days=1)


def _fmt_num(v):
    """Write provider numbers as returned (no rounding / reformatting)."""
    return "" if v is None else v


_IST_OFFSET_SECONDS = 19800
_DAY_STR_CACHE: Dict[int, str] = {}


def _ts_iso(epoch_seconds) -> str:
    # Whole-second epochs (what the provider sends) take an arithmetic fast path that produces
    # exactly datetime.fromtimestamp(ts, IST).isoformat(); anything else uses the original path.
    if type(epoch_seconds) is int:
        day, sod = divmod(epoch_seconds + _IST_OFFSET_SECONDS, 86400)
        ds = _DAY_STR_CACHE.get(day)
        if ds is None:
            ds = (datetime.date(1970, 1, 1) + datetime.timedelta(days=day)).isoformat()
            if len(_DAY_STR_CACHE) < 4096:
                _DAY_STR_CACHE[day] = ds
        return f"{ds}T{sod // 3600:02d}:{sod // 60 % 60:02d}:{sod % 60:02d}+05:30"
    return datetime.datetime.fromtimestamp(epoch_seconds, tz=IST).isoformat()


def _ts_date(epoch_seconds) -> datetime.date:
    if type(epoch_seconds) is int:
        return datetime.date.fromordinal(719163 + (epoch_seconds + _IST_OFFSET_SECONDS) // 86400)
    return datetime.datetime.fromtimestamp(epoch_seconds, tz=IST).date()


def dedupe_sorted(candles: List[Dict[str, Any]]):
    """Sort by ts and drop duplicate timestamps (keep last). Returns (candles, n_duplicates)."""
    by_ts: Dict[Any, Dict[str, Any]] = {}
    total = 0
    for c in candles or []:
        ts = c.get("ts")
        if ts is None:
            continue
        total += 1
        by_ts[ts] = c
    ordered = [by_ts[k] for k in sorted(by_ts)]
    return ordered, total - len(ordered)


def candles_to_csv(candles: List[Dict[str, Any]], header: bool = True, compact: bool = False) -> str:
    """Serialize native candles. Compact mode removes the redundant ISO timestamp
    column and keeps the provider epoch as the authoritative timestamp.  The ISO
    timestamp is exactly recoverable from epoch and Asia/Kolkata timezone, so no
    market information is lost while the ZIP becomes materially smaller.
    """
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    if header:
        w.writerow(CANDLE_COLUMNS_COMPACT if compact else CANDLE_COLUMNS)
    for c in candles:
        ts = c["ts"]
        if compact:
            w.writerow([ts, _fmt_num(c.get("o")), _fmt_num(c.get("h")), _fmt_num(c.get("l")),
                        _fmt_num(c.get("c")), _fmt_num(c.get("v"))])
        else:
            w.writerow([_ts_iso(ts), _fmt_num(c.get("o")), _fmt_num(c.get("h")), _fmt_num(c.get("l")),
                        _fmt_num(c.get("c")), _fmt_num(c.get("v")), ts])
    return buf.getvalue()


def _rows_to_csv(columns: List[str], rows: List[Dict[str, Any]]) -> str:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=columns, extrasaction="ignore", lineterminator="\n")
    w.writeheader()
    for r in rows:
        w.writerow(r)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Memory governor
# ---------------------------------------------------------------------------

def _rss_mb() -> Optional[float]:
    try:
        with open("/proc/self/status", "r") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024.0
    except Exception:
        pass
    return None


def _memory_used_mb() -> Optional[float]:
    """Container working set, including other workers and unreclaimable file cache.

    RSS of one worker alone misses the Gunicorn parent, siblings and kernel memory.
    Subtract only inactive file cache, which Linux can reclaim under pressure.
    """
    rss = _rss_mb()
    for current, stat in (("/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory.stat"),
                          ("/sys/fs/cgroup/memory/memory.usage_in_bytes", "/sys/fs/cgroup/memory/memory.stat")):
        try:
            with open(current) as f:
                used = int(f.read())
            with open(stat) as f:
                stats = dict(line.split() for line in f if len(line.split()) == 2)
            inactive = int(stats.get("total_inactive_file", stats.get("inactive_file", 0)))
            return max(rss or 0.0, max(0, used - inactive) / 1048576.0)
        except (OSError, ValueError):
            continue
    return rss


def _release_heap() -> None:
    """Best-effort release of freed Python/glibc heap pages back to the OS.
    gc.collect() alone can leave arenas resident, which matters on small Render instances.
    """
    gc.collect()
    try:
        libc = ctypes.CDLL("libc.so.6")
        trim = getattr(libc, "malloc_trim", None)
        if trim is not None:
            trim(0)
    except Exception:
        pass


def _memory_limit_mb() -> float:
    if MEMORY_LIMIT_ENV:
        try:
            return float(MEMORY_LIMIT_ENV)
        except ValueError:
            pass
    for path in ("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"):
        try:
            with open(path, "r") as f:
                raw = f.read().strip()
            if raw.isdigit() and int(raw) < (1 << 50):
                return int(raw) / 1048576.0
        except Exception:
            continue
    return 512.0  # Render's smallest paid instance; be conservative when nothing can be detected


class _MemoryPressure(Exception):
    pass


# ---------------------------------------------------------------------------
# Request validation / estimate
# ---------------------------------------------------------------------------

def resolve_dates(dates=None, start_date=None, end_date=None,
                  now: Optional[datetime.datetime] = None) -> Dict[str, Any]:
    """Validate the requested dates. Returns
    {'ok': True, 'start': date, 'end': date, 'explicit': set|None, 'warnings': [...]}
    or {'ok': False, 'error': str}. Never includes an unfinished session."""
    now = now or _ist_now()
    latest = latest_complete_session_date(now)
    earliest = now.date() - datetime.timedelta(days=MAX_LOOKBACK_DAYS)
    warnings: List[str] = []
    explicit = None
    try:
        if dates:
            explicit = sorted({datetime.date.fromisoformat(str(d).strip()) for d in dates if str(d).strip()})
            if not explicit:
                return {"ok": False, "error": "No valid dates given."}
            start, end = explicit[0], explicit[-1]
        else:
            if not start_date or not end_date:
                return {"ok": False, "error": "Pick a start and end date (or list specific dates)."}
            start = datetime.date.fromisoformat(str(start_date))
            end = datetime.date.fromisoformat(str(end_date))
    except ValueError:
        return {"ok": False, "error": "Dates must be YYYY-MM-DD."}
    if start > end:
        return {"ok": False, "error": "Start date is after end date."}
    if end > latest:
        warnings.append(
            f"End date {end.isoformat()} is a session that isn't complete yet (or in the future); "
            f"the export stops at {latest.isoformat()} so it only contains full trading days.")
        end = latest
        if explicit:
            explicit = [d for d in explicit if d <= latest]
        if start > end:
            return {"ok": False, "error": "The selected dates are all in the future or in a session that hasn't closed yet."}
    if start < earliest:
        return {"ok": False, "error": (
            f"Start date {start.isoformat()} is more than {MAX_LOOKBACK_DAYS} days back "
            f"(earliest allowed: {earliest.isoformat()}). Raise HISTORICAL_EXPORT_MAX_LOOKBACK_DAYS if your "
            f"INDstocks plan serves deeper intraday history.")}
    if (end - start).days + 1 > MAX_SPAN_DAYS:
        return {"ok": False, "error": (
            f"Range is {(end - start).days + 1} calendar days; the limit per export is {MAX_SPAN_DAYS} "
            f"(~{int(MAX_SPAN_DAYS * 5 / 7)} trading days). Split it into two exports.")}
    return {"ok": True, "start": start, "end": end,
            "explicit": set(explicit) if explicit else None, "warnings": warnings}


def estimate(n_symbols: int, interval: str, start: datetime.date, end: datetime.date,
             trading_days: int, include_indices: bool, compact: bool = COMPACT_DEFAULT) -> Dict[str, Any]:
    n_series = (len(MARKET_SERIES) + len(SECTOR_SERIES)) if include_indices else 0
    bpd = bars_per_day(interval)
    rows = (n_symbols + n_series) * trading_days * bpd
    span_days = (end - start).days + 1
    windows = -(-span_days // 7)  # 5m/1m provider windows are 7 calendar days
    batch_size = _scrips_per_call(interval)
    calls = (-(-n_symbols // batch_size) + n_series) * windows
    # Rough wall-clock: every call is spaced by INDstocks' request gap, the writer pauses per window,
    # and >1 worker overlaps the network time. Deliberately a rounded, honest "this takes a while".
    gap = float(getattr(ind, "_MIN_GAP_SECONDS", 0.12))
    per_call = max(gap, (_pace_for(interval) + 0.25) / _workers_for(interval))
    return {
        "symbols": n_symbols, "index_series": n_series, "trading_days_est": trading_days,
        "bars_per_day": bpd, "rows_est": rows, "api_calls_est": calls,
        "zip_mb_est": round(rows * (16.5 if compact else 22) / 1e6, 1),
        "compact": bool(compact),
        "est_minutes": max(1, int(round(calls * per_call / 60.0))),
        "gentle_mode": {"workers": _workers_for(interval), "pause_seconds_per_window": _pace_for(interval)},
        "too_large": rows > MAX_ROWS, "max_rows": MAX_ROWS,
    }


# ---------------------------------------------------------------------------
# Job registry. In-memory state provides fast live progress, local meta supports
# same-process/runtime recovery, and PostgreSQL persistence keeps job metadata
# plus the newest completed archive across Render restarts/redeploys.
# ---------------------------------------------------------------------------

_jobs: Dict[str, Dict[str, Any]] = {}
_jobs_lock = threading.Lock()
_last_meta_persist: Dict[str, float] = {}
_db_ready = False


def _db_conn():
    if not (PERSIST_DB and DATABASE_URL and psycopg2):
        return None
    return psycopg2.connect(DATABASE_URL)


def _db_ensure() -> bool:
    global _db_ready
    if _db_ready:
        return True
    conn = _db_conn()
    if conn is None:
        return False
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS historical_export_jobs (
                        job_id VARCHAR(12) PRIMARY KEY,
                        meta_json JSONB NOT NULL,
                        archive_size BIGINT,
                        archive_persisted BOOLEAN NOT NULL DEFAULT FALSE,
                        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS historical_export_archive_chunks (
                        job_id VARCHAR(12) NOT NULL REFERENCES historical_export_jobs(job_id) ON DELETE CASCADE,
                        part_no INT NOT NULL,
                        payload BYTEA NOT NULL,
                        PRIMARY KEY (job_id, part_no)
                    )
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_hist_export_jobs_updated ON historical_export_jobs(updated_at DESC)")
        _db_ready = True
        return True
    except Exception as e:
        print(f"[historical_export] database persistence init failed: {e}")
        return False
    finally:
        conn.close()


def _db_save_meta(job: Dict[str, Any], archive_persisted: Optional[bool] = None) -> None:
    if not _db_ensure():
        return
    data = _public(job)
    conn = _db_conn()
    if conn is None:
        return
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO historical_export_jobs(job_id, meta_json, archive_size, archive_persisted, created_at, updated_at)
                    VALUES (%s, %s, %s, COALESCE(%s, FALSE), NOW(), NOW())
                    ON CONFLICT(job_id) DO UPDATE SET
                        meta_json=EXCLUDED.meta_json,
                        archive_size=EXCLUDED.archive_size,
                        archive_persisted=CASE WHEN %s IS NULL THEN historical_export_jobs.archive_persisted ELSE %s END,
                        updated_at=NOW()
                """, (job["job_id"], psycopg2.extras.Json(data), job.get("size_bytes"), archive_persisted,
                      archive_persisted, archive_persisted))
    except Exception as e:
        print(f"[historical_export] database meta save failed: {e}")
    finally:
        conn.close()


def _db_load_meta(job_id: str) -> Optional[Dict[str, Any]]:
    if not _db_ensure():
        return None
    conn = _db_conn()
    if conn is None:
        return None
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT meta_json, archive_persisted, archive_size FROM historical_export_jobs WHERE job_id=%s", (job_id,))
            row = cur.fetchone()
            if not row:
                return None
            data = dict(row.get("meta_json") or {})
            data["archive_persisted"] = bool(row.get("archive_persisted"))
            data["size_bytes"] = int(row.get("archive_size") or data.get("size_bytes") or 0) or None
            data["storage"] = "postgres" if data["archive_persisted"] else data.get("storage") or "metadata-only"
            return data
    except Exception as e:
        print(f"[historical_export] database meta load failed: {e}")
        return None
    finally:
        conn.close()


def _db_list_meta(limit: int = 10) -> List[Dict[str, Any]]:
    if not _db_ensure():
        return []
    conn = _db_conn()
    if conn is None:
        return []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT job_id, meta_json, archive_persisted, archive_size FROM historical_export_jobs ORDER BY updated_at DESC LIMIT %s", (max(1, int(limit)),))
            out = []
            for row in cur.fetchall():
                data = dict(row.get("meta_json") or {})
                data["archive_persisted"] = bool(row.get("archive_persisted"))
                data["size_bytes"] = int(row.get("archive_size") or data.get("size_bytes") or 0) or None
                data["storage"] = "postgres" if data["archive_persisted"] else data.get("storage") or "metadata-only"
                out.append(data)
            return out
    except Exception as e:
        print(f"[historical_export] database meta list failed: {e}")
        return []
    finally:
        conn.close()


_archive_io_lock = threading.Lock()


def _db_store_archive(job_id: str, path: str) -> bool:
    with _archive_io_lock:
        return _db_store_archive_unlocked(job_id, path)


def _db_store_archive_unlocked(job_id: str, path: str) -> bool:
    if not (_db_ensure() and os.path.exists(path)):
        return False
    size = os.path.getsize(path)
    if size > DB_ARCHIVE_MAX_MB * 1024 * 1024:
        print(f"[historical_export] archive {size/1e6:.1f} MB exceeds DB persistence cap {DB_ARCHIVE_MAX_MB} MB; metadata only")
        return False
    conn = _db_conn()
    if conn is None:
        return False
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM historical_export_archive_chunks WHERE job_id=%s", (job_id,))
                with open(path, "rb") as f:
                    part = 0
                    while True:
                        chunk = f.read(DB_CHUNK_BYTES)
                        if not chunk:
                            break
                        cur.execute("INSERT INTO historical_export_archive_chunks(job_id, part_no, payload) VALUES (%s,%s,%s)",
                                    (job_id, part, psycopg2.Binary(chunk)))
                        part += 1
                cur.execute("UPDATE historical_export_jobs SET archive_persisted=TRUE, archive_size=%s, updated_at=NOW() WHERE job_id=%s", (size, job_id))
                # Keep only the newest N persisted archives to protect DB storage. Metadata remains.
                cur.execute("""
                    SELECT job_id FROM historical_export_jobs
                    WHERE archive_persisted=TRUE ORDER BY updated_at DESC OFFSET %s
                """, (DB_ARCHIVE_KEEP,))
                old = [r[0] for r in cur.fetchall()]
                for old_id in old:
                    cur.execute("DELETE FROM historical_export_archive_chunks WHERE job_id=%s", (old_id,))
                    cur.execute("UPDATE historical_export_jobs SET archive_persisted=FALSE WHERE job_id=%s", (old_id,))
        return True
    except Exception as e:
        print(f"[historical_export] archive database persistence failed: {e}")
        return False
    finally:
        conn.close()


def _db_restore_archive(job_id: str, dest: str) -> bool:
    # Concurrent browser requests must not restore into the same partial file.
    with _archive_io_lock:
        if os.path.exists(dest):
            return True
        return _db_restore_archive_unlocked(job_id, dest)


def _db_restore_archive_unlocked(job_id: str, dest: str) -> bool:
    meta = _db_load_meta(job_id)
    if not meta or not meta.get("archive_persisted"):
        return False
    conn = _db_conn()
    if conn is None:
        return False
    tmp = dest + ".restore.partial"
    try:
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        with open(tmp, "wb") as f:
            with conn.cursor(name=f"hist_export_restore_{job_id}") as cur:
                cur.itersize = 1
                cur.execute("SELECT payload FROM historical_export_archive_chunks WHERE job_id=%s ORDER BY part_no", (job_id,))
                count = 0
                restored_size = 0
                for (payload,) in cur:
                    f.write(payload)
                    restored_size += len(payload)
                    count += 1
        expected_size = meta.get("size_bytes") or meta.get("archive_size")
        if count == 0 or (expected_size is not None and restored_size != int(expected_size)):
            try: os.remove(tmp)
            except Exception: pass
            return False
        os.replace(tmp, dest)
        return True
    except Exception as e:
        print(f"[historical_export] archive restore failed: {e}")
        try: os.remove(tmp)
        except Exception: pass
        return False
    finally:
        conn.close()


def _job_paths(job_id: str):
    base = os.path.join(EXPORT_DIR, job_id)
    return base + ".zip", base + ".zip.partial", base + ".meta.json"


def _public(job: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in job.items() if not k.startswith("_")}


def _save_meta(job: Dict[str, Any]) -> None:
    try:
        os.makedirs(EXPORT_DIR, exist_ok=True)
        with open(_job_paths(job["job_id"])[2], "w", encoding="utf-8") as f:
            json.dump(_public(job), f, default=str)
    except Exception as e:
        print(f"[historical_export] could not save local job meta: {e}")
    _db_save_meta(job)


def _cleanup_old() -> None:
    """Keep only the newest KEEP_EXPORTS COMPLETED exports (failed/cancelled ones never
    push a good ZIP out); drop other stale metas after a day and stale partials after 6h."""
    try:
        if not os.path.isdir(EXPORT_DIR):
            return
        with _jobs_lock:
            active = {jid for jid, j in _jobs.items() if j.get("status") in ("pending", "running")}
        now = time.time()
        completed, others = [], []
        for name in os.listdir(EXPORT_DIR):
            path = os.path.join(EXPORT_DIR, name)
            if name.endswith(".zip.partial"):
                if name[:-len(".zip.partial")] not in active and now - os.path.getmtime(path) > 6 * 3600:
                    try:
                        os.remove(path)
                    except Exception:
                        pass
            elif name.endswith(".spool") and os.path.isdir(path):
                if name[:-len(".spool")] not in active and now - os.path.getmtime(path) > 3600:
                    shutil.rmtree(path, ignore_errors=True)
            elif name.endswith(".meta.json"):
                jid = name[:-len(".meta.json")]
                if jid in active:
                    continue
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        status = json.load(f).get("status")
                except Exception:
                    status = None
                (completed if status == "completed" else others).append((os.path.getmtime(path), jid))
        completed.sort(reverse=True)
        doomed = [jid for _, jid in completed[KEEP_EXPORTS:]]
        doomed += [jid for mtime, jid in others if now - mtime > 24 * 3600]
        for jid in doomed:
            for p in _job_paths(jid):
                try:
                    os.remove(p)
                except Exception:
                    pass
        if doomed:
            with _jobs_lock:
                for jid in doomed:
                    _jobs.pop(jid, None)
    except Exception as e:
        print(f"[historical_export] cleanup failed: {e}")


def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    if not valid_job_id(job_id):
        return None
    with _jobs_lock:
        job = _jobs.get(job_id)
        if job:
            return _public(job)
    meta = _job_paths(job_id)[2]
    if os.path.exists(meta):
        try:
            with open(meta, "r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("status") in ("pending", "running"):
                data["status"] = "error"
                data["stage"] = "interrupted"
                data["error"] = "Server restarted while this export was running. Start it again; completed exports are persisted."
            return data
        except Exception:
            pass
    data = _db_load_meta(job_id)
    if data and data.get("status") in ("pending", "running"):
        data["status"] = "error"
        data["stage"] = "interrupted"
        data["error"] = "Server restarted while this export was running. Start it again; completed exports are persisted."
        data["message"] = data["error"]
        _db_save_meta(data)
    return data


def list_jobs(limit: int = 10) -> List[Dict[str, Any]]:
    seen: Dict[str, Dict[str, Any]] = {}
    with _jobs_lock:
        for jid, j in _jobs.items():
            seen[jid] = _public(j)
    if os.path.isdir(EXPORT_DIR):
        for name in os.listdir(EXPORT_DIR):
            if name.endswith(".meta.json"):
                jid = name[:-len(".meta.json")]
                if jid not in seen:
                    j = get_job(jid)
                    if j:
                        seen[jid] = j
    for j in _db_list_meta(limit=max(limit * 2, 10)):
        jid = j.get("job_id")
        if jid and jid not in seen:
            if j.get("status") in ("pending", "running"):
                j["status"] = "error"
                j["stage"] = "interrupted"
                j["error"] = "Server restarted while this export was running. Start it again."
            seen[jid] = j
    out = sorted(seen.values(), key=lambda j: j.get("created_at") or "", reverse=True)
    return out[:limit]


def zip_path_for_download(job_id: str) -> Optional[str]:
    job = get_job(job_id)
    if not job or job.get("status") != "completed":
        return None
    path = _job_paths(job_id)[0]
    if os.path.exists(path):
        return path
    if _db_restore_archive(job_id, path):
        return path
    return None


def any_running() -> Optional[str]:
    with _jobs_lock:
        for jid, j in _jobs.items():
            if j.get("status") in ("pending", "running"):
                return jid
    return None


def cancel_job(job_id: str) -> bool:
    with _jobs_lock:
        job = _jobs.get(job_id)
        if not job or job.get("status") not in ("pending", "running"):
            return False
        job["_cancel"] = True
        job["message"] = "Cancelling..."
        return True


def _disk_check(spec: Dict[str, Any]) -> Optional[str]:
    """Refuse to start an export the disk clearly cannot hold (a full disk breaks far more than the export)."""
    try:
        os.makedirs(EXPORT_DIR, exist_ok=True)
        free = shutil.disk_usage(EXPORT_DIR).free
        days = (spec["end"] - spec["start"]).days + 1
        trading_days = len(spec["explicit"]) if spec.get("explicit") else max(1, int(days * 5 / 7))
        n_series = (len(MARKET_SERIES) + len(SECTOR_SERIES)) if spec.get("include_indices", True) else 0
        rows = (len(spec["symbols"]) + n_series) * trading_days * bars_per_day(spec["interval"])
        need = int(rows * 22 * 1.5) + 50 * 1024 * 1024  # zip estimate + headroom for spool files
        if free < need:
            return (f"Not enough free disk in {EXPORT_DIR} for this export: about {need / 1e6:.0f} MB needed, "
                    f"{free / 1e6:.0f} MB free. Export fewer days/stocks, or point HISTORICAL_EXPORT_DIR at a bigger disk.")
    except Exception:
        return None
    return None


def start_job(spec: Dict[str, Any]) -> Dict[str, Any]:
    """spec keys: symbols (list of dicts: symbol, exchange, cap_class, company_name,
    industry, isin, app_sector), interval, start (date), end (date), explicit (set|None),
    include_indices (bool), universe_mode (str), universe_source (str),
    universe_notes (list[str]), warnings (list[str]).
    Optional: extras (bool, default HISTORICAL_EXPORT_EXTRAS), trading_day_fn (date -> bool),
    holidays (list[str]), cost_model (dict)."""
    if not ind.credentials_configured():
        return {"ok": False, "error": "INDstocks credentials are not configured (INDSTOCKS_API_KEY / INDSTOCKS_MPIN / INDSTOCKS_TOTP_SECRET)."}
    disk_error = _disk_check(spec)
    if disk_error:
        return {"ok": False, "error": disk_error}
    # Refuse to begin a 1m job when the web process already leaves too little RAM
    # for even one small provider response. This is preferable to taking the site down.
    if spec.get("interval") == "1m":
        _release_heap()
        rss0, lim0 = _memory_used_mb(), _memory_limit_mb()
        if rss0 is not None and lim0 - rss0 < max(64.0, MEMORY_RESERVE_MB * 0.65):
            return {"ok": False, "error": (
                f"Not enough free RAM to safely start a 1-minute export: server is using {rss0:.0f} MB of {lim0:.0f} MB. "
                f"At least ~{max(64.0, MEMORY_RESERVE_MB * 0.65):.0f} MB headroom is required. Restart when idle, export fewer dates, "
                f"or use a larger-memory Render instance.")}
    job_id = uuid.uuid4().hex[:12]
    extras = bool(spec.get("extras", EXTRAS_DEFAULT))
    compact = bool(spec.get("compact", COMPACT_DEFAULT))
    data_plan = [{"dataset": "EQUITY_OHLCV", "status": "REQUESTED", "source": "INDstocks → Upstox fallback", "note": "Native candle interval"}]
    if spec.get("include_indices", True):
        for stem, _name, _kind in MARKET_SERIES:
            data_plan.append({"dataset": stem, "status": "REQUESTED", "source": "INDstocks → Upstox → Dhan", "note": "Index/context series"})
        for stem, _name in SECTOR_SERIES:
            data_plan.append({"dataset": stem, "status": "REQUESTED", "source": "INDstocks → Upstox → Dhan", "note": "Sector index"})
    if extras:
        for ds, src in [("corporate_actions", "NSE → Upstox"), ("announcements", "NSE → Upstox where supported"),
                        ("financial_results", "NSE/Upstox"), ("bulk_block_deals", "NSE"),
                        ("FII_DII", "NSE → Upstox"), ("global_context_GIFT_NIFTY", "Stooq → Upstox")]:
            data_plan.append({"dataset": ds, "status": "REQUESTED", "source": src, "note": "Extra context dataset"})
    data_plan += [
        {"dataset": "historical_bid_ask_depth", "status": "NOT_SUPPORTED", "source": "current APIs", "note": "Collect live going forward or use licensed history"},
        {"dataset": "point_in_time_universe", "status": "NOT_SUPPORTED", "source": "current APIs", "note": "Snapshot only; needs reconstitution history"},
    ]
    job = {
        "job_id": job_id, "status": "pending", "stage": "queued", "message": "Queued...",
        "created_at": datetime.datetime.utcnow().isoformat() + "Z", "finished_at": None,
        "params": {
            "interval": spec["interval"], "start_date": spec["start"].isoformat(), "end_date": spec["end"].isoformat(),
            "specific_dates": sorted(d.isoformat() for d in spec["explicit"]) if spec.get("explicit") else None,
            "include_indices": bool(spec.get("include_indices", True)),
            "include_extras": extras,
            "universe_mode": spec.get("universe_mode"), "universe_source": spec.get("universe_source"),
            "symbols_requested": len(spec["symbols"]),
            "compact": compact,
            "gentle_mode": {"workers": _workers_for(spec["interval"]), "pause_seconds_per_window": _pace_for(spec["interval"]),
                            "scrips_per_call": _scrips_per_call(spec["interval"])},
        },
        "progress": {"chunks_done": 0, "chunks_total": 0, "symbols_with_data": 0, "symbols_without_data": 0,
                     "rows_written": 0, "files_written": 0},
        "warnings": list(spec.get("warnings") or []), "error": None, "file_name": None, "size_bytes": None,
        "elapsed_seconds": 0.0, "data_plan": data_plan, "storage": "building-local", "archive_persisted": False, "_cancel": False,
    }
    with _jobs_lock:
        running = next((jid for jid, j in _jobs.items() if j.get("status") in ("pending", "running")), None)
        if running:
            return {"ok": False, "error": f"Another historical export ({running}) is still running. Wait for it or cancel it first."}
        _jobs[job_id] = job
    _save_meta(job)
    _cleanup_old()
    spec = dict(spec)
    spec["extras"] = extras
    spec["compact"] = compact
    t = threading.Thread(target=_run_job, args=(job_id, spec), daemon=True, name=f"hist-export-{job_id}")
    t.start()
    return {"ok": True, "job_id": job_id, "job": _public(job)}


def _update(job_id: str, **kwargs) -> None:
    snapshot = None
    with _jobs_lock:
        job = _jobs.get(job_id)
        if job:
            for k, v in kwargs.items():
                if k == "progress":
                    job["progress"].update(v)
                else:
                    job[k] = v
            now = time.monotonic()
            force = any(k in kwargs for k in ("status", "finished_at", "file_name", "data_completeness"))
            if force or now - _last_meta_persist.get(job_id, 0.0) >= 5.0:
                _last_meta_persist[job_id] = now
                snapshot = dict(job)
    if snapshot:
        _save_meta(snapshot)


def _is_cancelled(job_id: str) -> bool:
    with _jobs_lock:
        job = _jobs.get(job_id)
        return bool(job and job.get("_cancel"))


# ---------------------------------------------------------------------------
# The export itself
# ---------------------------------------------------------------------------

class _Cancelled(Exception):
    pass


def _cleanup_partial(path: str) -> None:
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception:
        pass


def _day_wanted(d: datetime.date, start: datetime.date, end: datetime.date, explicit) -> bool:
    if explicit is not None:
        return d in explicit
    return start <= d <= end


def _failure(codes, ws, we, err) -> Dict[str, Any]:
    return {"scrips": list(codes), "start": ws.isoformat(), "end": we.isoformat(), "error": str(err)[:300]}


def _fetch_resilient(job_id: str, codes: List[str], interval: str, ws, we):
    """One window for <=5 scrips. If the batch call fails, retry each scrip on its own so ONE bad
    scrip (an invalid code, a delisted stock) cannot cost the other four their data.
    Returns ({scrip: candles}, [failure dicts])."""
    if _is_cancelled(job_id):
        return {c: [] for c in codes}, []
    data, err = ind.fetch_window(codes, interval, ws, we, retries=WINDOW_RETRIES)
    if data is not None:
        return data, []
    if len(codes) == 1:
        return {codes[0]: []}, [_failure(codes, ws, we, err)]
    merged: Dict[str, List[Dict[str, Any]]] = {}
    failures: List[Dict[str, Any]] = []
    for c in codes:
        if _is_cancelled(job_id):
            merged[c] = []
            continue
        d, e = ind.fetch_window([c], interval, ws, we, retries=WINDOW_RETRIES)
        if d is None:
            merged[c] = []
            failures.append(_failure([c], ws, we, e))
        else:
            merged.update(d)
    return merged, failures


def _index_candidates_for(stem: str, name: str) -> List[Dict[str, str]]:
    # INDstocks historical docs define scrip-codes as SEGMENT_TOKEN (e.g. NSE_3045).
    # Do not send undocumented IDX_/INDEX_/bare token shapes; those created avoidable 400s.
    all_candidates = ind.index_code_candidates([name] + INDEX_NAME_ALTERNATES.get(stem, []))
    return [c for c in all_candidates if c.get("variant") == "EXCH_ID"]


def _resolve_index_series(job_id: str, series_list, interval: str, fetch_start, fetch_end, before_fetch=None):
    """[(stem, index_name)] -> (resolved, unresolved, probe_rows).

    The old resolver only checked that the NAME existed in the provider's index list, then discovered
    at fetch time -- twelve times over -- that the provider would not serve that code. Now each
    candidate code is PROBED with a small real request before the big fetch:
      1. the provider's own index list (exact name after normalisation, plus known aliases);
      2. for each match, probe ONLY the documented '<EXCH>_<id>' code shape;
      3. the first code that returns candles at the requested interval wins;
      4. if no code serves intraday candles, the same probe is run at 1-day resolution and, if that
         works, the series is exported as DAILY (file name says so) instead of silently empty.
    Every probe (accepted or rejected, with the provider's error text) goes to reference/index_probe.csv."""
    resolved, unresolved, probe_rows = [], [], []
    taken: Dict[str, str] = {}
    probe_end = fetch_end
    intraday_start = max(fetch_start, probe_end - datetime.timedelta(days=7))
    daily_start = max(fetch_start, probe_end - datetime.timedelta(days=30))

    def probe(stem, cand, iv, ws, we):
        if before_fetch:
            before_fetch()
        data, err = ind.fetch_window([cand["code"]], iv, ws, we, retries=0)
        n = len(data.get(cand["code"], [])) if data is not None else 0
        probe_rows.append({"series": stem, "index_name": cand["name"], "code": cand["code"], "code_shape": cand["variant"],
                           "interval": iv, "candles_returned": n, "accepted": data is not None, "error": err or ""})
        return data is not None, n

    for stem, name in series_list:
        if _is_cancelled(job_id):
            raise _Cancelled()
        cands = _index_candidates_for(stem, name)
        if not cands:
            unresolved.append({"series": stem, "index_name": name,
                               "error": "no index with this name (or a known alias) in the provider's index list -- see reference/index_instruments.csv"})
            continue
        chosen, used_iv, empty_ok = None, None, None
        for cand in cands:
            ok, n = probe(stem, cand, interval, intraday_start, probe_end)
            if ok and n > 0:
                chosen, used_iv = cand, interval
                break
            if ok and empty_ok is None:
                empty_ok = cand
        if chosen is None and interval != "1d":
            for cand in cands:
                ok, n = probe(stem, cand, "1d", daily_start, probe_end)
                if ok and n > 0:
                    chosen, used_iv = cand, "1d"
                    break
        if chosen is None and empty_ok is not None:
            chosen, used_iv = empty_ok, interval  # accepted but no candles in the probe window: keep, coverage will say NO_DATA
        if chosen is None:
            errs = "; ".join(sorted({r["error"] for r in probe_rows if r["series"] == stem and r["error"]}))[:250]
            unresolved.append({"series": stem, "index_name": name,
                               "error": f"INDstocks rejected the documented scrip-code ({len(cands)} tried at {interval} and 1d); fallback providers will be attempted: {errs}"})
            continue
        if chosen["code"] in taken:
            unresolved.append({"series": stem, "index_name": name, "error": f"resolves to the same instrument as {taken[chosen['code']]}"})
            continue
        taken[chosen["code"]] = stem
        resolved.append({"stem": stem, "name": chosen["name"], "requested_name": name, "scrip": chosen["code"],
                         "code_shape": chosen["variant"], "interval": used_iv, "daily_fallback": used_iv != interval})
    return resolved, unresolved, probe_rows


PROBE_COLUMNS = ["series", "index_name", "code", "code_shape", "interval", "candles_returned", "accepted", "error"]
UNIVERSE_COLUMNS = ["symbol", "file_stem", "exchange", "cap_class", "company_name", "industry", "isin",
                    "app_sector", "sector_index_file", "data_status", "days_with_data", "rows", "duplicates_removed",
                    "resolved_via", "provider_symbol", "scrip_code", "note"]
MEMBERSHIP_COLUMNS = ["snapshot_date", "symbol", "isin", "universe", "cap_class", "effective_from", "effective_to",
                      "weight", "is_point_in_time", "source", "caveat"]
SECTOR_MEMBERSHIP_COLUMNS = ["snapshot_date", "symbol", "isin", "app_sector", "sector_index_file", "industry_nse",
                             "mapping_status", "effective_from", "effective_to", "is_point_in_time", "source", "caveat"]
CALENDAR_COLUMNS = ["date", "weekday", "exchange", "is_weekend", "is_exchange_holiday_per_app", "app_says_trading_day",
                    "export_has_candles", "calendar_conflict", "preopen_start_ist", "preopen_end_ist", "market_open_ist",
                    "market_close_ist", "special_session", "expected_bars", "calendar_source"]


def _universe_row(meta: Dict[str, Any], status: str, days: int, rows: int, duplicates_removed: int = 0,
                  resolved_via: str = "", provider_symbol: str = "", scrip_code: str = "", note: str = "") -> Dict[str, Any]:
    sector = meta.get("app_sector") or sector_data.get_sector(meta["symbol"]) or ""
    return {
        "symbol": meta["symbol"], "file_stem": file_stem(meta["symbol"]), "exchange": meta.get("exchange") or "NSE",
        "cap_class": meta.get("cap_class") or "UNKNOWN", "company_name": meta.get("company_name") or "",
        "industry": meta.get("industry") or "", "isin": meta.get("isin") or "",
        "app_sector": sector, "sector_index_file": SECTOR_NAME_TO_FILE.get(sector, ""),
        "data_status": status, "days_with_data": days, "rows": rows, "duplicates_removed": duplicates_removed,
        "resolved_via": resolved_via, "provider_symbol": provider_symbol, "scrip_code": scrip_code, "note": note,
    }


def _resolve_equity(meta: Dict[str, Any]):
    """-> (scrip, resolved_via, provider_symbol). Symbol first; ISIN only as a fallback for renamed tickers."""
    sym = meta["symbol"]
    try:
        return ind.resolve_equity_scrip_code_strict(sym), "SYMBOL", sym.strip().upper()
    except Exception as first_err:
        isin = (meta.get("isin") or "").strip()
        if isin:
            try:
                hit = ind.resolve_equity_scrip_by_isin(isin)
            except Exception:
                hit = None
            if hit:
                return hit[1], "ISIN", hit[0]
        raise first_err


def _build_calendar_rows(start, end, explicit, trading_day_fn, days_with_data, interval, source_note):
    rows = []
    cur = start
    bpd = bars_per_day(interval)
    while cur <= end:
        if explicit is None or cur in explicit:
            weekend = cur.weekday() >= 5
            try:
                app_trading = bool(trading_day_fn(cur)) if trading_day_fn else (not weekend)
            except Exception:
                app_trading = not weekend
            has = cur in days_with_data
            rows.append({
                "date": cur.isoformat(), "weekday": cur.strftime("%A"), "exchange": "NSE", "is_weekend": weekend,
                "is_exchange_holiday_per_app": (not weekend) and (not app_trading), "app_says_trading_day": app_trading,
                "export_has_candles": has, "calendar_conflict": app_trading != has,
                "preopen_start_ist": "09:00", "preopen_end_ist": "09:08", "market_open_ist": "09:15", "market_close_ist": "15:30",
                "special_session": "", "expected_bars": bpd if app_trading else 0, "calendar_source": source_note,
            })
        cur += datetime.timedelta(days=1)
    return rows


def _cost_model_doc(spec: Dict[str, Any], generated_at: str) -> Dict[str, Any]:
    doc: Dict[str, Any] = {
        "generated_at_utc": generated_at,
        "note": ("Charge rates exactly as configured in the app when this export was built (env-overridable). The app does not "
                 "track when each rate took effect, so effective_date is the export date, not the date the government/broker "
                 "changed the rate -- for older sessions, verify against the contract-note rates of that period. Apply per trade "
                 "on ACTUAL turnover (buy_value, sell_value), not as one blanket bps number."),
        "effective_date": generated_at[:10],
        "intraday_mis": spec.get("cost_model") or {},
    }
    try:
        import tax_calculator as tc
        doc["delivery"] = {
            "brokerage_pct": tc.DELIVERY_BROKERAGE_PCT, "brokerage_cap_inr": tc.DELIVERY_BROKERAGE_CAP_INR,
            "brokerage_min_inr": tc.DELIVERY_BROKERAGE_MIN_INR, "stt_pct_both_sides": tc.DELIVERY_STT_PCT,
            "stamp_duty_buy_pct": tc.DELIVERY_STAMP_DUTY_BUY_PCT, "exchange_txn_pct": tc.DELIVERY_EXCHANGE_TXN_PCT,
            "sebi_pct": tc.DELIVERY_SEBI_PCT, "gst_pct": tc.DELIVERY_GST_PCT,
            "dp_charge_per_isin_inr": tc.DELIVERY_DP_CHARGE_PER_ISIN_INR,
        }
    except Exception:
        pass
    return doc


def _spool_index_fallback(provider, names, interval, start, end, explicit,
                          path, compact, before_fetch):
    """Stage bounded windows on disk; a failed provider never leaves a partial ZIP entry."""
    configured = fallbacks.upstox_configured if provider == "Upstox" else fallbacks.dhan_configured
    if not configured():
        key = "UPSTOX_ACCESS_TOKEN" if provider == "Upstox" else "DHAN_ACCESS_TOKEN"
        raise fallbacks.FallbackError(f"CREDENTIAL_MISSING: {key} is not configured")
    stats = {"rows": 0, "days": set(), "first": None, "last": None, "dups": 0}
    with open(path, "w", encoding="utf-8", newline="") as dest:
        dest.write(",".join(CANDLE_COLUMNS) + "\n")
        for frm, to in fallbacks._date_windows(start, end, 7):
            if explicit is not None and not any(frm <= d <= to for d in explicit):
                continue
            before_fetch()
            if provider == "Upstox":
                candles, meta = fallbacks.fetch_upstox_index(names, interval, frm, to)
            else:
                candles, meta = fallbacks.fetch_dhan_index_by_names(
                    names, interval, _ist_midnight(frm), _ist_midnight(to + datetime.timedelta(days=1)))
            candles, dups = dedupe_sorted(candles)
            # Fence each window, including providers returning an inclusive end boundary.
            candles = [c for c in candles if _day_wanted(_ts_date(c["ts"]), frm, to, explicit)]
            stats.update(meta)
            stats["dups"] += dups
            if candles:
                dest.write(candles_to_csv(candles, header=False, compact=compact))
                stats["rows"] += len(candles)
                stats["days"].update(_ts_date(c["ts"]) for c in candles)
                if stats["first"] is None:
                    stats["first"] = candles[0]["ts"]
                stats["last"] = candles[-1]["ts"]
            del candles
    return stats


def _run_job(job_id: str, spec: Dict[str, Any]) -> None:
    t0 = time.monotonic()
    zip_final, zip_partial, _meta = _job_paths(job_id)
    spool_dir = os.path.join(EXPORT_DIR, job_id + ".spool")
    interval = spec["interval"]
    start, end, explicit = spec["start"], spec["end"], spec.get("explicit")
    compact = bool(spec.get("compact", COMPACT_DEFAULT))
    fetch_start = _ist_midnight(start)
    fetch_end = _ist_midnight(end + datetime.timedelta(days=1))
    bpd = bars_per_day(interval)
    workers = _workers_for(interval)
    pace = _pace_for(interval)
    generated_at = datetime.datetime.utcnow().isoformat() + "Z"
    snapshot_date = _ist_now().date().isoformat()

    quality_rows = None
    data_anomalies = None
    failures_all: List[Dict[str, Any]] = []
    unresolved_symbols: List[Dict[str, Any]] = []
    universe_rows: List[Dict[str, Any]] = []
    series_coverage: List[Dict[str, Any]] = []
    days_with_data = set()
    counters = {"rows": 0, "files": 0, "with": 0, "without": 0}
    peak_rss = [0.0]
    peak_working_set = [0.0]
    memory_events = [0]
    last_heap_release = [0.0]
    isin_resolved: List[Dict[str, str]] = []

    def refresh(stage, message, **prog):
        _update(job_id, stage=stage, message=message, elapsed_seconds=round(time.monotonic() - t0, 1),
                progress={"rows_written": counters["rows"], "files_written": counters["files"],
                          "symbols_with_data": counters["with"], "symbols_without_data": counters["without"], **prog})

    def govern(before_fetch: bool = False):
        """Keep a large RAM safety margin BEFORE the next provider JSON response.

        The old governor only checked after a window had already been fetched and parsed.
        Render can OOM-kill the process during that transient parse/allocation spike.
        This version checks before every fetch and keeps both a fractional and absolute reserve.
        """
        if _is_cancelled(job_id):
            raise _Cancelled()
        # Drop whole-application GC overhead between ordinary bounded requests.
        # Pressure handling below still collects immediately when needed.
        if time.monotonic() - last_heap_release[0] >= 5.0:
            _release_heap()
            last_heap_release[0] = time.monotonic()
        rss = _memory_used_mb()
        if rss is not None:
            peak_rss[0] = max(peak_rss[0], _rss_mb() or 0.0)
            peak_working_set[0] = max(peak_working_set[0], rss)
            limit = _memory_limit_mb()
            # On small instances, preserving an absolute reserve is more useful than
            # waiting until 90% of cgroup memory is already consumed.
            soft_mb = min(limit * MEMORY_SOFT_FRACTION, max(0.0, limit - MEMORY_RESERVE_MB))
            hard_mb = min(limit * MEMORY_HARD_FRACTION, max(0.0, limit - max(64.0, MEMORY_RESERVE_MB * 0.65)))
            # Never make soft >= hard because of a tiny configured container.
            if soft_mb >= hard_mb:
                soft_mb = max(0.0, hard_mb - 24.0)
            if rss >= soft_mb:
                memory_events[0] += 1
                time.sleep(0.5 if before_fetch else 0.25)
                _release_heap()
                rss = _memory_used_mb() or rss
            waited = 0.0
            while rss >= hard_mb:
                if _is_cancelled(job_id):
                    raise _Cancelled()
                if waited >= MEMORY_WAIT_SECONDS:
                    raise _MemoryPressure(
                        f"Server memory stayed at {rss:.0f} MB of {limit:.0f} MB with insufficient safety headroom for "
                        f"the next historical API response. Export stopped cleanly instead of letting Render OOM-kill the service. "
                        f"Use fewer dates/stocks or a larger-memory Render instance."
                    )
                _update(job_id, message=(f"Paused before next fetch: RAM {rss:.0f}/{limit:.0f} MB; "
                                         f"keeping {MEMORY_RESERVE_MB:.0f} MB response headroom..."),
                        elapsed_seconds=round(time.monotonic() - t0, 1))
                time.sleep(2.0)
                waited += 2.0
                _release_heap()
                rss = _memory_used_mb() or rss
        if not before_fetch:
            time.sleep(pace if pace > 0 else 0)

    try:
        os.makedirs(EXPORT_DIR, exist_ok=True)
        os.makedirs(spool_dir, exist_ok=True)
        quality_rows = DiskRows(os.path.join(spool_dir, "quality.sqlite"))
        data_anomalies = DiskRows(os.path.join(spool_dir, "anomalies.sqlite"))
        govern(before_fetch=True)
        _update(job_id, status="running")
        refresh("resolving", "Resolving instrument codes...")

        # ---- resolve equity scrip codes ---------------------------------
        sym_by_scrip: Dict[str, Dict[str, Any]] = {}
        via_by_scrip: Dict[str, tuple] = {}
        for meta in spec["symbols"]:
            sym = meta["symbol"]
            try:
                scrip, via, provider_symbol = _resolve_equity(meta)
                if scrip in sym_by_scrip:
                    # Same instrument already claimed by another universe entry. Never drop it silently: record it.
                    universe_rows.append(_universe_row(
                        meta, "DUPLICATE_INSTRUMENT", 0, 0, resolved_via=via, provider_symbol=provider_symbol, scrip_code=scrip,
                        note=f"resolves to the same provider instrument as {sym_by_scrip[scrip]['symbol']}; its candles are in that symbol's files"))
                    continue
                sym_by_scrip[scrip] = meta
                via_by_scrip[scrip] = (via, provider_symbol)
                if via == "ISIN":
                    isin_resolved.append({"symbol": sym, "provider_symbol": provider_symbol, "isin": meta.get("isin") or ""})
            except Exception as e:
                unresolved_symbols.append({"symbol": sym, "error": str(e)[:200]})
                universe_rows.append(_universe_row(meta, "UNRESOLVED", 0, 0, note=str(e)[:200]))
        equity_scrips = list(sym_by_scrip.keys())
        eq_batch_size = _scrips_per_call(interval)
        eq_chunks = [equity_scrips[i:i + eq_batch_size] for i in range(0, len(equity_scrips), eq_batch_size)]
        if not equity_scrips:
            raise RuntimeError("None of the selected symbols could be resolved in the INDstocks instrument master.")

        # ---- resolve + probe index series --------------------------------
        index_series: List[Dict[str, Any]] = []
        unresolved_series: List[Dict[str, Any]] = []
        probe_rows: List[Dict[str, Any]] = []
        fallback_attempts: List[Dict[str, Any]] = []
        if spec.get("include_indices", True):
            refresh("resolving", "Checking index instrument codes against the provider...")
            index_series, unresolved_series, probe_rows = _resolve_index_series(
                job_id, [(s, n) for (s, n, _g) in MARKET_SERIES] + list(SECTOR_SERIES), interval, fetch_start, fetch_end, lambda: govern(before_fetch=True))

        # ---- the task list: ONE provider call = ONE window for <=5 scrips ----
        eq_windows = ind.plan_windows(interval, fetch_start, fetch_end)
        if explicit is not None:  # only windows that contain a wanted day (results are identical, fewer calls)
            eq_windows = [(ws, we) for (ws, we) in eq_windows
                          if any(ws.date() <= d < we.date() for d in explicit)]
        tasks: List[Dict[str, Any]] = []
        for (ws, we) in eq_windows:
            for chunk in eq_chunks:
                tasks.append({"kind": "eq", "codes": chunk, "interval": interval, "ws": ws, "we": we})
        n_equity_tasks = len(tasks)
        ix_windows_cache: Dict[str, list] = {}
        for s in index_series:
            iv = s["interval"]
            if iv not in ix_windows_cache:
                wl = ind.plan_windows(iv, fetch_start, fetch_end)
                if explicit is not None:
                    wl = [(ws, we) for (ws, we) in wl if any(ws.date() <= d < we.date() for d in explicit)]
                ix_windows_cache[iv] = wl
            for idx, (ws, we) in enumerate(ix_windows_cache[iv]):
                tasks.append({"kind": "ix", "series": s, "codes": [s["scrip"]], "interval": iv, "ws": ws, "we": we, "idx": idx})
        total_tasks = len(tasks)

        # per-symbol running stats (tiny) -- candles themselves are never kept
        sym_stats: Dict[str, Dict[str, Any]] = {c: {"days": set(), "rows": 0, "dups": 0} for c in equity_scrips}

        duplicate_close_rows_dropped = 0
        # per-index-series stats + on-disk spool of per-window CSV blocks (chronological concat at the end)
        ix_stats: Dict[str, Dict[str, Any]] = {
            s["stem"]: {"rows": 0, "days": set(), "first": None, "last": None, "dups": 0, "parts": {}} for s in index_series}

        refresh("equities", f"Fetching {len(equity_scrips)} stocks ({interval}), one 7-day window at a time...",
                chunks_total=total_tasks, chunks_done=0)

        def consume_equity(task, data):
            nonlocal duplicate_close_rows_dropped
            for scrip in task["codes"]:
                meta = sym_by_scrip[scrip]
                st = sym_stats[scrip]
                candles, dups = dedupe_sorted(data.get(scrip, []))
                st["dups"] += dups
                by_day: Dict[datetime.date, List[Dict[str, Any]]] = {}
                for c in candles:
                    d = _ts_date(c["ts"])
                    if _day_wanted(d, start, end, explicit):
                        by_day.setdefault(d, []).append(c)
                stem = file_stem(meta["symbol"])
                for d in sorted(by_day):
                    if d in st["days"]:  # cannot happen (windows are day-aligned); never write a duplicate zip entry
                        failures_all.append({"scrips": [meta["symbol"]], "start": d.isoformat(), "end": d.isoformat(),
                                             "error": "day returned by two windows; second copy skipped"})
                        continue
                    day_candles = by_day[d]
                    # INDstocks can append a 15:30 stock row that is an exact duplicate of 15:29.
                    # The NSE cash tradable 1-minute grid used by this engine is 09:15..15:29 (375 bars).
                    if interval == "1m" and len(day_candles) >= 2:
                        a, b = day_candles[-2], day_candles[-1]
                        ta = datetime.datetime.fromtimestamp(a["ts"], tz=IST).time()
                        tb = datetime.datetime.fromtimestamp(b["ts"], tz=IST).time()
                        same = all(a.get(k) == b.get(k) for k in ("o", "h", "l", "c", "v"))
                        if ta == datetime.time(15, 29) and tb == datetime.time(15, 30) and same:
                            day_candles = day_candles[:-1]
                            duplicate_close_rows_dropped += 1
                    # Negative volume is invalid as a trading-volume feature. Keep the price bar, sanitize
                    # volume to zero, and record the exact anomaly for auditability.
                    for c in day_candles:
                        try:
                            if float(c.get("v", 0) or 0) < 0:
                                data_anomalies.append({"symbol": meta["symbol"], "date": d.isoformat(),
                                                       "timestamp": _ts_iso(c["ts"]), "type": "NEGATIVE_VOLUME",
                                                       "raw_value": c.get("v"), "action": "SET_TO_ZERO"})
                                c["v"] = 0
                        except (TypeError, ValueError):
                            pass
                    zf.writestr(f"historical_data/{d.isoformat()}/{stem}.csv", candles_to_csv(day_candles, compact=compact))
                    if not st["days"]:
                        counters["with"] += 1
                    st["days"].add(d)
                    st["rows"] += len(day_candles)
                    counters["rows"] += len(day_candles)
                    counters["files"] += 1
                    days_with_data.add(d)
                    quality_rows.append({
                        "symbol": meta["symbol"], "date": d.isoformat(), "bars": len(day_candles),
                        "expected_bars": bpd, "complete": len(day_candles) == bpd,
                        "first_timestamp": _ts_iso(day_candles[0]["ts"]),
                        "last_timestamp": _ts_iso(day_candles[-1]["ts"]),
                    })

        def consume_index(task, data):
            s = task["series"]
            stem = s["stem"]
            st = ix_stats[stem]
            candles, dups = dedupe_sorted(data.get(s["scrip"], []))
            st["dups"] += dups
            candles = [c for c in candles if _day_wanted(_ts_date(c["ts"]), start, end, explicit)]
            if not candles:
                return
            part = os.path.join(spool_dir, f"{stem}.{task['idx']:05d}.part")
            with open(part, "w", encoding="utf-8", newline="") as f:
                f.write(candles_to_csv(candles, header=False, compact=compact))
            st["parts"][task["idx"]] = part
            st["rows"] += len(candles)
            st["days"].update(_ts_date(c["ts"]) for c in candles)
            first, last = candles[0]["ts"], candles[-1]["ts"]
            st["first"] = first if st["first"] is None else min(st["first"], first)
            st["last"] = last if st["last"] is None else max(st["last"], last)

        def run_task(task):
            govern(before_fetch=True)
            data, fails = _fetch_resilient(job_id, task["codes"], task["interval"], task["ws"], task["we"])
            return task, data, fails

        tasks_done = 0
        with zipfile.ZipFile(zip_partial, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=ZIP_LEVEL, allowZip64=True) as zf:
            # ---- fetch + write, a bounded number of windows in flight ----
            pending = set()
            it = iter(tasks)
            exhausted = False
            with ThreadPoolExecutor(max_workers=workers) as pool:
                try:
                    while True:
                        max_pending = workers
                        while not exhausted and len(pending) < max_pending:
                            try:
                                pending.add(pool.submit(run_task, next(it)))
                            except StopIteration:
                                exhausted = True
                        if not pending:
                            break
                        done, _ = wait(pending, timeout=1.0, return_when=FIRST_COMPLETED)
                        if _is_cancelled(job_id):
                            raise _Cancelled()
                        while done:
                            fut = done.pop()
                            pending.discard(fut)
                            try:
                                task, data, fails = fut.result()
                            except (_MemoryPressure, _Cancelled):
                                raise
                            except Exception as e:  # a worker blowing up must not lose the whole export
                                failures_all.append({"scrips": ["?"], "start": "", "end": "", "error": f"window task crashed: {e}"[:300]})
                                tasks_done += 1
                                continue
                            for fl in fails:
                                if task["kind"] == "eq":
                                    fl["scrips"] = [sym_by_scrip[c]["symbol"] if c in sym_by_scrip else c for c in fl["scrips"]]
                                else:
                                    fl["scrips"] = [task["series"]["stem"]]
                                failures_all.append(fl)
                            if task["kind"] == "eq":
                                consume_equity(task, data)
                            else:
                                consume_index(task, data)
                            del data
                            del fut
                            tasks_done += 1
                            refresh("equities" if tasks_done <= n_equity_tasks else "indices",
                                    f"Fetched {tasks_done}/{total_tasks} windows...", chunks_total=total_tasks, chunks_done=tasks_done)
                            govern()
                except BaseException:
                    for f in pending:
                        f.cancel()
                    raise

            # ---- index files: concatenate spooled blocks chronologically --------
            for s in index_series:
                stem = s["stem"]
                st = ix_stats[stem]
                daily = s["interval"] == "1d" and interval != "1d"
                folder = "market" if stem in ("NIFTY", "INDIA_VIX") else "sectors"
                out_name = f"{folder}/{stem}{'_DAILY' if daily else ''}.csv"
                if st["parts"]:
                    with zf.open(out_name, "w", force_zip64=True) as dest:
                        dest.write((",".join(CANDLE_COLUMNS) + "\n").encode("utf-8"))
                        for idx in sorted(st["parts"], reverse=True):  # windows are indexed newest-first
                            with open(st["parts"][idx], "rb") as src:
                                shutil.copyfileobj(src, dest, 1024 * 1024)
                            os.remove(st["parts"][idx])
                    counters["files"] += 1
                counters["rows"] += st["rows"]
                series_coverage.append({
                    "series": stem, "index_name": s["name"], "requested_name": s["requested_name"], "folder": folder,
                    "file": out_name if st["parts"] else None, "interval_written": s["interval"],
                    "daily_fallback": daily, "provider_code": s["scrip"], "code_shape": s["code_shape"],
                    "rows": st["rows"], "days": len(st["days"]),
                    "first_timestamp": _ts_iso(st["first"]) if st["first"] is not None else None,
                    "last_timestamp": _ts_iso(st["last"]) if st["last"] is not None else None,
                    "duplicates_removed": st["dups"],
                    "status": ("DAILY_FALLBACK" if daily else "OK") if st["rows"] else "NO_DATA",
                })
            # ---- fallback providers for rejected/missing indices ----------------
            # Primary is always INDstocks. Only unresolved series reach this block.
            # Upstox uses its documented NSE_INDEX instrument_key and V3 historical endpoint.
            # Dhan is an optional third route; its public instrument master supplies the official securityId.
            if unresolved_series:
                refresh("indices", "Trying documented fallback providers for rejected indices...")
                still_unresolved = []
                for u in unresolved_series:
                    stem, req_name = u["series"], u["index_name"]
                    names = [req_name] + INDEX_NAME_ALTERNATES.get(stem, [])
                    folder = "market" if stem in ("NIFTY", "INDIA_VIX") else "sectors"
                    out_name = f"{folder}/{stem}.csv"
                    result = None
                    for provider in ("Upstox", "Dhan"):
                        part = os.path.join(spool_dir, stem + ".fallback.csv")
                        try:
                            result = _spool_index_fallback(
                                provider, names, interval, start, end, explicit, part, compact,
                                lambda: govern(before_fetch=True))
                            fallback_attempts.append({"dataset": stem, "provider": provider,
                                "status": "SUCCESS" if result["rows"] else "EMPTY",
                                "identifier": result.get("instrument_key", ""), "rows": result["rows"], "error": ""})
                        except (_Cancelled, _MemoryPressure):
                            raise
                        except Exception as e:
                            result = None
                            err = str(e)[:300]
                            fallback_attempts.append({"dataset": stem, "provider": provider,
                                "status": "CREDENTIAL_MISSING" if err.startswith("CREDENTIAL_MISSING") else "FAILED",
                                "identifier": "", "rows": 0, "error": err})
                        if result and result["rows"]:
                            zf.write(part, out_name)
                            counters["rows"] += result["rows"]
                            counters["files"] += 1
                            series_coverage.append({
                                "series": stem, "index_name": result.get("name", req_name), "requested_name": req_name,
                                "folder": folder, "file": out_name, "interval_written": interval, "daily_fallback": False,
                                "provider_code": result.get("instrument_key", ""), "code_shape": "FALLBACK_DOCUMENTED",
                                "provider": provider, "rows": result["rows"], "days": len(result["days"]),
                                "first_timestamp": _ts_iso(result["first"]), "last_timestamp": _ts_iso(result["last"]),
                                "duplicates_removed": result["dups"], "status": "FALLBACK_OK"})
                            os.remove(part)
                            break
                    if not result or not result["rows"]:
                        still_unresolved.append(u)
                    _release_heap()
                unresolved_series = still_unresolved

            # ---- equity gap fallback -----------------------------------------------------
            # INDstocks remains primary. If a resolved equity came back with no data or is missing
            # one or more dates that other universe symbols prove were trading dates, Upstox is
            # allowed to fill ONLY those absent day files using the documented NSE_EQ|ISIN key.
            # Existing INDstocks files are never overwritten or blended within a day.
            expected_stock_days = set(days_with_data)
            if expected_stock_days and fallbacks.upstox_configured():
                refresh("equities", "Trying Upstox fallback for missing stock-days...")
                for scrip in equity_scrips:
                    meta, st = sym_by_scrip[scrip], sym_stats[scrip]
                    missing_days = sorted(expected_stock_days - set(st["days"]))
                    isin = (meta.get("isin") or "").strip()
                    if not missing_days or not isin:
                        continue
                    try:
                        written = 0
                        failed_days = 0
                        fb_meta = {"instrument_key": f"NSE_EQ|{isin}"}
                        for missing_day in missing_days:
                            govern(before_fetch=True)
                            try:
                                fb_candles, fb_meta = fallbacks.fetch_upstox_equity(isin, interval, missing_day, missing_day)
                            except (_Cancelled, _MemoryPressure):
                                raise
                            except Exception as e:
                                failed_days += 1
                                fallback_attempts.append({"dataset": f"EQUITY:{meta['symbol']}", "provider": "Upstox",
                                    "status": "FAILED", "identifier": f"NSE_EQ|{isin}", "rows": 0,
                                    "error": f"{missing_day.isoformat()}: {e}"[:300]})
                                continue
                            fb_candles, fb_dups = dedupe_sorted(fb_candles)
                            by_day = {}
                            for c in fb_candles:
                                d = _ts_date(c["ts"])
                                if d in missing_days:
                                    by_day.setdefault(d, []).append(c)
                            for d in [missing_day]:
                                day_candles = by_day.get(d) or []
                                if not day_candles:
                                    continue
                                # Apply the same canonical stock-session hygiene as the primary path.
                                if interval == "1m" and len(day_candles) >= 2:
                                    a, b = day_candles[-2], day_candles[-1]
                                    ta = datetime.datetime.fromtimestamp(a["ts"], tz=IST).time()
                                    tb = datetime.datetime.fromtimestamp(b["ts"], tz=IST).time()
                                    if ta == datetime.time(15, 29) and tb == datetime.time(15, 30) and all(a.get(k) == b.get(k) for k in ("o","h","l","c","v")):
                                        day_candles = day_candles[:-1]
                                for c in day_candles:
                                    try:
                                        if float(c.get("v", 0) or 0) < 0:
                                            data_anomalies.append({"symbol": meta["symbol"], "date": d.isoformat(),
                                                                   "timestamp": _ts_iso(c["ts"]), "type": "NEGATIVE_VOLUME",
                                                                   "raw_value": c.get("v"), "action": "SET_TO_ZERO_UPSTOX_FALLBACK"})
                                            c["v"] = 0
                                    except (TypeError, ValueError):
                                        pass
                                zf.writestr(f"historical_data/{d.isoformat()}/{file_stem(meta['symbol'])}.csv", candles_to_csv(day_candles, compact=compact))
                                if not st["days"]:
                                    counters["with"] += 1
                                st["days"].add(d); st["rows"] += len(day_candles); st["dups"] += fb_dups
                                counters["rows"] += len(day_candles); counters["files"] += 1; written += len(day_candles)
                                quality_rows.append({"symbol": meta["symbol"], "date": d.isoformat(), "bars": len(day_candles),
                                                     "expected_bars": bpd, "complete": len(day_candles) == bpd,
                                                     "first_timestamp": _ts_iso(day_candles[0]["ts"]),
                                                     "last_timestamp": _ts_iso(day_candles[-1]["ts"])})
                            del fb_candles, by_day, day_candles
                        fallback_attempts.append({"dataset": f"EQUITY:{meta['symbol']}", "provider": "Upstox",
                                                  "status": "PARTIAL" if failed_days and written else ("SUCCESS" if written else ("FAILED" if failed_days else "EMPTY")),
                                                  "identifier": fb_meta.get("instrument_key", ""), "rows": written,
                                                  "error": f"{failed_days} date request(s) failed" if failed_days else ("" if written else f"No candles for {len(missing_days)} missing date(s)")})
                    except (_Cancelled, _MemoryPressure):
                        raise
                    except Exception as e:
                        err = str(e)[:300]
                        fallback_attempts.append({"dataset": f"EQUITY:{meta['symbol']}", "provider": "Upstox",
                                                  "status": "CREDENTIAL_MISSING" if err.startswith("CREDENTIAL_MISSING") else "FAILED",
                                                  "identifier": f"NSE_EQ|{isin}", "rows": 0, "error": err})
                    _release_heap()
            elif expected_stock_days and not fallbacks.upstox_configured():
                missing_count = sum(1 for scrip in equity_scrips if expected_stock_days - set(sym_stats[scrip]["days"]))
                if missing_count:
                    fallback_attempts.append({"dataset": "EQUITY_MISSING_DAYS", "provider": "Upstox",
                                              "status": "CREDENTIAL_MISSING", "identifier": "NSE_EQ|ISIN", "rows": 0,
                                              "error": f"{missing_count} resolved symbol(s) have missing dates; configure UPSTOX_ACCESS_TOKEN to backfill them"})

            # Persist the complete provider chain so a missing file is never ambiguous.
            if fallback_attempts:
                zf.writestr("reference/fallback_provider_attempts.csv", _rows_to_csv(
                    ["dataset", "provider", "status", "identifier", "rows", "error"], fallback_attempts))
            zf.writestr("reference/provider_capabilities.csv", _rows_to_csv(
                ["provider", "configured", "purpose"], fallbacks.provider_capabilities()))

            # Report spool files remain open until packaging is complete.

            # ---- universe status per stock ------------------------------------
            for scrip in equity_scrips:
                meta, st = sym_by_scrip[scrip], sym_stats[scrip]
                via, provider_symbol = via_by_scrip[scrip]
                if st["days"]:
                    universe_rows.append(_universe_row(meta, "OK", len(st["days"]), st["rows"], st["dups"], via, provider_symbol, scrip))
                else:
                    counters["without"] += 1
                    universe_rows.append(_universe_row(
                        meta, "NO_DATA", 0, 0, st["dups"], via, provider_symbol, scrip,
                        note="resolved to a provider instrument but no candles came back (see quality/fetch_failures.csv for failed windows)"))

            # ---- extra datasets (network, best-effort, each isolated) ----------
            dataset_status: List[Dict[str, Any]] = []
            if spec.get("extras", EXTRAS_DEFAULT):
                govern(before_fetch=True)
                refresh("extras", "Fetching events, corporate actions and global series...", chunks_total=total_tasks, chunks_done=tasks_done)
                ectx = historical_extras.ExtrasContext(
                    start, end, explicit, [m["symbol"] for m in spec["symbols"]],
                    [m.get("isin") or "" for m in spec["symbols"]], lambda: _is_cancelled(job_id),
                    lambda msg: _update(job_id, message=msg, elapsed_seconds=round(time.monotonic() - t0, 1)))
                def extras_memory_check():
                    used = _memory_used_mb()
                    if used is not None and used >= min(_memory_limit_mb() * MEMORY_HARD_FRACTION, _memory_limit_mb() - MEMORY_RESERVE_MB):
                        ectx.deadline = 0
                        raise historical_extras.DatasetError("Extras stopped for memory headroom; candle data is preserved")
                ectx.memory_check = extras_memory_check
                try:
                    dataset_status = historical_extras.run_extras(zf, ectx)
                except historical_extras.ExtrasCancelled:
                    raise _Cancelled()
                except Exception as e:  # extras must never fail the candle export they ride along with
                    dataset_status = [{"dataset": "extras_runner", "file": "", "status": "FAILED", "rows": 0, "resolution": "",
                                       "point_in_time": "", "source": "", "note": f"{type(e).__name__}: {e}"[:300]}]
            else:
                dataset_status = [{"dataset": "extras", "file": "", "status": "SKIPPED_BY_REQUEST", "rows": 0, "resolution": "",
                                   "point_in_time": "", "source": "", "note": "extra datasets were switched off for this export"}]

            # ---- reference / universe / quality / manifest / README ------------
            refresh("packaging", "Writing reference files, quality report and manifest...", chunks_total=total_tasks, chunks_done=tasks_done)
            universe_rows.sort(key=lambda r: r["symbol"])
            zf.writestr("universe/large_midcap.csv", _rows_to_csv(UNIVERSE_COLUMNS, universe_rows))

            uni_label = str(spec.get("universe_mode") or "")
            caveat_u = ("SNAPSHOT of today's NSE index constituents, NOT historical membership. Do not apply to dates before snapshot_date "
                        "without accepting survivorship bias. Keep this file from every export to build real history.")
            zf.writestr("universe/universe_membership.csv", _rows_to_csv(MEMBERSHIP_COLUMNS, [{
                "snapshot_date": snapshot_date, "symbol": m["symbol"], "isin": m.get("isin") or "", "universe": uni_label,
                "cap_class": m.get("cap_class") or "UNKNOWN", "effective_from": snapshot_date, "effective_to": "", "weight": "",
                "is_point_in_time": False, "source": spec.get("universe_source") or "", "caveat": caveat_u,
            } for m in sorted(spec["symbols"], key=lambda x: x["symbol"])]))

            sec_rows = []
            for m in sorted(spec["symbols"], key=lambda x: x["symbol"]):
                sector = m.get("app_sector") or sector_data.get_sector(m["symbol"]) or ""
                sec_rows.append({
                    "snapshot_date": snapshot_date, "symbol": m["symbol"], "isin": m.get("isin") or "", "app_sector": sector,
                    "sector_index_file": SECTOR_NAME_TO_FILE.get(sector, ""), "industry_nse": m.get("industry") or "",
                    "mapping_status": "MAPPED" if sector else "UNMAPPED_IN_APP (sector_data.py has no entry; industry_nse is NSE's own label)",
                    "effective_from": snapshot_date, "effective_to": "", "is_point_in_time": False,
                    "source": "sector_data.SECTOR_MAP (hand-maintained) + NSE constituent CSV industry",
                    "caveat": "Today's classification, not historical. An unmapped stock is left unmapped on purpose -- a wrong sector is worse than none.",
                })
            zf.writestr("universe/sector_membership.csv", _rows_to_csv(SECTOR_MEMBERSHIP_COLUMNS, sec_rows))

            try:
                provider_rows = ind.get_nse_instrument_rows([v[1] for v in via_by_scrip.values()])
                cols: List[str] = []
                for r in provider_rows.values():
                    for k in r.keys():
                        if k not in cols:
                            cols.append(k)
                im_rows = []
                for scrip, meta in sym_by_scrip.items():
                    via, psym = via_by_scrip[scrip]
                    row = {"symbol": meta["symbol"], "file_stem": file_stem(meta["symbol"]), "scrip_code": scrip, "resolved_via": via}
                    row.update(provider_rows.get(psym, {}))
                    im_rows.append(row)
                zf.writestr("reference/instrument_master.csv",
                            _rows_to_csv(["symbol", "file_stem", "scrip_code", "resolved_via"] + cols, sorted(im_rows, key=lambda r: r["symbol"])))
                dataset_status.append({"dataset": "instrument_master", "file": "reference/instrument_master.csv", "status": "OK", "rows": len(im_rows),
                                       "resolution": "snapshot", "point_in_time": "no: provider's current master",
                                       "source": "INDstocks /market/instruments", "note": "Every column the provider sends; tick size / lot / price band / listing dates only appear if the provider includes them."})
            except Exception as e:
                dataset_status.append({"dataset": "instrument_master", "file": "", "status": "FAILED", "rows": 0, "resolution": "", "point_in_time": "",
                                       "source": "INDstocks /market/instruments", "note": str(e)[:300]})
            try:
                ix_rows = ind.get_index_instrument_rows()
                zf.writestr("reference/index_instruments.csv", _rows_to_csv(["exch", "index_name", "security_id"], ix_rows))
            except Exception as e:
                print(f"[historical_export] index instrument list not written: {e}")
            if probe_rows:
                zf.writestr("reference/index_probe.csv", _rows_to_csv(PROBE_COLUMNS, probe_rows))

            calendar_note = ("app hard-coded NSE holiday list (2026 only) + MARKET_HOLIDAYS_IN env; weekends excluded. "
                             "Dates outside 2026 may be wrong -- check calendar_conflict rows against export_has_candles.")
            cal_rows = _build_calendar_rows(start, end, explicit, spec.get("trading_day_fn"), days_with_data, interval, calendar_note)
            zf.writestr("reference/trading_calendar.csv", _rows_to_csv(CALENDAR_COLUMNS, cal_rows))
            zf.writestr("reference/cost_model.json", json.dumps(_cost_model_doc(spec, generated_at), indent=2, default=str))
            for name, path, note in (("trading_calendar", "reference/trading_calendar.csv", calendar_note),
                                     ("cost_model", "reference/cost_model.json", "Rates as configured in the app; see file for caveats."),
                                     ("universe_membership", "universe/universe_membership.csv", "SNAPSHOT, not point-in-time"),
                                     ("sector_membership", "universe/sector_membership.csv", "SNAPSHOT, not point-in-time")):
                dataset_status.append({"dataset": name, "file": path, "status": "OK", "rows": "", "resolution": "snapshot/static",
                                       "point_in_time": "no" if "membership" in name else "n/a", "source": "app", "note": note})

            write_csv_entry(zf, "quality/data_quality.csv",
                            ["symbol", "date", "bars", "expected_bars", "complete", "first_timestamp", "last_timestamp"], quality_rows)
            if data_anomalies:
                write_csv_entry(zf, "quality/data_anomalies.csv",
                                ["symbol", "date", "timestamp", "type", "raw_value", "action"], data_anomalies)
            # Date-level completeness makes empty-but-HTTP-success responses visible.
            date_counts: Dict[str, int] = {}
            for st in sym_stats.values():
                for dd in st["days"]:
                    date_counts[dd.isoformat()] = date_counts.get(dd.isoformat(), 0) + 1
            expected_symbols = len(spec["symbols"])
            completeness_rows = []
            for dd in sorted(date_counts):
                n = date_counts[dd]; ratio = (n / expected_symbols) if expected_symbols else 0
                completeness_rows.append({"date": dd, "symbols_with_data": n, "expected_symbols": expected_symbols,
                                          "coverage_ratio": round(ratio, 6),
                                          "research_eligible": ratio >= float(os.environ.get("HISTORICAL_MIN_DATE_COVERAGE", "0.98")),
                                          "status": "COMPLETE" if n == expected_symbols else "PARTIAL"})
            zf.writestr("quality/date_completeness.csv", _rows_to_csv(
                ["date", "symbols_with_data", "expected_symbols", "coverage_ratio", "research_eligible", "status"], completeness_rows))

            zf.writestr("quality/fetch_failures.csv",
                        _rows_to_csv(["scrips", "start", "end", "error"],
                                     [{**f, "scrips": ";".join(f["scrips"])} for f in failures_all]))
            zf.writestr("quality/dataset_status.csv", _rows_to_csv(historical_extras.STATUS_COLUMNS, dataset_status))

            days_sorted = sorted(days_with_data)
            incomplete = quality_rows.incomplete
            manifest = {
                "generated_at_utc": generated_at,
                "app": "AI Market Scanner / MarketPredictor",
                "exporter_version": "v3.2.5-streamed-memory-safe-export",
                "data_source": ("INDstocks primary historical candles; documented Upstox/Dhan fallbacks for rejected/missing "
                                "series; NSE/Stooq legacy extra routes with Upstox fallbacks. See reference/fallback_provider_attempts.csv "
                                "and quality/dataset_status.csv."),
                "interval": interval,
                "expected_bars_per_full_session": bpd,
                "timezone": "Asia/Kolkata (timestamps carry an explicit +05:30 offset; `epoch` is the provider's original unix seconds)",
                "requested": {
                    "start_date": start.isoformat(), "end_date": end.isoformat(),
                    "specific_dates": sorted(d.isoformat() for d in explicit) if explicit else None,
                },
                "universe": {
                    "mode": spec.get("universe_mode"), "source": spec.get("universe_source"),
                    "notes": spec.get("universe_notes") or [],
                    "symbols_requested": len(spec["symbols"]),
                    "symbols_with_data": counters["with"],
                    "symbols_without_data": counters["without"],
                    "symbols_unresolved": unresolved_symbols,
                    "symbols_resolved_via_isin_fallback": isin_resolved,
                    "is_point_in_time": False,
                },
                "coverage": {
                    "trading_days_with_data": len(days_sorted),
                    "first_day": days_sorted[0].isoformat() if days_sorted else None,
                    "last_day": days_sorted[-1].isoformat() if days_sorted else None,
                    "days": [d.isoformat() for d in days_sorted],
                    "stock_day_files": len(quality_rows),
                    "stock_days_incomplete": incomplete,
                    "rows_total_incl_indices": counters["rows"],
                },
                "index_series": series_coverage,
                "index_series_unresolved": unresolved_series,
                "duplicate_1530_stock_rows_dropped": duplicate_close_rows_dropped,
                "negative_volume_rows_sanitized": len(data_anomalies),
                "datasets": dataset_status,
                "fetch_windows_failed_after_retries": len(failures_all),
                "fallback_attempts": fallback_attempts,
                "processing": {
                    "mode": "streaming: one provider window at a time, written straight into the ZIP",
                    "compact_candles": compact,
                    "workers": workers, "pause_seconds_per_window": pace, "zip_level": ZIP_LEVEL,
                    "equity_scrips_per_call": eq_batch_size, "memory_reserve_mb": MEMORY_RESERVE_MB,
                    "peak_process_rss_mb": round(peak_rss[0], 1) or None,
                    "peak_container_working_set_mb": round(peak_working_set[0], 1) or None, "memory_slowdown_events": memory_events[0],
                },
                "integrity": {
                    "aggregation": "none -- candles stay at the provider's native requested interval; no lower-timeframe resampling is performed",
                    "compact_format": ("epoch,open,high,low,close,volume; ISO timestamp omitted because it is exactly derivable from epoch" if compact else "timestamp,open,high,low,close,volume,epoch"),
                    "price_adjustment": "none applied; OHLC prices are not adjusted by this exporter",
                    "duplicate_timestamps": "de-duplicated keeping the last occurrence; exact duplicate stock 15:30 rows are dropped when they repeat 15:29",
                    "volume_hygiene": "negative provider volume is recorded in quality/data_anomalies.csv and sanitized to zero; OHLC is preserved",
                    "session_filter": "provider bars are retained except the proven exact-duplicate stock 15:30 row rule above",
                    "incomplete_sessions_excluded": "only sessions that have closed (>= 15:45 IST) are requested",
                },
                "warnings": list(spec.get("warnings") or []),
            }
            zf.writestr("manifest.json", json.dumps(manifest, indent=2, default=str))
            zf.writestr("README.txt", _readme(interval, bpd, compact))

        if _is_cancelled(job_id):
            raise _Cancelled()

        os.replace(zip_partial, zip_final)
        size = os.path.getsize(zip_final)
        stamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        file_name = f"marketpredictor_historical_{interval}_{start.isoformat()}_to_{end.isoformat()}_{stamp}.zip"

        warns = list(spec.get("warnings") or [])
        if failures_all:
            warns.append(f"{len(failures_all)} API window(s) still failed after {WINDOW_RETRIES} retries -- see quality/fetch_failures.csv. "
                         f"Days missing for the affected symbols are DATA GAPS, not non-trading days.")
        if counters["without"]:
            warns.append(f"{counters['without']} symbol(s) returned no candles in this range (see universe/large_midcap.csv `note`).")
        if unresolved_symbols:
            warns.append(f"{len(unresolved_symbols)} symbol(s) could not be resolved in the INDstocks instrument master.")
        if isin_resolved:
            warns.append(f"{len(isin_resolved)} symbol(s) were matched by ISIN because their ticker is not in the instrument master: "
                         + ", ".join(f"{r['symbol']}->{r['provider_symbol']}" for r in isin_resolved[:8]))
        if unresolved_series:
            warns.append("Index series not found at INDstocks: " + ", ".join(u["series"] for u in unresolved_series)
                         + " (see reference/index_probe.csv and manifest.json index_series_unresolved).")
        empty_series = [s["series"] for s in series_coverage if s["status"] == "NO_DATA"]
        if empty_series:
            warns.append("No candles returned for: " + ", ".join(empty_series))
        daily_series = [s["series"] for s in series_coverage if s["daily_fallback"] and s["rows"]]
        if daily_series:
            warns.append(f"INTRADAY index candles were not served for {len(daily_series)} series; they are written as DAILY (*_DAILY.csv): "
                         + ", ".join(daily_series[:6]) + (" ..." if len(daily_series) > 6 else ""))
        bad_ds = [d["dataset"] for d in dataset_status if d["status"] in ("FAILED", "PARTIAL")]
        if bad_ds:
            warns.append("Extra datasets that failed or are partial: " + ", ".join(bad_ds) + " -- see quality/dataset_status.csv.")
        if incomplete:
            warns.append(f"{incomplete} stock-day file(s) have fewer bars than a full {interval} session ({bpd}); "
                         f"see quality/data_quality.csv (early-close days and thinly traded stocks show up here).")
        if not days_sorted:
            warns.append("No trading days with data were found in the requested range.")

        # Compact, UI-safe completeness summary.  The ZIP remains the source of record,
        # but the export screen should tell the user immediately what actually arrived.
        ui_items = []
        for s in series_coverage:
            ui_items.append({
                "dataset": s.get("series"),
                "status": s.get("status") or ("OK" if s.get("rows") else "NO_DATA"),
                "rows": s.get("rows", 0),
                "source": s.get("provider") or "INDstocks",
                "note": ("Daily fallback" if s.get("daily_fallback") else "")
            })
        # Anything still unresolved has exhausted the configured documented fallbacks.
        attempt_by_dataset = {}
        for a in fallback_attempts:
            attempt_by_dataset.setdefault(a.get("dataset"), []).append(a)
        for u in unresolved_series:
            attempts = attempt_by_dataset.get(u.get("series"), [])
            reason = "; ".join(
                f"{a.get('provider')}: {a.get('status')}" + (f" ({a.get('error')})" if a.get('error') else "")
                for a in attempts[-3:]
            ) or (u.get("reason") or "No provider returned data")
            ui_items.append({"dataset": u.get("series"), "status": "MISSING", "rows": 0,
                             "source": "fallback chain exhausted", "note": reason[:500]})
        for d in dataset_status:
            ui_items.append({"dataset": d.get("dataset"), "status": d.get("status"),
                             "rows": d.get("rows", 0), "source": d.get("source", ""),
                             "note": d.get("note", "")})
        fallback_successes = sum(1 for a in fallback_attempts if a.get("status") == "SUCCESS")
        bad_statuses = {"FAILED", "PARTIAL", "NO_DATA", "MISSING", "NOT_AVAILABLE", "CREDENTIAL_MISSING", "REJECTED"}
        missing_count = sum(1 for x in ui_items if str(x.get("status") or "").upper() in bad_statuses)
        ok_count = sum(1 for x in ui_items if str(x.get("status") or "").upper() in {"OK", "FALLBACK_OK", "SUCCESS", "DAILY_FALLBACK"})
        data_completeness = {
            "stocks_with_data": counters["with"], "stocks_requested": len(spec["symbols"]),
            "stocks_without_data": counters["without"], "trading_days": len(days_sorted),
            "index_series_ok": sum(1 for s in series_coverage if s.get("rows")),
            "index_series_total": len(series_coverage) + len(unresolved_series),
            "fallback_successes": fallback_successes, "datasets_ok": ok_count,
            "datasets_missing_or_limited": missing_count, "items": ui_items,
        }

        _update(job_id, status="running", stage="persisting", finished_at=None,
                message=f"Saving completed archive ({size / 1e6:.1f} MB)...",
                file_name=file_name, size_bytes=size, warnings=warns,
                data_completeness=data_completeness,
                elapsed_seconds=round(time.monotonic() - t0, 1),
                progress={"rows_written": counters["rows"], "files_written": counters["files"],
                          "symbols_with_data": counters["with"], "symbols_without_data": counters["without"],
                          "chunks_done": tasks_done, "chunks_total": total_tasks},
                summary={"first_day": days_sorted[0].isoformat() if days_sorted else None,
                         "last_day": days_sorted[-1].isoformat() if days_sorted else None,
                         "trading_days": len(days_sorted), "failed_windows": len(failures_all),
                         "incomplete_stock_days": incomplete,
                         "index_series_ok": sum(1 for s in series_coverage if s["rows"]),
                         "index_series_total": len(series_coverage) + len(unresolved_series),
                         "peak_process_rss_mb": round(peak_rss[0], 1) or None})
        # Render's normal filesystem is ephemeral.  Persist the finished archive in PostgreSQL
        # (chunked, bounded, newest N only) so a later deploy/restart does not erase the download.
        persisted = _db_store_archive(job_id, zip_final) if PERSIST_DB else False
        storage = "postgres+local" if persisted else "local-ephemeral"
        if PERSIST_DB and not persisted:
            warns2 = list(warns)
            warns2.append(f"Finished ZIP was not persisted to PostgreSQL (cap {DB_ARCHIVE_MAX_MB} MB or persistence error); a Render redeploy can erase this local copy.")
        else:
            warns2 = warns
        _update(job_id, status="completed", stage="done", archive_persisted=persisted, storage=storage, warnings=warns2,
                finished_at=datetime.datetime.utcnow().isoformat() + "Z",
                elapsed_seconds=round(time.monotonic() - t0, 1),
                message=(f"Done: {counters['with']} stocks x {len(days_sorted)} trading days, "
                         f"{counters['rows']:,} candle rows, {size / 1e6:.1f} MB. "
                         + ("Saved persistently across deploys." if persisted else "Stored on local runtime disk.")))
    except _Cancelled:
        _cleanup_partial(zip_partial)
        _update(job_id, status="cancelled", stage="cancelled", message="Cancelled.",
                finished_at=datetime.datetime.utcnow().isoformat() + "Z",
                elapsed_seconds=round(time.monotonic() - t0, 1))
    except _MemoryPressure as e:
        _cleanup_partial(zip_partial)
        _update(job_id, status="error", stage="error", error=str(e)[:500], message=f"Stopped to protect the server: {str(e)[:300]}",
                finished_at=datetime.datetime.utcnow().isoformat() + "Z",
                elapsed_seconds=round(time.monotonic() - t0, 1))
    except Exception as e:
        import traceback
        traceback.print_exc()
        _cleanup_partial(zip_partial)
        _update(job_id, status="error", stage="error", error=str(e)[:500], message=f"Failed: {str(e)[:300]}",
                finished_at=datetime.datetime.utcnow().isoformat() + "Z",
                elapsed_seconds=round(time.monotonic() - t0, 1))
    finally:
        for report in (quality_rows, data_anomalies):
            if report is not None:
                report.close()
        shutil.rmtree(spool_dir, ignore_errors=True)
        gc.collect()
        with _jobs_lock:
            job = _jobs.get(job_id)
            snapshot = dict(job) if job else None
        if snapshot:
            _save_meta(snapshot)


def _readme(interval: str, bpd: int, compact: bool = False) -> str:
    return f"""MarketPredictor historical market-data export
=============================================

Raw candles from INDstocks, plus reference and event datasets. No trades, no Replay results, no decision-layer output.

Layout
------
historical_data/<YYYY-MM-DD>/<SYMBOL>.csv   one file per stock per trading day (see universe file for file_stem)
market/NIFTY.csv, market/INDIA_VIX.csv      NIFTY 50 and India VIX, whole selected range, same interval
sectors/<INDEX>.csv                         NIFTY sectoral indices, whole selected range, same interval
                                            A file named *_DAILY.csv means the provider would not serve that index
                                            intraday: it holds DAILY candles (see reference/index_probe.csv).
universe/large_midcap.csv                   symbol, exchange, cap_class (LARGE/MID), company, industry, ISIN,
                                            the app's own sector + matching sector index file, per-symbol data status,
                                            how the symbol was resolved (SYMBOL or ISIN) and a note for problem rows
universe/universe_membership.csv            dated SNAPSHOT of membership (effective_from/effective_to columns are there
                                            so successive exports can be stacked into a real history)
universe/sector_membership.csv              dated SNAPSHOT of the app's sector map + NSE industry
reference/instrument_master.csv             the provider's own instrument columns for every stock in the universe
reference/index_instruments.csv             every index the provider lists (exch, name, security_id)
reference/index_probe.csv                   each index code that was tried, and what the provider answered
reference/trading_calendar.csv              per-date session times, holiday flag, and whether candles exist (calendar_conflict = mismatch)
reference/cost_model.json                   brokerage/STT/stamp/exchange/SEBI/GST rates the app is configured with
events/corporate_actions.csv                NSE corporate actions for the universe (price factor is PARSED from text - verify)
events/announcements.csv                    NSE announcements with the source's publication + exchange-received timestamps
events/results.csv                          result filings with timestamps (figures are in the linked XBRL, not parsed here)
events/bulk_deals.csv, block_deals.csv      NSE bulk/block deals
flows/fii_dii_latest.csv                    latest day only (NSE serves no history on that endpoint)
global/<SERIES>.csv                         overnight/global DAILY series; use `known_from_ist`, not `date`, to decide what was
                                            knowable before the 09:15 IST open (Asian same-day closes come AFTER India opens)
quality/data_quality.csv                    bars per symbol per day vs the {bpd} expected for a full {interval} session
quality/fetch_failures.csv                  API windows that failed even after retries (= genuine data gaps)
quality/dataset_status.csv                  every extra dataset: OK / OK_EMPTY / PARTIAL / FAILED / NOT_AVAILABLE and why
manifest.json                               request, coverage, counts, integrity notes, index-series status, processing stats

Candle columns
--------------
{("COMPACT: epoch, open, high, low, close, volume. ISO timestamp is omitted to reduce ZIP size; recover it exactly from epoch in Asia/Kolkata." if compact else "timestamp, open, high, low, close, volume, epoch. Timestamp is IST ISO-8601 and epoch is the provider original.")}

Integrity notes
---------------
* Interval is the provider's native {interval}; nothing is resampled or aggregated, so no candle is
  built from later data.
* The provider's timestamp convention (candle start vs candle end) is preserved as-is. Verify it against
  a known price before relying on entry-at-open logic: the first bar of a session should have
  timestamp 09:15 if timestamps are candle START times.
* Duplicate timestamps (same candle returned twice) were de-duplicated, keeping the last.
* Only sessions that had closed at export time are included.
* A day missing for one stock while other stocks have it is a data gap or suspension, not a holiday --
  check quality/data_quality.csv and quality/fetch_failures.csv.
* Index files may have zero volume; India VIX intraday availability depends on the provider (see manifest.json).
* An extra dataset that is missing is missing for a stated reason in quality/dataset_status.csv. An empty
  file is never used to mean "failed".
* SURVIVORSHIP / LOOK-AHEAD WARNING: cap_class, universe membership and sector are today's classification applied
  to past dates. They are NOT point-in-time. Treat any result that depends on them accordingly.
* NOT AVAILABLE from any source this app can call (listed in dataset_status.csv): historical bid/ask, order book,
  tick data, pre-open auction data, GIFT Nifty history, earnings consensus, historical index reconstitution.
"""
