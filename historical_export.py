"""
Historical market-data export ("ground truth" dataset builder).

Produces a ZIP of RAW candles straight from INDstocks -- no trades, no Replay
results, no aggregation -- so the data can be analysed independently of this
app's own decision layer:

    historical_data/<YYYY-MM-DD>/<SYMBOL>.csv   one file per stock per day
    market/NIFTY.csv, market/INDIA_VIX.csv      whole selected range, one file each
    sectors/<INDEX>.csv                         NIFTY sectoral indices, whole range
    universe/large_midcap.csv                   symbol, exchange, cap class, ...
    quality/data_quality.csv                    bars per symbol per day vs expected
    quality/fetch_failures.csv                  any API window that failed after retries
    manifest.json, README.txt

Integrity rules this module follows:
  * Candles are requested at the NATIVE interval and written as returned. There
    is no resampling / re-bucketing, so no candle is ever built from later ones.
  * Timestamps are written as IST ISO-8601 with an explicit +05:30 offset, plus
    the provider's original epoch seconds in `epoch`, so nothing is re-derived.
  * Duplicate timestamps (same candle returned by two adjacent API windows) are
    de-duplicated keeping the last one -- the same rule the Replay engine's own
    loader (market_data._candles_to_df) applies -- and counted in the quality file.
  * A failed API window is retried, then RECORDED. A dataset never silently has a
    hole that looks like "no trading that day".

This module is deliberately independent of app.py (no circular import): the
caller resolves the universe and passes it in.
"""
import csv
import datetime
import io
import json
import os
import re
import tempfile
import threading
import time
import uuid
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import indstocks_client as ind
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
WORKERS = max(1, int(os.environ.get("HISTORICAL_EXPORT_WORKERS", "3")))
WINDOW_RETRIES = max(0, int(os.environ.get("HISTORICAL_EXPORT_WINDOW_RETRIES", "2")))
KEEP_EXPORTS = max(1, int(os.environ.get("HISTORICAL_EXPORT_KEEP", "3")))

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
# sector_data's own sector name -> the file stem above, so the universe file can
# say which sector index file belongs to each stock.
SECTOR_NAME_TO_FILE = {
    "Bank": "NIFTYBANK", "IT": "NIFTYIT", "Auto": "NIFTYAUTO", "Pharma": "NIFTYPHARMA",
    "FMCG": "NIFTYFMCG", "Metal": "NIFTYMETAL", "Energy": "NIFTYENERGY",
    "Financial Services": "NIFTYFINSERVICE", "Realty": "NIFTYREALTY", "Media": "NIFTYMEDIA",
    "PSU Bank": "NIFTYPSUBANK", "Infrastructure": "NIFTYINFRA",
}

CANDLE_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume", "epoch"]

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


def _ts_iso(epoch_seconds) -> str:
    return datetime.datetime.fromtimestamp(epoch_seconds, tz=IST).isoformat()


def _ts_date(epoch_seconds) -> datetime.date:
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


def candles_to_csv(candles: List[Dict[str, Any]]) -> str:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(CANDLE_COLUMNS)
    for c in candles:
        ts = c["ts"]
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
             trading_days: int, include_indices: bool) -> Dict[str, Any]:
    n_series = (len(MARKET_SERIES) + len(SECTOR_SERIES)) if include_indices else 0
    bpd = bars_per_day(interval)
    rows = (n_symbols + n_series) * trading_days * bpd
    span_days = (end - start).days + 1
    windows = -(-span_days // 7)  # 5m/1m provider windows are 7 calendar days
    calls = (-(-n_symbols // 5) + (-(-n_series // 5) if n_series else 0)) * windows
    return {
        "symbols": n_symbols, "index_series": n_series, "trading_days_est": trading_days,
        "bars_per_day": bpd, "rows_est": rows, "api_calls_est": calls,
        "zip_mb_est": round(rows * 22 / 1e6, 1),
        "too_large": rows > MAX_ROWS, "max_rows": MAX_ROWS,
    }


# ---------------------------------------------------------------------------
# Job registry (single gunicorn worker + threads -> in-process is enough; the
# finished ZIP and a small meta file live on disk so a worker restart on the
# same instance doesn't lose a finished export).
# ---------------------------------------------------------------------------

_jobs: Dict[str, Dict[str, Any]] = {}
_jobs_lock = threading.Lock()


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
        print(f"[historical_export] could not save job meta: {e}")


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
                # The process that owned it is gone (restart) -- it can't still be running.
                data["status"] = "error"
                data["error"] = "Server restarted while this export was running. Start it again."
            return data
        except Exception:
            return None
    return None


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
    out = sorted(seen.values(), key=lambda j: j.get("created_at") or "", reverse=True)
    return out[:limit]


def zip_path_for_download(job_id: str) -> Optional[str]:
    job = get_job(job_id)
    if not job or job.get("status") != "completed":
        return None
    path = _job_paths(job_id)[0]
    return path if os.path.exists(path) else None


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


def start_job(spec: Dict[str, Any]) -> Dict[str, Any]:
    """spec keys: symbols (list of dicts: symbol, exchange, cap_class, company_name,
    industry, isin, app_sector), interval, start (date), end (date), explicit (set|None),
    include_indices (bool), universe_mode (str), universe_source (str),
    universe_notes (list[str]), warnings (list[str])."""
    if not ind.credentials_configured():
        return {"ok": False, "error": "INDstocks credentials are not configured (INDSTOCKS_API_KEY / INDSTOCKS_MPIN / INDSTOCKS_TOTP_SECRET)."}
    job_id = uuid.uuid4().hex[:12]
    job = {
        "job_id": job_id, "status": "pending", "stage": "queued", "message": "Queued...",
        "created_at": datetime.datetime.utcnow().isoformat() + "Z", "finished_at": None,
        "params": {
            "interval": spec["interval"], "start_date": spec["start"].isoformat(), "end_date": spec["end"].isoformat(),
            "specific_dates": sorted(d.isoformat() for d in spec["explicit"]) if spec.get("explicit") else None,
            "include_indices": bool(spec.get("include_indices", True)),
            "universe_mode": spec.get("universe_mode"), "universe_source": spec.get("universe_source"),
            "symbols_requested": len(spec["symbols"]),
        },
        "progress": {"chunks_done": 0, "chunks_total": 0, "symbols_with_data": 0, "symbols_without_data": 0,
                     "rows_written": 0, "files_written": 0},
        "warnings": list(spec.get("warnings") or []), "error": None, "file_name": None, "size_bytes": None,
        "elapsed_seconds": 0.0, "_cancel": False,
    }
    with _jobs_lock:
        running = next((jid for jid, j in _jobs.items() if j.get("status") in ("pending", "running")), None)
        if running:
            return {"ok": False, "error": f"Another historical export ({running}) is still running. Wait for it or cancel it first."}
        _jobs[job_id] = job
    _cleanup_old()
    t = threading.Thread(target=_run_job, args=(job_id, spec), daemon=True, name=f"hist-export-{job_id}")
    t.start()
    return {"ok": True, "job_id": job_id, "job": _public(job)}


def _update(job_id: str, **kwargs) -> None:
    with _jobs_lock:
        job = _jobs.get(job_id)
        if job:
            for k, v in kwargs.items():
                if k == "progress":
                    job["progress"].update(v)
                else:
                    job[k] = v


def _is_cancelled(job_id: str) -> bool:
    with _jobs_lock:
        job = _jobs.get(job_id)
        return bool(job and job.get("_cancel"))


# ---------------------------------------------------------------------------
# The export itself
# ---------------------------------------------------------------------------

def _resolve_series(series_list):
    """[(stem, index_name, ...)] -> ([(stem, index_name, scrip)], [unresolved dicts]).
    Exact-name matching only, and two series may never share one instrument."""
    ok, bad = [], []
    taken: Dict[str, str] = {}
    for item in series_list:
        stem, name = item[0], item[1]
        try:
            scrip = ind.resolve_index_scrip_code_exact(name)
        except Exception as e:
            bad.append({"series": stem, "index_name": name, "error": str(e)[:200]})
            continue
        if scrip in taken:
            bad.append({"series": stem, "index_name": name, "error": f"resolves to the same instrument as {taken[scrip]}"})
            continue
        taken[scrip] = stem
        ok.append((stem, name, scrip))
    return ok, bad


def _day_wanted(d: datetime.date, start: datetime.date, end: datetime.date, explicit) -> bool:
    if explicit is not None:
        return d in explicit
    return start <= d <= end


def _fetch_chunk(job_id, codes, interval, fetch_start, fetch_end):
    return ind.get_historical_with_report(
        codes, interval, fetch_start, fetch_end,
        window_retries=WINDOW_RETRIES, cancel_check=lambda: _is_cancelled(job_id))


def _run_job(job_id: str, spec: Dict[str, Any]) -> None:
    t0 = time.monotonic()
    zip_final, zip_partial, _meta = _job_paths(job_id)
    interval = spec["interval"]
    start, end, explicit = spec["start"], spec["end"], spec.get("explicit")
    fetch_start = _ist_midnight(start)
    fetch_end = _ist_midnight(end + datetime.timedelta(days=1))
    bpd = bars_per_day(interval)
    generated_at = datetime.datetime.utcnow().isoformat() + "Z"

    quality_rows: List[Dict[str, Any]] = []
    failures_all: List[Dict[str, Any]] = []
    unresolved_symbols: List[Dict[str, Any]] = []
    universe_rows: List[Dict[str, Any]] = []
    series_coverage: List[Dict[str, Any]] = []
    days_with_data = set()
    rows_written = 0
    files_written = 0
    symbols_with_data = 0
    symbols_without_data = 0

    def refresh(stage, message, **prog):
        _update(job_id, stage=stage, message=message, elapsed_seconds=round(time.monotonic() - t0, 1),
                progress={"rows_written": rows_written, "files_written": files_written,
                          "symbols_with_data": symbols_with_data, "symbols_without_data": symbols_without_data, **prog})

    try:
        os.makedirs(EXPORT_DIR, exist_ok=True)
        _update(job_id, status="running")
        refresh("resolving", "Resolving instrument codes...")

        # ---- resolve equity scrip codes ---------------------------------
        sym_by_scrip: Dict[str, Dict[str, Any]] = {}
        for meta in spec["symbols"]:
            sym = meta["symbol"]
            try:
                scrip = ind.resolve_equity_scrip_code_strict(sym)
                if scrip in sym_by_scrip:
                    continue  # same instrument listed twice in the universe
                sym_by_scrip[scrip] = meta
            except Exception as e:
                unresolved_symbols.append({"symbol": sym, "error": str(e)[:200]})
                universe_rows.append(_universe_row(meta, "UNRESOLVED", 0, 0))
        equity_scrips = list(sym_by_scrip.keys())
        eq_chunks = [equity_scrips[i:i + 5] for i in range(0, len(equity_scrips), 5)]

        index_series: List[tuple] = []
        unresolved_series: List[Dict[str, Any]] = []
        if spec.get("include_indices", True):
            index_series, unresolved_series = _resolve_series(
                [(s, n) for (s, n, _g) in MARKET_SERIES] + SECTOR_SERIES)
        sector_scrip_to_series = {scrip: (stem, name) for stem, name, scrip in index_series}
        ix_scrips = list(sector_scrip_to_series.keys())
        ix_chunks = [ix_scrips[i:i + 5] for i in range(0, len(ix_scrips), 5)]

        total_chunks = len(eq_chunks) + len(ix_chunks)
        chunks_done = 0
        refresh("equities", f"Fetching {len(equity_scrips)} stocks ({interval})...", chunks_total=total_chunks, chunks_done=0)

        if not equity_scrips:
            raise RuntimeError("None of the selected symbols could be resolved in the INDstocks instrument master.")

        with zipfile.ZipFile(zip_partial, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
            # ---- equities: chunks fetched in parallel, written by THIS thread only
            with ThreadPoolExecutor(max_workers=WORKERS) as pool:
                futs = {pool.submit(_fetch_chunk, job_id, chunk, interval, fetch_start, fetch_end): chunk
                        for chunk in eq_chunks}
                for fut in as_completed(futs):
                    if _is_cancelled(job_id):
                        for f in futs:
                            f.cancel()
                        raise _Cancelled()
                    chunk = futs.pop(fut)  # drop our reference so the chunk's candles can be freed
                    try:
                        result, failures = fut.result()
                    except Exception as e:
                        result, failures = {}, [{"scrips": chunk, "start": fetch_start.isoformat(),
                                                 "end": fetch_end.isoformat(), "error": f"chunk failed: {e}"[:300]}]
                    for fl in failures:
                        fl["scrips"] = [sym_by_scrip[s]["symbol"] if s in sym_by_scrip else s for s in fl["scrips"]]
                        failures_all.append(fl)
                    for scrip in chunk:
                        meta = sym_by_scrip[scrip]
                        candles, dups = dedupe_sorted(result.get(scrip, []))
                        by_day: Dict[datetime.date, List[Dict[str, Any]]] = {}
                        for c in candles:
                            d = _ts_date(c["ts"])
                            if _day_wanted(d, start, end, explicit):
                                by_day.setdefault(d, []).append(c)
                        if not by_day:
                            symbols_without_data += 1
                            universe_rows.append(_universe_row(meta, "NO_DATA", 0, 0))
                            continue
                        symbols_with_data += 1
                        stem = file_stem(meta["symbol"])
                        n_rows = 0
                        for d in sorted(by_day):
                            day_candles = by_day[d]
                            zf.writestr(f"historical_data/{d.isoformat()}/{stem}.csv", candles_to_csv(day_candles))
                            files_written += 1
                            n_rows += len(day_candles)
                            days_with_data.add(d)
                            quality_rows.append({
                                "symbol": meta["symbol"], "date": d.isoformat(), "bars": len(day_candles),
                                "expected_bars": bpd, "complete": len(day_candles) == bpd,
                                "first_timestamp": _ts_iso(day_candles[0]["ts"]),
                                "last_timestamp": _ts_iso(day_candles[-1]["ts"]),
                            })
                        rows_written += n_rows
                        universe_rows.append(_universe_row(meta, "OK", len(by_day), n_rows, duplicates_removed=dups))
                    chunks_done += 1
                    refresh("equities", f"Fetched {chunks_done}/{total_chunks} batches...", chunks_total=total_chunks, chunks_done=chunks_done)

            # ---- indices ---------------------------------------------------
            if ix_chunks:
                refresh("indices", "Fetching NIFTY, India VIX and sector indices...", chunks_total=total_chunks, chunks_done=chunks_done)
                with ThreadPoolExecutor(max_workers=min(WORKERS, len(ix_chunks))) as pool:
                    futs = {pool.submit(_fetch_chunk, job_id, chunk, interval, fetch_start, fetch_end): chunk
                            for chunk in ix_chunks}
                    for fut in as_completed(futs):
                        if _is_cancelled(job_id):
                            for f in futs:
                                f.cancel()
                            raise _Cancelled()
                        chunk = futs.pop(fut)
                        try:
                            result, failures = fut.result()
                        except Exception as e:
                            result, failures = {}, [{"scrips": chunk, "start": fetch_start.isoformat(),
                                                     "end": fetch_end.isoformat(), "error": f"chunk failed: {e}"[:300]}]
                        for fl in failures:
                            fl["scrips"] = [sector_scrip_to_series[s][0] if s in sector_scrip_to_series else s for s in fl["scrips"]]
                            failures_all.append(fl)
                        for scrip in chunk:
                            stem, name = sector_scrip_to_series[scrip]
                            candles, dups = dedupe_sorted(result.get(scrip, []))
                            candles = [c for c in candles if _day_wanted(_ts_date(c["ts"]), start, end, explicit)]
                            folder = "market" if stem in ("NIFTY", "INDIA_VIX") else "sectors"
                            if candles:
                                zf.writestr(f"{folder}/{stem}.csv", candles_to_csv(candles))
                                files_written += 1
                                rows_written += len(candles)
                            series_coverage.append({
                                "series": stem, "index_name": name, "folder": folder, "rows": len(candles),
                                "days": len({_ts_date(c['ts']) for c in candles}),
                                "first_timestamp": _ts_iso(candles[0]["ts"]) if candles else None,
                                "last_timestamp": _ts_iso(candles[-1]["ts"]) if candles else None,
                                "duplicates_removed": dups,
                                "status": "OK" if candles else "NO_DATA",
                            })
                        chunks_done += 1
                        refresh("indices", f"Fetched {chunks_done}/{total_chunks} batches...", chunks_total=total_chunks, chunks_done=chunks_done)

            # ---- universe / quality / manifest / README -------------------
            refresh("packaging", "Writing universe, quality report and manifest...", chunks_total=total_chunks, chunks_done=chunks_done)
            universe_rows.sort(key=lambda r: r["symbol"])
            zf.writestr("universe/large_midcap.csv", _rows_to_csv(UNIVERSE_COLUMNS, universe_rows))
            quality_rows.sort(key=lambda r: (r["symbol"], r["date"]))
            zf.writestr("quality/data_quality.csv",
                        _rows_to_csv(["symbol", "date", "bars", "expected_bars", "complete", "first_timestamp", "last_timestamp"], quality_rows))
            zf.writestr("quality/fetch_failures.csv",
                        _rows_to_csv(["scrips", "start", "end", "error"],
                                     [{**f, "scrips": ";".join(f["scrips"])} for f in failures_all]))

            days_sorted = sorted(days_with_data)
            incomplete = sum(1 for r in quality_rows if not r["complete"])
            manifest = {
                "generated_at_utc": generated_at,
                "app": "AI Market Scanner / MarketPredictor",
                "data_source": "INDstocks (IndMoney) historical candles API",
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
                    "symbols_with_data": symbols_with_data,
                    "symbols_without_data": symbols_without_data,
                    "symbols_unresolved": unresolved_symbols,
                    "is_point_in_time": False,
                },
                "coverage": {
                    "trading_days_with_data": len(days_sorted),
                    "first_day": days_sorted[0].isoformat() if days_sorted else None,
                    "last_day": days_sorted[-1].isoformat() if days_sorted else None,
                    "days": [d.isoformat() for d in days_sorted],
                    "stock_day_files": sum(1 for _ in quality_rows),
                    "stock_days_incomplete": incomplete,
                    "rows_total_incl_indices": rows_written,
                },
                "index_series": series_coverage,
                "index_series_unresolved": unresolved_series,
                "fetch_windows_failed_after_retries": len(failures_all),
                "integrity": {
                    "aggregation": "none -- candles are the provider's native interval, written as returned",
                    "price_adjustment": "none applied by this app; whatever INDstocks serves is what is exported",
                    "duplicate_timestamps": "de-duplicated keeping the last occurrence (same rule the Replay engine's loader uses)",
                    "session_filter": "none -- pre/post-session bars, if the provider returns any, are kept as returned",
                    "incomplete_sessions_excluded": "only sessions that have closed (>= 15:45 IST) are requested",
                },
                "warnings": list(spec.get("warnings") or []),
            }
            zf.writestr("manifest.json", json.dumps(manifest, indent=2, default=str))
            zf.writestr("README.txt", _readme(interval, bpd))

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
        if symbols_without_data:
            warns.append(f"{symbols_without_data} symbol(s) returned no candles in this range.")
        if unresolved_symbols:
            warns.append(f"{len(unresolved_symbols)} symbol(s) could not be resolved in the INDstocks instrument master.")
        if unresolved_series:
            warns.append("Index series not found at INDstocks: " + ", ".join(u["series"] for u in unresolved_series))
        empty_series = [s["series"] for s in series_coverage if s["status"] == "NO_DATA"]
        if empty_series:
            warns.append("No intraday candles returned for: " + ", ".join(empty_series))
        if incomplete:
            warns.append(f"{incomplete} stock-day file(s) have fewer bars than a full {interval} session ({bpd}); "
                         f"see quality/data_quality.csv (early-close days and thinly traded stocks show up here).")
        if not days_sorted:
            warns.append("No trading days with data were found in the requested range.")

        _update(job_id, status="completed", stage="done", finished_at=datetime.datetime.utcnow().isoformat() + "Z",
                message=(f"Done: {symbols_with_data} stocks x {len(days_sorted)} trading days, "
                         f"{rows_written:,} candle rows, {size / 1e6:.1f} MB."),
                file_name=file_name, size_bytes=size, warnings=warns,
                elapsed_seconds=round(time.monotonic() - t0, 1),
                progress={"rows_written": rows_written, "files_written": files_written,
                          "symbols_with_data": symbols_with_data, "symbols_without_data": symbols_without_data,
                          "chunks_done": chunks_done, "chunks_total": total_chunks},
                summary={"first_day": days_sorted[0].isoformat() if days_sorted else None,
                         "last_day": days_sorted[-1].isoformat() if days_sorted else None,
                         "trading_days": len(days_sorted), "failed_windows": len(failures_all),
                         "incomplete_stock_days": incomplete})
    except _Cancelled:
        _cleanup_partial(zip_partial)
        _update(job_id, status="cancelled", stage="cancelled", message="Cancelled.",
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
        with _jobs_lock:
            job = _jobs.get(job_id)
            snapshot = dict(job) if job else None
        if snapshot:
            _save_meta(snapshot)


class _Cancelled(Exception):
    pass


def _cleanup_partial(path: str) -> None:
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception:
        pass


UNIVERSE_COLUMNS = ["symbol", "file_stem", "exchange", "cap_class", "company_name", "industry", "isin",
                    "app_sector", "sector_index_file", "data_status", "days_with_data", "rows", "duplicates_removed"]


def _universe_row(meta: Dict[str, Any], status: str, days: int, rows: int, duplicates_removed: int = 0) -> Dict[str, Any]:
    sector = meta.get("app_sector") or sector_data.get_sector(meta["symbol"]) or ""
    return {
        "symbol": meta["symbol"], "file_stem": file_stem(meta["symbol"]), "exchange": meta.get("exchange") or "NSE",
        "cap_class": meta.get("cap_class") or "UNKNOWN", "company_name": meta.get("company_name") or "",
        "industry": meta.get("industry") or "", "isin": meta.get("isin") or "",
        "app_sector": sector, "sector_index_file": SECTOR_NAME_TO_FILE.get(sector, ""),
        "data_status": status, "days_with_data": days, "rows": rows, "duplicates_removed": duplicates_removed,
    }


def _readme(interval: str, bpd: int) -> str:
    return f"""MarketPredictor historical market-data export
=============================================

Raw candles from INDstocks. No trades, no Replay results, no decision-layer output.

Layout
------
historical_data/<YYYY-MM-DD>/<SYMBOL>.csv   one file per stock per trading day (see universe file for file_stem)
market/NIFTY.csv, market/INDIA_VIX.csv      NIFTY 50 and India VIX, whole selected range, same interval
sectors/<INDEX>.csv                         NIFTY sectoral indices, whole selected range, same interval
universe/large_midcap.csv                   symbol, exchange, cap_class (LARGE/MID), company, industry, ISIN,
                                            the app's own sector + matching sector index file, and per-symbol data status
quality/data_quality.csv                    bars per symbol per day vs the {bpd} expected for a full {interval} session
quality/fetch_failures.csv                  API windows that failed even after retries (= genuine data gaps)
manifest.json                               request, coverage, counts, integrity notes

Candle columns
--------------
timestamp  IST wall-clock time of the candle as returned by the provider, ISO-8601 with +05:30 offset
open, high, low, close, volume   as returned by the provider (no rounding, no adjustment by this app)
epoch      the provider's original unix timestamp in seconds (timestamp is derived from it, nothing else)

Integrity notes
---------------
* Interval is the provider's native {interval}; nothing is resampled or aggregated, so no candle is
  built from later data.
* The provider's timestamp convention (candle start vs candle end) is preserved as-is. Verify it against
  a known price before relying on entry-at-open logic: the first bar of a session should have
  timestamp 09:15 if timestamps are candle START times.
* Duplicate timestamps (same candle returned by two adjacent API windows) were de-duplicated, keeping the last.
* Only sessions that had closed at export time are included.
* A day missing for one stock while other stocks have it is a data gap or suspension, not a holiday --
  check quality/data_quality.csv and quality/fetch_failures.csv.
* Index files may have zero volume; India VIX intraday availability depends on the provider (see manifest.json).
* SURVIVORSHIP / LOOK-AHEAD WARNING: cap_class and universe membership are today's NSE index constituents applied
  to past dates. They are NOT point-in-time. Treat any result that depends on universe membership accordingly.
"""
