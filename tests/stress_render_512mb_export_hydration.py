"""Offline Render-like 512 MB stress + persisted-export hydration test.

Runs the real historical_export producer under a 512 MB address-space limit,
then simulates a Render restart by removing the local ZIP and hydrating Replay
from the durable copy. No provider/network/database credentials are required.
"""
import ast
import datetime as dt
import json
import os
import resource
import shutil
import tempfile
import time
import zipfile
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

# Avoid importing provider/network modules with live credentials.
os.environ["INTRADAY_SERVER_AUTO_ON_START"] = "false"
os.environ["PAPER_AUTO_ON_START"] = "false"
os.environ["EXIT_MONITOR_SERVER_AUTO_ON_START"] = "false"
os.environ["DATABASE_URL"] = ""
os.environ["HISTORICAL_EXPORT_PERSIST_DB"] = "0"
os.environ["HISTORICAL_EXPORT_PACE_SECONDS"] = "0"
os.environ["HISTORICAL_EXPORT_MEMORY_LIMIT_MB"] = "512"
os.environ["HISTORICAL_EXPORT_MEMORY_WAIT_SECONDS"] = "0"
os.environ["HISTORICAL_EXPORT_1M_SCRIPS_PER_CALL"] = "1"

# The production requirements contain ijson. The sandbox does not ship it, so provide
# a tiny bounded parser only for this offline test. This is NOT included in the app ZIP.
import sys
sys.path.insert(0, str(ROOT))
from types import ModuleType

if "ijson" not in sys.modules:
    ijson = ModuleType("ijson")
    import json as _json
    def _items(fileobj, prefix):
        dec = _json.JSONDecoder()
        first = True
        buf = ""
        eof = False
        while True:
            chunk = fileobj.read(64 * 1024)
            if chunk:
                buf += chunk.decode("utf-8") if isinstance(chunk, (bytes, bytearray)) else chunk
            else:
                eof = True
            while True:
                buf = buf.lstrip()
                if first:
                    if not buf:
                        break
                    if buf[0] != "[":
                        raise ValueError("expected array")
                    buf = buf[1:]
                    first = False
                buf = buf.lstrip()
                if buf.startswith("]"):
                    return
                if buf.startswith(","):
                    buf = buf[1:]
                    continue
                if not buf:
                    break
                try:
                    obj, idx = dec.raw_decode(buf)
                except ValueError:
                    if eof:
                        raise
                    break
                buf = buf[idx:]
                yield obj
            if eof:
                if buf.strip() in ("", "]"):
                    return
                raise ValueError("incomplete JSON")
    ijson.items = _items
    sys.modules["ijson"] = ijson

import historical_export as he


def _load_hydration_functions():
    source = (ROOT / "app.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    names = ["_parse_historical_export_candle_csv", "_replay_export_member_stems", "_hydrate_replay_cache_from_historical_exports"]
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    ns = {
        "datetime": dt, "pd": pd, "io": __import__("io"), "os": os, "zipfile": zipfile,
        "List": list, "Optional": __import__("typing").Optional, "Dict": dict, "Any": object,
        "ZoneInfo": __import__("zoneinfo").ZoneInfo,
        "INTRADAY_REPLAY_EXPORT_REUSE_ENABLED": True,
        "_cap_allowlist_symbol": lambda s: str(s).upper().removesuffix(".NS"),
    }
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(ROOT / "app.py"), "exec"), ns)
    return ns


def main():
    started = time.monotonic()
    # After imports, cap the actual test process like a conservative Render instance.
    hard = 512 * 1024**2
    resource.setrlimit(resource.RLIMIT_AS, (hard, hard))

    with tempfile.TemporaryDirectory() as td:
        he.EXPORT_DIR = td
        he.PERSIST_DB = False
        he._db_save_meta = lambda *a, **k: None
        he._db_ensure = lambda: False
        he._db_conn = lambda: None
        he._resolve_equity = lambda m: (m["symbol"], "SYMBOL", m["symbol"])
        he.ind.get_nse_instrument_rows = lambda syms: {}
        he.ind.get_index_instrument_rows = lambda: []
        he._memory_used_mb = he._rss_mb
        he._pace_for = lambda interval: 0

        start = dt.date(2026, 7, 1)
        end = dt.date(2026, 7, 28)
        nstocks = 250
        sessions = sum((start + dt.timedelta(days=i)).weekday() < 5 for i in range((end - start).days + 1))

        def fetch(jid, codes, interval, ws, we):
            out = {}
            for code in codes:
                base = 100 + int(code[1:])
                candles = []
                day = ws.date()
                while day < we.date():
                    if day.weekday() < 5:
                        ts = int(dt.datetime.combine(day, dt.time(9, 15), tzinfo=he.IST).timestamp())
                        # 375 one-minute candles: enough rows to produce a real archive but
                        # still small enough that local CI completes quickly.
                        candles.extend({
                            "ts": ts + i * 60,
                            "o": base + i * 0.003,
                            "h": base + i * 0.003 + 0.1,
                            "l": base + i * 0.003 - 0.1,
                            "c": base + i * 0.003 + 0.02,
                            "v": 1000 + i,
                        } for i in range(375))
                    day += dt.timedelta(days=1)
                out[code] = candles
            return out, []

        he._fetch_resilient = fetch
        jid = "abcdef123456"
        he._jobs[jid] = {"job_id": jid, "status": "pending", "progress": {}, "warnings": []}
        symbols = [{"symbol": f"S{i:03d}"} for i in range(nstocks)]
        spec = {
            "symbols": symbols, "start": start, "end": end, "interval": "1m",
            "include_indices": False, "extras": False, "compact": True,
        }
        he._run_job(jid, spec)
        job = he.get_job(jid)
        assert job and job["status"] == "completed", job
        archive = Path(he.zip_path_for_download(jid))
        assert archive.exists()
        with zipfile.ZipFile(archive) as zf:
            assert zf.testzip() is None
            assert "manifest.json" in zf.namelist()
            members = [n for n in zf.namelist() if n.startswith("historical_data/") and n.endswith(".csv")]
            assert len(members) == nstocks * sessions

        # Simulate Render restart/local ephemeral disk loss while retaining the durable copy.
        durable = Path(td) / "durable.zip"
        shutil.copy2(archive, durable)
        archive.unlink()
        assert not archive.exists() and durable.exists()

        class FakeHistory:
            def list_jobs(self, limit=1000):
                return [{
                    "job_id": jid, "status": "completed",
                    "archive_persisted": True, "size_bytes": durable.stat().st_size,
                    "params": {"interval": "1m", "start_date": start.isoformat(), "end_date": end.isoformat()},
                }]
            def archive_access_status(self, job_id):
                return {"accessible": True, "mode": "postgres", "size_bytes": durable.stat().st_size}
            def zip_path_for_download(self, job_id):
                return str(durable)
            @staticmethod
            def file_stem(s):
                import re
                return re.sub(r"[^A-Z0-9._-]", "_", str(s).upper().removesuffix(".NS"))

        ns = _load_hydration_functions()
        ns["historical_export"] = FakeHistory()
        ns["get_db_connection"] = lambda: None
        hydrated = []
        ns["_store_cached_replay_days_bulk"] = lambda rows: hydrated.extend(rows)
        ns["_replay_set_runtime"] = lambda *a, **k: None

        # Hydrate a representative slice of the durable export after restart.
        dates = [start + dt.timedelta(days=i) for i in range((end - start).days + 1) if (start + dt.timedelta(days=i)).weekday() < 5][:5]
        tickers = [f"S{i:03d}.NS" for i in range(20)]
        result = ns["_hydrate_replay_cache_from_historical_exports"](tickers, dates, "1m", "IN", {}, None)
        assert result["historical_export_source_available"] is True
        assert result["historical_export_reused_ticker_days"] == len(tickers) * len(dates)
        assert result["historical_export_remaining_missing"] == 0
        assert len(hydrated) == len(tickers) * len(dates)
        assert all(len(frame) == 375 for *_, frame in hydrated)

        peak_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
        elapsed = round(time.monotonic() - started, 2)
        report = {
            "status": "PASS", "address_space_limit_mb": 512,
            "stocks": nstocks, "trading_sessions": sessions,
            "archive_member_stock_days": nstocks * sessions,
            "archive_mb": round(durable.stat().st_size / 1024**2, 2),
            "archive_crc": "PASS", "local_zip_removed_after_build": True,
            "durable_archive_used_after_restart": True,
            "hydrated_ticker_days": len(hydrated),
            "peak_rss_mb": round(peak_mb, 1), "elapsed_seconds": elapsed,
        }
        Path(ROOT / "RENDER_512MB_SYNTHETIC_TEST_RESULTS.json").write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
