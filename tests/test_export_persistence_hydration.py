"""Synthetic tests for persistent Export Data durability + Replay hydration.

These tests intentionally use the real source functions via AST extraction so they
can run offline without DATABASE_URL/provider credentials. They catch the Render
restart case where a local metadata file says an archive is persistent even though
PostgreSQL has already evicted its chunks.
"""
import ast
import csv
import datetime as dt
import json
import os
import shutil
import tempfile
import threading
import zipfile
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
HIST = (ROOT / "historical_export.py").read_text(encoding="utf-8")
APP = (ROOT / "app.py").read_text(encoding="utf-8")


def _fn(source, name):
    tree = ast.parse(source)
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
    return node


def _load_get_job(local_meta, db_meta):
    ns = {"Optional": __import__("typing").Optional, "Dict": dict, "Any": object, "json": json, "os": os}
    nodes = [_fn(HIST, "get_job")]
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(ROOT / "historical_export.py"), "exec"), ns)
    lock = threading.Lock()
    ns.update({
        "_jobs": {},
        "_jobs_lock": lock,
        "_public": lambda x: dict(x),
        "valid_job_id": lambda x: x == "abcdef123456",
        "_job_paths": lambda jid: ("", "", local_meta),
        "_db_load_meta": lambda jid: dict(db_meta) if db_meta else None,
        "_db_save_meta": lambda *a, **k: None,
        "json": json,
        "os": os,
    })
    return ns["get_job"]


def test_stale_local_persistence_flag_is_overridden_by_db():
    with tempfile.TemporaryDirectory() as td:
        meta = Path(td) / "abcdef123456.meta.json"
        meta.write_text(json.dumps({
            "job_id": "abcdef123456", "status": "completed",
            "archive_persisted": True, "storage": "postgres", "size_bytes": 123,
        }))
        get_job = _load_get_job(str(meta), {
            "job_id": "abcdef123456", "status": "completed",
            "archive_persisted": False, "storage": "metadata-only", "size_bytes": 123,
        })
        got = get_job("abcdef123456")
        assert got["archive_persisted"] is False
        assert got["storage"] == "metadata-only"


def _load_hydrator():
    names = [
        "_parse_historical_export_candle_csv",
        "_replay_export_member_stems",
        "_hydrate_replay_cache_from_historical_exports",
    ]
    tree = ast.parse(APP)
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    ns = {
        "datetime": dt,
        "datetime_module": dt,
        "pd": pd,
        "io": __import__("io"),
        "os": os,
        "zipfile": zipfile,
        "List": list,
        "Optional": __import__("typing").Optional,
        "Dict": dict,
        "Any": object,
        "ZoneInfo": __import__("zoneinfo").ZoneInfo,
        "INTRADAY_REPLAY_EXPORT_REUSE_ENABLED": True,
        "_cap_allowlist_symbol": lambda s: str(s).upper().removesuffix(".NS"),
    }
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(ROOT / "app.py"), "exec"), ns)
    return ns


class FakeHistory:
    def __init__(self, restored_archive, job):
        self.restored_archive = Path(restored_archive)
        self.job = dict(job)

    @staticmethod
    def file_stem(s):
        s = str(s).upper().replace("&", "_AND_")
        import re
        return re.sub(r"[^A-Z0-9._-]", "_", s)

    def list_jobs(self, limit=1000):
        return [dict(self.job)]

    def archive_access_status(self, job_id):
        return {"accessible": True, "mode": "postgres", "size_bytes": self.restored_archive.stat().st_size}

    def zip_path_for_download(self, job_id):
        return str(self.restored_archive)


def _make_export_zip(path, dates, tickers):
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=1) as zf:
        for day in dates:
            for ticker in tickers:
                member = f"historical_data/{day.isoformat()}/{ticker}.csv"
                buf = []
                buf.append("epoch,open,high,low,close,volume\n")
                start = int(dt.datetime.combine(day, dt.time(9, 15), tzinfo=dt.timezone(dt.timedelta(hours=5, minutes=30))).timestamp())
                for i in range(10):
                    px = 100 + i * 0.1
                    buf.append(f"{start+i*60},{px:.2f},{px+0.1:.2f},{px-0.1:.2f},{px+0.05:.2f},{100+i}\n")
                zf.writestr(member, "".join(buf))
        zf.writestr("manifest.json", json.dumps({"synthetic": True, "interval": "1m"}))


def test_persistent_export_hydrates_without_local_zip_and_without_network():
    ns = _load_hydrator()
    dates = [dt.date(2026, 7, 28), dt.date(2026, 7, 29)]
    tickers = ["AAA.NS", "BBB.NS", "CCC.NS"]
    with tempfile.TemporaryDirectory() as td:
        source = Path(td) / "source.zip"
        durable = Path(td) / "durable.zip"
        _make_export_zip(source, dates, ["AAA", "BBB", "CCC"])
        shutil.copy2(source, durable)
        source.unlink()  # simulate Render local-disk loss after deploy

        job = {
            "job_id": "abcdef123456", "status": "completed",
            "params": {"interval": "1m", "start_date": dates[0].isoformat(), "end_date": dates[-1].isoformat()},
            "archive_persisted": True, "size_bytes": durable.stat().st_size,
        }
        fake_history = FakeHistory(durable, job)

        stored = []
        ns["historical_export"] = fake_history
        ns["get_db_connection"] = lambda: None
        ns["_store_cached_replay_days_bulk"] = lambda rows: stored.extend(rows)
        ns["_replay_set_runtime"] = lambda *a, **k: None

        result = ns["_hydrate_replay_cache_from_historical_exports"](
            tickers=tickers,
            trading_dates=dates,
            interval="1m",
            market="IN",
            stats_out={},
            runtime_run_id=None,
        )

        assert result["historical_export_source_available"] is True
        assert result["historical_export_reused_ticker_days"] == len(tickers) * len(dates)
        assert result["historical_export_remaining_missing"] == 0
        assert len(stored) == len(tickers) * len(dates)
        assert all(not frame.empty and len(frame) == 10 for _, _, _, _, frame in stored)


def test_zip_is_valid_and_manifest_is_present():
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "synthetic.zip"
        _make_export_zip(path, [dt.date(2026, 7, 28)], ["AAA"])
        with zipfile.ZipFile(path) as zf:
            assert zf.testzip() is None
            assert "manifest.json" in zf.namelist()
            assert "historical_data/2026-07-28/AAA.csv" in zf.namelist()
