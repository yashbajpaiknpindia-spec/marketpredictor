"""End-to-end: worker + app._scalper_fetch_quotes + indstocks_client against a mock INDstocks server."""
import os
import sys
import threading
import time
from datetime import datetime
from http.server import HTTPServer
from zoneinfo import ZoneInfo

import pytest

sys.path.insert(0, os.path.dirname(__file__))
from test_indstocks_auth_hardening import Provider, make_handler  # noqa: E402

os.environ.setdefault("INDSTOCKS_API_KEY", "client")
os.environ.setdefault("INDSTOCKS_MPIN", "1234")
os.environ.setdefault("INDSTOCKS_TOTP_SECRET", "JBSWY3DPEHPK3PXP")
os.environ["INDSTOCKS_MIN_REQUEST_GAP"] = "0"
os.environ.pop("INDSTOCKS_ACCESS_TOKEN", None)
os.environ.pop("INDSTOCKS_TOKEN", None)
os.environ["INDSTOCKS_TOKEN_DB_STORE"] = "0"


@pytest.fixture()
def rig(monkeypatch):
    import app as A
    import indstocks_client as c
    p = Provider()
    srv = HTTPServer(("127.0.0.1", 0), make_handler(p))
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    monkeypatch.setattr(c, "BASE_URL", f"http://127.0.0.1:{srv.server_address[1]}")
    c.configure_token_store(None)
    c._token_cache.update({"token": None, "expires_at": 0.0})
    c._token_state.update({"fail_count": 0, "cooldown_until": 0.0, "last_attempt_at": 0.0, "last_mint_at": 0.0})
    c._instrument_cache.update({"equity": None, "loaded_at": 0.0})
    yield A, c, p
    srv.shutdown()


CODES = ["NSE_2885", "NSE_11536"]


def test_full_quote_with_complete_depth_skips_the_second_call(rig):
    A, c, p = rig
    p.full_has_depth = True
    out = A._scalper_fetch_quotes(CODES)
    assert p.calls["full"] == 1 and p.calls["mkt"] == 0
    assert all(out[k]["_depth_levels_valid"] == 5 for k in CODES)
    assert A.SCALPER_DEPTH_DIAG["valid_rows"] == 2
    assert A.SCALPER_DEPTH_DIAG["verdict"].startswith("L5 RECEIVING")


def test_full_quote_without_depth_falls_back_to_mkt_for_missing_codes_only(rig):
    A, c, p = rig
    p.full_has_depth = False
    out = A._scalper_fetch_quotes(CODES)
    assert p.calls["full"] == 1 and p.calls["mkt"] == 1
    assert all(out[k]["_depth_levels_valid"] == 5 for k in CODES)
    assert out["NSE_2885"]["live_price"] == 100.0


def test_worker_records_snapshots_from_documented_payloads(rig):
    A, c, p = rig
    from research.scalper.scalper_live_paper import LivePaperWorker, DEFAULT_SETTINGS
    stored, sessions = [], {}
    settings = {**DEFAULT_SETTINGS, "enabled": True, "poll_interval_ms": 1000}
    w = LivePaperWorker(
        get_settings=lambda: settings, is_trading_day=lambda *_: True,
        get_universe=lambda n: ["RELIANCE", "TCS"], resolve_scrip=A._scalper_resolve_scrip,
        fetch_quotes=A._scalper_fetch_quotes,
        create_session=lambda payload: sessions.setdefault("id", 7),
        update_session=lambda sid, patch: None,
        persist_snapshot_batch=lambda rows: stored.extend(rows),
        persist_trade=lambda t: None, load_open_trades=lambda: [],
        fetch_market_ltp=A._scalper_fetch_market_ltp)
    now = datetime(2026, 10, 7, 10, 0, 5, tzinfo=ZoneInfo("Asia/Kolkata"))
    w._start_session(settings, now)
    assert w.state["mapped_count"] == 2, w.state["last_message"]
    for k in range(3):
        w._poll_once(settings, now.replace(second=5 + k))
        w._last_flush = 0.0
    w._flush_snapshots()
    assert w.state["snapshot_count"] >= 6
    assert len(stored) >= 6, "snapshots must reach persist_snapshot_batch"
    assert all(len(r["bid_prices"]) == 5 and r["bid_prices"][0] > 0 for r in stored)
    assert w.state["depth_coverage_pct"] == 100.0


def test_blocked_token_endpoint_makes_one_attempt_and_never_floods(rig):
    A, c, p = rig
    from research.scalper.scalper_live_paper import LivePaperWorker, DEFAULT_SETTINGS
    p.mode = "cloudflare"
    settings = {**DEFAULT_SETTINGS, "enabled": True}
    w = LivePaperWorker(
        get_settings=lambda: settings, is_trading_day=lambda *_: True,
        get_universe=lambda n: [f"S{i}" for i in range(250)], resolve_scrip=A._scalper_resolve_scrip,
        fetch_quotes=A._scalper_fetch_quotes, create_session=lambda payload: 1,
        update_session=lambda sid, patch: None, persist_snapshot_batch=lambda rows: None,
        persist_trade=lambda t: None, load_open_trades=lambda: [], fetch_market_ltp=A._scalper_fetch_market_ltp)
    now = datetime(2026, 10, 7, 10, 0, 5, tzinfo=ZoneInfo("Asia/Kolkata"))
    w._start_session(settings, now)
    w._poll_once(settings, now)
    assert p.token_calls == 1 and p.calls["instruments"] == 0
    assert w.state["phase"] == "waiting_instruments"
