"""Regression tests for the experimental V12 fail-closed quote gate.

These tests validate data-integrity behavior only. They do not claim trading
profitability and do not constitute a blind market test.
"""
from datetime import datetime

import pytest

from research.scalper.scalper_live_paper import (
    DEFAULT_SETTINGS,
    LivePaperWorker,
    _parse_depth,
    extract_depth_levels,
)


def _worker():
    return LivePaperWorker(
        get_settings=lambda: dict(DEFAULT_SETTINGS),
        is_trading_day=lambda *_: True,
        get_universe=lambda _limit: [],
        resolve_scrip=lambda symbol: symbol,
        fetch_quotes=lambda _symbols: {},
        create_session=lambda _payload: 1,
        update_session=lambda *_: None,
        persist_snapshot_batch=lambda *_: None,
        persist_trade=lambda *_: None,
        load_open_trades=lambda: [],
    )


def _quote(bid=100.0, ask=100.1, levels=5):
    depth = []
    for i in range(levels):
        depth.append({
            "buy": {"price": bid - i * 0.1, "quantity": 100 + i},
            "sell": {"price": ask + i * 0.1, "quantity": 110 + i},
        })
    return {"market_depth": {"depth": depth}}


def test_complete_five_level_book_has_five_valid_levels():
    parsed = _parse_depth(_quote())
    assert parsed["valid_levels"] == 5
    assert parsed["bid1"] > 0
    assert parsed["ask1"] > parsed["bid1"]
    assert parsed["spread_pct"] > 0
    assert parsed["book_integrity_valid"] is True


def test_missing_book_does_not_look_like_a_valid_zero_spread_quote():
    parsed = _parse_depth({})
    assert parsed["valid_levels"] == 0
    assert parsed["bid1"] == 0
    assert parsed["ask1"] == 0
    assert parsed["spread_pct"] == 0
    assert parsed["book_integrity_valid"] is False


def test_partial_book_is_rejected_by_entry_gate():
    worker = _worker()
    worker._last_signal_direction["TEST"] = 1
    ok, reason = worker._maybe_enter(
        "TEST", "TEST", datetime(2026, 10, 12, 10, 0), 100.0,
        {"bid1": 100.0, "ask1": 100.1, "spread_pct": 0.1,
         "valid_levels": 4, "imbalance_l5": -0.5, "microprice_edge_pct": 0.01},
        {"direction": 1, "confidence": 90.0},
        {**DEFAULT_SETTINGS, "v12_mode": False},
    )
    assert not ok
    assert reason == "v12_research_invalid_or_wide_quote"


@pytest.mark.parametrize(
    "depth",
    [
        {"bid1": 0, "ask1": 100.1, "spread_pct": 0, "valid_levels": 5},
        {"bid1": 100.1, "ask1": 100.0, "spread_pct": 0, "valid_levels": 5},
        {"bid1": 100.0, "ask1": 100.1, "spread_pct": 0, "valid_levels": 5},
        {"bid1": 100.0, "ask1": 100.1, "spread_pct": float("nan"), "valid_levels": 5},
        {"bid1": 100.0, "ask1": 100.1, "spread_pct": 0.2, "valid_levels": 5},
    ],
)
def test_invalid_or_untradeable_top_of_book_is_rejected(depth):
    worker = _worker()
    worker._last_signal_direction["TEST"] = 1
    ok, reason = worker._maybe_enter(
        "TEST", "TEST", datetime(2026, 10, 12, 10, 0), 100.0,
        {**depth, "imbalance_l5": -0.5, "microprice_edge_pct": 0.01},
        {"direction": 1, "confidence": 90.0},
        {**DEFAULT_SETTINGS, "v12_mode": False},
    )
    assert not ok
    assert reason == "v12_research_invalid_or_wide_quote"


def test_research_defaults_fail_closed_without_frozen_calibration():
    assert DEFAULT_SETTINGS["v12_require_calibration"] is True
    assert DEFAULT_SETTINGS["v12_calibration_profile"] is None
    assert DEFAULT_SETTINGS["v12_max_adverse_probability"] == 0.35


def test_out_of_order_depth_levels_fail_integrity_check():
    quote = _quote()
    levels = quote["market_depth"]["depth"]
    levels[2]["buy"]["price"] = levels[1]["buy"]["price"] + 0.05
    parsed = _parse_depth(quote)
    assert parsed["valid_levels"] == 5
    assert parsed["book_integrity_valid"] is False


def test_stale_database_toggle_cannot_disable_v12_calibration_requirement():
    worker = _worker()
    features = {
        "ret_1": 1.0, "ret_3": 1.0, "ret_5": 1.0,
        "range_pct": 1.0, "body_pct": 1.0, "close_location": 1.0,
        "vol_ratio_20": 2.0, "range_ratio_20": 2.0,
        "rs_1": 1.0, "rs_3": 1.0,
        "market_ret_1": 1.0, "market_ret_3": 1.0,
        "ofi_proxy": 0.0, "imbalance_l5": 0.0,
        "microprice_edge_pct": 0.0, "freshness": 1.0,
    }
    settings = {
        **DEFAULT_SETTINGS,
        "v12_mode": True,
        "v12_require_calibration": False,
        "v12_model_enabled": False,
        "v12_calibration_profile": None,
    }
    result = worker._score(features, datetime(2026, 10, 12, 10, 0), settings)
    assert result["calibration_status"] == "missing_profile"
    assert result["direction"] == 0


def test_missing_configured_model_does_not_fall_back_to_heuristic():
    worker = _worker()
    features = {
        "ret_1": 1.0, "ret_3": 1.0, "ret_5": 1.0,
        "range_pct": 1.0, "body_pct": 1.0, "close_location": 1.0,
        "vol_ratio_20": 2.0, "range_ratio_20": 2.0,
        "rs_1": 1.0, "rs_3": 1.0,
        "market_ret_1": 1.0, "market_ret_3": 1.0,
        "ofi_proxy": 0.0, "imbalance_l5": 0.0,
        "microprice_edge_pct": 0.0, "freshness": 1.0,
    }
    settings = {
        **DEFAULT_SETTINGS,
        "v12_mode": True,
        "v12_model_enabled": True,
        "v12_model_artifact_path": "research/scalper/artifacts/definitely_missing_model.joblib",
        "v12_calibration_profile": None,
    }
    result = worker._score(features, datetime(2026, 10, 12, 10, 0), settings)
    assert result["v12_model_artifact_status"] == "unavailable:FileNotFoundError"
    assert result["rejection_reason"] == "v12_model_artifact_unavailable"
    assert result["direction"] == 0


def test_quote_router_integrity_matches_worker_parser():
    valid = extract_depth_levels(_quote())
    assert valid["valid_levels"] == 5
    assert valid["book_integrity_valid"] is True
    malformed = _quote()
    malformed["market_depth"]["depth"][3]["sell"]["price"] = 99.0
    parsed = extract_depth_levels(malformed)
    assert parsed["valid_levels"] == 5
    assert parsed["book_integrity_valid"] is False


def test_session_config_hash_mismatch_blocks_v12_signal():
    worker = _worker()
    worker._session = {
        "audit_manifest": {
            "config_sha256": "0" * 64,
            "model_sha256": "",
        }
    }
    features = {
        "ret_1": 1.0, "ret_3": 1.0, "ret_5": 1.0,
        "range_pct": 1.0, "body_pct": 1.0, "close_location": 1.0,
        "vol_ratio_20": 2.0, "range_ratio_20": 2.0,
        "rs_1": 1.0, "rs_3": 1.0,
        "market_ret_1": 1.0, "market_ret_3": 1.0,
        "ofi_proxy": 0.0, "imbalance_l5": 0.0,
        "microprice_edge_pct": 0.0, "freshness": 1.0,
    }
    settings = {
        **DEFAULT_SETTINGS,
        "v12_mode": True,
        "v12_model_enabled": False,
        "v12_calibration_profile": None,
    }
    result = worker._score(features, datetime(2026, 10, 12, 10, 0), settings)
    assert result["direction"] == 0
    assert result["rejection_reason"] == "v12_config_hash_mismatch"
