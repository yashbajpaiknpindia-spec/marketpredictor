from pathlib import Path

import pandas as pd

from research.scalper.v12_blind_portfolio_audit import audit_live_paper_session, create_frozen_manifest
from research.scalper.scalper_live_paper import DEFAULT_SETTINGS, _v12_config_sha256


def _fixture():
    session_date = "2026-10-12"
    trades = pd.DataFrame([
        {
            "session_id": 99, "trade_key": "t1", "status": "CLOSED",
            "entry_time": f"{session_date} 09:30:00", "exit_time": f"{session_date} 09:40:00",
            "net_pnl_inr": 400.0, "net_pct": 0.20, "notional_inr": 200000.0,
            "exit_reason": "TARGET",
        },
        {
            "session_id": 99, "trade_key": "t2", "status": "CLOSED",
            "entry_time": f"{session_date} 10:00:00", "exit_time": f"{session_date} 10:10:00",
            "net_pnl_inr": -200.0, "net_pct": -0.10, "notional_inr": 200000.0,
            "exit_reason": "STOP",
        },
    ])
    stamps = pd.date_range(
        f"{session_date} 09:15:00",
        f"{session_date} 15:25:00",
        freq="5s",
    )
    snapshots = pd.DataFrame([
        {"session_id": 99, "ticker": ticker, "captured_at": stamp, "ltp": 100.0}
        for ticker in ("AAA", "BBB")
        for stamp in stamps
    ])
    manifest = {
        "source_commit": "a" * 40,
        "model_sha256": "b" * 64,
        "config_sha256": "c" * 64,
        "frozen_at": "2026-10-09 20:00:00",
        "test_session_date": session_date,
        "entry_rule_version": "v1-frozen",
        "exit_policy_id": "live_v12_profit_lock_l5_flip_v1",
        "cost_model_id": "nse-roundtrip-v1",
    }
    session_manifest = {
        **{k: manifest[k] for k in (
            "source_commit", "model_sha256", "config_sha256",
            "entry_rule_version", "exit_policy_id", "cost_model_id",
        )},
        "session_date": session_date,
    }
    trades["metadata"] = [
        {"audit_manifest": dict(session_manifest), "v12_p_target_first": 0.8, "v12_model_sha256": manifest["model_sha256"], "v12_config_sha256": manifest["config_sha256"]},
        {"audit_manifest": dict(session_manifest), "v12_p_target_first": 0.8, "v12_model_sha256": manifest["model_sha256"], "v12_config_sha256": manifest["config_sha256"]},
    ]
    session = {
        "session_id": 99,
        "session_date": session_date,
        "session_start_at": f"{session_date} 09:15:00",
        "audit_manifest": session_manifest,
    }
    return trades, snapshots, session, manifest


def test_valid_frozen_session_reports_actual_portfolio_pf():
    trades, snapshots, session, manifest = _fixture()
    result = audit_live_paper_session(trades, snapshots, session, manifest)
    assert result["blind_test"] is True, result
    assert result["evidence_class"] == "LIVE_PAPER_PORTFOLIO_LEDGER_AUDIT"
    assert result["trade_count"] == 2
    assert result["wins"] == 1 and result["losses"] == 1
    assert result["profit_factor"] == 2.0
    assert result["net_pnl_inr"] == 200.0


def test_overlapping_positions_disqualify_blind_portfolio_proof():
    trades, snapshots, session, manifest = _fixture()
    trades.loc[1, "entry_time"] = "2026-10-12 09:35:00"
    result = audit_live_paper_session(trades, snapshots, session, manifest)
    assert result["blind_test"] is False
    assert "portfolio_position_overlap" in result["reasons"]


def test_candidate_frozen_after_session_start_is_not_blind():
    trades, snapshots, session, manifest = _fixture()
    manifest["frozen_at"] = "2026-10-12 09:16:00"
    result = audit_live_paper_session(trades, snapshots, session, manifest)
    assert result["blind_test"] is False
    assert "candidate_not_proven_frozen_before_session" in result["reasons"]


def test_post_cutoff_entry_disqualifies_session():
    trades, snapshots, session, manifest = _fixture()
    trades.loc[1, "entry_time"] = "2026-10-12 15:25:00"
    result = audit_live_paper_session(trades, snapshots, session, manifest)
    assert result["blind_test"] is False
    assert "entry_at_or_after_cutoff" in result["reasons"]


def test_incomplete_or_duplicate_snapshot_coverage_disqualifies_session():
    trades, snapshots, session, manifest = _fixture()
    snapshots = snapshots[snapshots["captured_at"].dt.strftime("%H:%M:%S") == "15:25:00"].copy()
    result = audit_live_paper_session(trades, snapshots, session, manifest)
    assert result["blind_test"] is False
    assert "incomplete_full_session_snapshot_coverage" in result["reasons"]

    trades, snapshots, session, manifest = _fixture()
    snapshots = pd.concat([snapshots, snapshots.iloc[[0]]], ignore_index=True)
    result = audit_live_paper_session(trades, snapshots, session, manifest)
    assert result["blind_test"] is False
    assert "duplicate_snapshot_key" in result["reasons"]


def test_trade_metadata_fingerprint_mismatch_disqualifies_session():
    trades, snapshots, session, manifest = _fixture()
    trades.at[1, "metadata"] = {
        "audit_manifest": {
            **session["audit_manifest"],
            "model_sha256": "d" * 64,
        },
        "v12_model_sha256": manifest["model_sha256"],
        "v12_config_sha256": manifest["config_sha256"],
    }
    result = audit_live_paper_session(trades, snapshots, session, manifest)
    assert result["blind_test"] is False
    assert "trade_manifest_mismatch:model_sha256" in result["reasons"]


def test_database_upsert_persists_final_trade_metadata():
    app_source = Path("app.py").read_text(encoding="utf-8")
    assert "metadata=EXCLUDED.metadata" in app_source


def test_frozen_manifest_captures_exact_code_model_and_policy_hashes(tmp_path):
    model = tmp_path / "model.joblib"
    model.write_bytes(b"frozen-model-bytes")
    settings = dict(DEFAULT_SETTINGS)
    manifest = create_frozen_manifest(
        settings,
        source_commit="d" * 40,
        model_path=str(model),
        frozen_at="2026-10-09 20:00:00",
        test_session_date="2026-10-12",
    )
    assert manifest["source_commit"] == "d" * 40
    assert len(manifest["model_sha256"]) == 64
    assert manifest["config_sha256"] == _v12_config_sha256(settings)
    assert manifest["test_session_date"] == "2026-10-12"


def test_stale_per_symbol_snapshots_disqualify_blind_scalper_proof():
    trades, snapshots, session, manifest = _fixture()
    result = audit_live_paper_session(
        trades, snapshots, session, manifest, max_symbol_gap_p95_seconds=1.0
    )
    assert result["blind_test"] is False
    assert result["snapshot_gap_p95_seconds"] == 5.0
    assert "snapshot_freshness_p95_exceeds_limit_or_unverifiable" in result["reasons"]
