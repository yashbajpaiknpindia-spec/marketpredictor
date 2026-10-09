"""Auditor for genuine V12 blind paper-session evidence.

This module never simulates candidate outcomes. It audits the actual closed
paper-trade ledger and marks a session blind-test eligible only when the
strategy manifest was frozen before the session and the portfolio ledger is
complete, time-consistent, non-overlapping, and covered by full-session data.
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, Optional
import math
import re
import pandas as pd


_REQUIRED_MANIFEST = (
    "source_commit",
    "model_sha256",
    "config_sha256",
    "frozen_at",
    "test_session_date",
    "entry_rule_version",
    "exit_policy_id",
    "cost_model_id",
)


def _ts(value: Any) -> pd.Timestamp:
    return pd.to_datetime(value, errors="coerce")


def _has_value(value: Any) -> bool:
    return value is not None and str(value).strip() not in ("", "None", "nan", "NaT")


def _drawdown(values: Iterable[float]) -> float:
    running = 0.0
    peak = 0.0
    max_dd = 0.0
    for value in values:
        running += float(value)
        peak = max(peak, running)
        max_dd = max(max_dd, peak - running)
    return max_dd


def create_frozen_manifest(
    settings: Dict[str, Any],
    *,
    source_commit: str,
    model_path: str,
    frozen_at: str,
    test_session_date: str,
) -> Dict[str, Any]:
    """Build the manifest before the test session from exact code/model/settings hashes."""
    from research.scalper.scalper_live_paper import (
        DEFAULT_SETTINGS,
        _sha256_file,
        _v12_config_sha256,
    )

    return {
        "source_commit": str(source_commit or ""),
        "model_sha256": _sha256_file(model_path),
        "config_sha256": _v12_config_sha256(settings),
        "frozen_at": str(frozen_at),
        "test_session_date": str(test_session_date)[:10],
        "entry_rule_version": str(settings.get("v12_entry_rule_version") or DEFAULT_SETTINGS["v12_entry_rule_version"]),
        "exit_policy_id": str(settings.get("v12_calibration_exit_policy_id") or DEFAULT_SETTINGS["v12_calibration_exit_policy_id"]),
        "cost_model_id": str(settings.get("v12_cost_model_id") or DEFAULT_SETTINGS["v12_cost_model_id"]),
    }


def audit_live_paper_session(
    trades: pd.DataFrame,
    snapshots: pd.DataFrame,
    session: Dict[str, Any],
    manifest: Dict[str, Any],
    *,
    entry_cutoff: str = "15:25:00",
    exit_cutoff: str = "15:25:00",
    market_open: str = "09:15:00",
    market_close: str = "15:25:00",
    max_positions: int = 1,
    max_symbol_gap_p95_seconds: float = 5.0,
) -> Dict[str, Any]:
    """Audit a real paper-session ledger and determine blind-test eligibility.

    `trades` must contain actual persisted paper trades, not candidate-scoring
    rows. `snapshots` must contain the entire test session. Timestamps are
    interpreted in the exchange-local timezone already used by the application.
    """
    reasons = []
    required_manifest_missing = [key for key in _REQUIRED_MANIFEST if not _has_value(manifest.get(key))]
    if required_manifest_missing:
        reasons.append("manifest_missing:" + ",".join(required_manifest_missing))

    session_date = str(session.get("session_date") or manifest.get("test_session_date") or "")[:10]
    if not session_date:
        reasons.append("session_date_missing")
    if str(manifest.get("test_session_date") or "")[:10] != session_date:
        reasons.append("manifest_session_date_mismatch")

    frozen_at = _ts(manifest.get("frozen_at"))
    session_start = _ts(session.get("started_at") or session.get("session_start_at"))
    if pd.isna(session_start) and session_date:
        session_start = _ts(f"{session_date} {market_open}")
    frozen_before_start = bool(not pd.isna(frozen_at) and not pd.isna(session_start) and frozen_at < session_start)
    if not frozen_before_start:
        reasons.append("candidate_not_proven_frozen_before_session")

    snapshot_count = int(len(snapshots))
    full_session_coverage = False
    snapshot_gap_p95_seconds = None
    snapshot_gap_max_seconds = None
    freshness_pass = False
    first_snapshot = last_snapshot = None
    if snapshot_count and "captured_at" in snapshots.columns:
        snap_frame = snapshots.copy()
        snap_frame["captured_at"] = pd.to_datetime(snap_frame["captured_at"], errors="coerce")
        snap_times = snap_frame["captured_at"].dropna()
        if len(snap_times):
            first_snapshot = snap_times.min()
            last_snapshot = snap_times.max()
            expected_open = _ts(f"{session_date} {market_open}") if session_date else pd.NaT
            expected_close = _ts(f"{session_date} {market_close}") if session_date else pd.NaT
            full_session_coverage = bool(
                not pd.isna(expected_open) and not pd.isna(expected_close)
                and first_snapshot <= expected_open + pd.Timedelta(minutes=5)
                and last_snapshot >= expected_close - pd.Timedelta(minutes=5)
            )
            gap_group_cols = ["ticker"] if "ticker" in snap_frame.columns else []
            if "session_id" in snap_frame.columns:
                gap_group_cols.insert(0, "session_id")
            if gap_group_cols:
                gaps = (
                    snap_frame.sort_values(gap_group_cols + ["captured_at"])
                    .groupby(gap_group_cols, sort=False)["captured_at"]
                    .diff().dt.total_seconds().dropna()
                )
                if len(gaps):
                    snapshot_gap_p95_seconds = float(gaps.quantile(0.95))
                    snapshot_gap_max_seconds = float(gaps.max())
                    freshness_pass = snapshot_gap_p95_seconds <= float(max_symbol_gap_p95_seconds)
        if "session_id" in snapshots.columns and session.get("session_id") is not None:
            if not snapshots["session_id"].eq(session["session_id"]).all():
                reasons.append("snapshot_session_id_mismatch")
        duplicate_keys = [k for k in ("session_id", "ticker", "captured_at") if k in snapshots.columns]
        if len(duplicate_keys) >= 2 and snapshots.duplicated(duplicate_keys).any():
            reasons.append("duplicate_snapshot_key")
    if not full_session_coverage:
        reasons.append("incomplete_full_session_snapshot_coverage")
    if not freshness_pass:
        reasons.append("snapshot_freshness_p95_exceeds_limit_or_unverifiable")

    t = trades.copy()
    if t.empty:
        reasons.append("no_closed_trades")
        closed = t.copy()
    else:
        if "status" not in t.columns:
            reasons.append("trade_status_missing")
            closed = t.iloc[0:0].copy()
        else:
            closed = t[t["status"].astype(str).str.upper() == "CLOSED"].copy()
            if len(closed) != len(t):
                reasons.append("ledger_contains_open_or_nonclosed_rows")

    if not closed.empty:
        for col in ("entry_time", "exit_time", "net_pnl_inr", "trade_key"):
            if col not in closed.columns:
                reasons.append("trade_column_missing:" + col)
        if "trade_key" in closed.columns and closed["trade_key"].duplicated().any():
            reasons.append("duplicate_trade_key")
        closed["entry_time"] = pd.to_datetime(closed.get("entry_time"), errors="coerce")
        closed["exit_time"] = pd.to_datetime(closed.get("exit_time"), errors="coerce")
        if closed[["entry_time", "exit_time"]].isna().any().any():
            reasons.append("invalid_trade_timestamp")
        else:
            if (closed["exit_time"] <= closed["entry_time"]).any():
                reasons.append("exit_not_after_entry")
            if session_date:
                cutoff_entry = _ts(f"{session_date} {entry_cutoff}")
                cutoff_exit = _ts(f"{session_date} {exit_cutoff}")
                if (closed["entry_time"].dt.strftime("%Y-%m-%d") != session_date).any():
                    reasons.append("entry_date_mismatch")
                if (closed["exit_time"].dt.strftime("%Y-%m-%d") != session_date).any():
                    reasons.append("exit_date_mismatch")
                if (closed["entry_time"] >= cutoff_entry).any():
                    reasons.append("entry_at_or_after_cutoff")
                if (closed["exit_time"] > cutoff_exit).any():
                    reasons.append("exit_after_cutoff")
            if "session_id" in closed.columns and session.get("session_id") is not None:
                if not closed["session_id"].eq(session["session_id"]).all():
                    reasons.append("trade_session_id_mismatch")

            # Sweep actual intervals. A position released at an exit timestamp
            # is released before a new entry at that same timestamp.
            events = []
            for row in closed.itertuples(index=False):
                events.append((row.exit_time, 0, 1))
                events.append((row.entry_time, 1, 1))
            events.sort(key=lambda e: (e[0], e[1]))
            active = 0
            peak_active = 0
            for _, event_type, _ in events:
                active += 1 if event_type == 1 else -1
                peak_active = max(peak_active, active)
            if peak_active > int(max_positions):
                reasons.append("portfolio_position_overlap")

    net_values = []
    if not closed.empty and "net_pnl_inr" in closed.columns:
        pnl = pd.to_numeric(closed["net_pnl_inr"], errors="coerce")
        if pnl.isna().any() or not pnl.map(math.isfinite).all():
            reasons.append("invalid_net_pnl")
        else:
            closed = closed.assign(_net_pnl=pnl).sort_values("exit_time")
            net_values = closed["_net_pnl"].astype(float).tolist()

    winners = [v for v in net_values if v > 0]
    losers = [v for v in net_values if v < 0]
    positive_sum = float(sum(winners))
    negative_sum = float(abs(sum(losers)))
    pf = (positive_sum / negative_sum) if negative_sum > 0 else (None if positive_sum == 0 else None)
    net_pnl = float(sum(net_values))
    max_dd = _drawdown(net_values)

    manifest_complete = not required_manifest_missing
    if _has_value(manifest.get("source_commit")) and not re.fullmatch(r"[0-9a-fA-F]{40}", str(manifest.get("source_commit"))):
        reasons.append("invalid_source_commit_hash")
        manifest_complete = False
    for hash_key in ("model_sha256", "config_sha256"):
        if _has_value(manifest.get(hash_key)) and not re.fullmatch(r"[0-9a-fA-F]{64}", str(manifest.get(hash_key))):
            reasons.append("invalid_" + hash_key)
            manifest_complete = False
    expected_fingerprint_keys = (
        "source_commit", "model_sha256", "config_sha256",
        "entry_rule_version", "exit_policy_id", "cost_model_id",
    )
    session_manifest = session.get("audit_manifest")
    if not isinstance(session_manifest, dict):
        session_config = session.get("config") or session.get("config_json") or {}
        if isinstance(session_config, str):
            try:
                import json
                session_config = json.loads(session_config)
            except (ValueError, TypeError):
                session_config = {}
        if not isinstance(session_config, dict):
            session_config = {}
        session_manifest = session_config.get("audit_manifest")
        if not isinstance(session_manifest, dict) and isinstance(session_config.get("config"), dict):
            session_manifest = session_config["config"].get("audit_manifest")
    if not isinstance(session_manifest, dict):
        reasons.append("session_audit_manifest_missing")
    else:
        if str(session_manifest.get("session_date") or "")[:10] != session_date:
            reasons.append("session_manifest_date_mismatch")
        for key in expected_fingerprint_keys:
            if session_manifest.get(key) != manifest.get(key):
                reasons.append("session_manifest_mismatch:" + key)

    if not closed.empty:
        if "metadata" not in closed.columns:
            reasons.append("trade_audit_manifest_missing")
        else:
            def _metadata_object(value):
                if isinstance(value, dict):
                    return value
                if isinstance(value, str):
                    try:
                        import json
                        decoded = json.loads(value)
                        return decoded if isinstance(decoded, dict) else {}
                    except (ValueError, TypeError):
                        return {}
                return {}
            for value in closed["metadata"]:
                metadata = _metadata_object(value)
                trade_manifest = metadata.get("audit_manifest")
                if not isinstance(trade_manifest, dict):
                    reasons.append("trade_audit_manifest_missing")
                    continue
                for key in expected_fingerprint_keys:
                    if trade_manifest.get(key) != manifest.get(key):
                        reasons.append("trade_manifest_mismatch:" + key)
                        break
                if metadata.get("v12_model_sha256") != manifest.get("model_sha256"):
                    reasons.append("trade_model_hash_mismatch")
                if metadata.get("v12_config_sha256") != manifest.get("config_sha256"):
                    reasons.append("trade_config_hash_mismatch")

    ledger_integrity = not any(
        r.startswith((
            "ledger_contains", "trade_column_missing", "duplicate_trade_key",
            "invalid_trade_timestamp", "exit_not_after_entry", "entry_at_or_after_cutoff",
            "exit_after_cutoff", "entry_date_mismatch", "exit_date_mismatch",
            "trade_session_id_mismatch", "snapshot_session_id_mismatch",
            "duplicate_snapshot_key", "portfolio_position_overlap", "invalid_net_pnl",
        ))
        for r in reasons
    ) and len(closed) > 0
    blind_test = bool(
        manifest_complete and frozen_before_start and full_session_coverage
        and ledger_integrity and session_date and freshness_pass and not reasons
        and str(manifest.get("source_commit") or "").strip()
        and str(manifest.get("model_sha256") or "").strip()
        and str(manifest.get("config_sha256") or "").strip()
    )

    return {
        "ok": True,
        "evidence_class": "LIVE_PAPER_PORTFOLIO_LEDGER_AUDIT" if blind_test else "DIAGNOSTIC_NOT_BLIND_PROOF",
        "blind_test": blind_test,
        "blind_test_eligible": blind_test,
        "session_id": session.get("session_id"),
        "session_date": session_date,
        "snapshot_count": snapshot_count,
        "first_snapshot": None if first_snapshot is None else str(first_snapshot),
        "last_snapshot": None if last_snapshot is None else str(last_snapshot),
        "full_session_coverage": full_session_coverage,
        "snapshot_gap_p95_seconds": None if snapshot_gap_p95_seconds is None else round(snapshot_gap_p95_seconds, 3),
        "snapshot_gap_max_seconds": None if snapshot_gap_max_seconds is None else round(snapshot_gap_max_seconds, 3),
        "freshness_pass": freshness_pass,
        "max_symbol_gap_p95_seconds": float(max_symbol_gap_p95_seconds),
        "frozen_before_session": frozen_before_start,
        "manifest_complete": manifest_complete,
        "ledger_integrity": ledger_integrity,
        "trade_count": int(len(closed)),
        "wins": int(len(winners)),
        "losses": int(len(losers)),
        "flat_trades": int(sum(1 for v in net_values if v == 0)),
        "win_rate": None if not net_values else float(len(winners) / len(net_values)),
        "positive_pnl_inr": round(positive_sum, 2),
        "negative_pnl_inr": round(negative_sum, 2),
        "net_pnl_inr": round(net_pnl, 2),
        "profit_factor": None if pf is None else round(pf, 6),
        "max_drawdown_inr": round(max_dd, 2),
        "exit_reason_counts": {} if closed.empty or "exit_reason" not in closed.columns
            else {str(k): int(v) for k, v in closed["exit_reason"].value_counts(dropna=False).items()},
        "reasons": sorted(set(reasons)),
        "limitations": [
            "This audits the actual stored paper-trade ledger; it does not estimate unseen counterfactual trades.",
            "A blind label is valid only if the manifest was frozen before the session and the ledger/snapshot coverage checks pass.",
            "PF is computed from realized net_pnl_inr after the worker's recorded cost model; any unrecorded slippage remains a limitation.",
        ],
    }
