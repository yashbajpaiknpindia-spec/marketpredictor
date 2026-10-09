
from pathlib import Path
import sys
import pandas as pd
import numpy as np
import pytest

ROOT = Path(__file__).resolve().parents[1]
SCALPER = ROOT / "research" / "scalper"
sys.path.insert(0, str(ROOT))

from research.scalper.v12_calibration import (fit_v12_calibration, apply_v12_calibration, validate_v12_calibration_profile)
from research.scalper.ultra_scalper_engine import ScalperConfig, score_event


def _fixture(n=240):
    t=pd.date_range("2026-10-07 13:56:20", periods=n, freq="s")
    # deterministic score/outcome relationship with enough samples per bin
    px=100+np.cumsum(np.where(np.arange(n)%3==0,0.02,-0.005))
    return pd.DataFrame({
        "ticker":["AAA"]*n,
        "captured_at":t,
        "ltp":px,
        "raw_confidence":50+40*(np.arange(n)%20)/19,
        "signal_confidence":50+40*(np.arange(n)%20)/19,
        "ofi_proxy":0.0,"imbalance_l5":0.0,"microprice_edge_pct":0.0,
        "signal_direction":np.where(np.arange(n)%2,1,-1),
        "edge_pass":True,"score_pass":True,"l2_pass":True,
    })


def test_profile_is_train_only_and_outputs_two_probabilities():
    x=_fixture()
    p=fit_v12_calibration(x.iloc[:160], horizon_seconds=30, bins=8)
    assert 100 <= p.train_rows < 160
    assert p.version == "v12-calibration-v2-first-barrier"
    assert p.target_gross_pct == 0.1363 + 0.015 + 0.4487
    assert len(p.target_hit_rates)==len(p.adverse_rates)
    a,b=apply_v12_calibration([55,75],p.to_dict())
    assert np.all((a>=0)&(a<=1))
    assert np.all((b>=0)&(b<=1))


def test_engine_never_treats_raw_confidence_as_probability():
    cfg=ScalperConfig(calibration_profile=None, require_calibrated_probability=True)
    row=pd.Series({
        "ret_1":1,"ret_3":1,"ret_5":1,"range_pct":1,"body_pct":1,"close_location":1,
        "vol_ratio_20":2,"range_ratio_20":2,"rs_1":1,"rs_3":1,"market_ret_1":1,"market_ret_3":1,
    })
    r=score_event(row,cfg)
    assert r["confidence_semantics"]=="model_score_0_100_not_probability"
    assert r["calibration_status"]=="missing_profile"
    assert r["direction"]==0


def test_profile_is_bound_to_execution_policy():
    x = _fixture()
    p = fit_v12_calibration(x.iloc[:160], horizon_seconds=30, bins=8).to_dict()
    ok, reason = validate_v12_calibration_profile(
        p, horizon_seconds=30, round_trip_cost_pct=0.1363,
        entry_slippage_pct=0.015, target_gross_pct=0.60,
        protection_pct=0.18,
    )
    assert ok and reason == "compatible"
    ok, reason = validate_v12_calibration_profile(
        p, horizon_seconds=30, round_trip_cost_pct=0.1363,
        entry_slippage_pct=0.015, target_gross_pct=0.1413,
        protection_pct=0.18,
    )
    assert not ok and reason == "mismatched_target_gross_pct"
    ok, reason = validate_v12_calibration_profile(
        p, horizon_seconds=30, round_trip_cost_pct=0.1363,
        entry_slippage_pct=0.015, target_gross_pct=0.60,
        protection_pct=0.18, exit_policy_id="live_v12_profit_lock_l5_flip_v1",
    )
    assert not ok and reason == "mismatched_exit_policy_id"


def test_legacy_profile_is_rejected_instead_of_mislabelled():
    p = {
        "version": "v12-calibration-v1",
        "horizon_seconds": 300,
        "round_trip_cost_pct": 0.1363,
        "protection_pct": 0.22,
        "score_edges": [0, 1, 2],
        "positive_rates": [0.8, 0.9],
        "adverse_rates": [0.1, 0.1],
    }
    ok, reason = validate_v12_calibration_profile(
        p, horizon_seconds=300, round_trip_cost_pct=0.1363,
        entry_slippage_pct=0.015, target_gross_pct=0.60,
        protection_pct=0.18,
    )
    assert not ok and reason == "unsupported_profile_version"


def test_future_labels_do_not_cross_calendar_sessions():
    from research.scalper.v12_calibration import _future_outcomes
    x = pd.DataFrame({
        "ticker": ["AAA", "AAA"],
        "session_id": [1, 2],
        "captured_at": pd.to_datetime(["2026-10-07 15:29:50", "2026-10-08 09:15:00"]),
        "ltp": [100.0, 101.0],
        "signal_direction": [1, 1],
    })
    terminal, target_first, adverse_first = _future_outcomes(
        x, horizon_seconds=30, target_gross_pct=0.1, protection_pct=0.1,
    )
    assert np.isnan(terminal).all()
    assert np.isnan(target_first).all()
    assert np.isnan(adverse_first).all()


def test_incompatible_legacy_profile_cannot_fall_back_to_heuristic():
    cfg = ScalperConfig(
        calibration_profile={
            "version": "v12-calibration-v1",
            "horizon_seconds": 300,
            "round_trip_cost_pct": 0.1363,
            "protection_pct": 0.22,
            "score_edges": [0, 1, 2],
            "positive_rates": [0.8, 0.9],
            "adverse_rates": [0.1, 0.1],
        },
        require_calibrated_probability=False,
    )
    row = pd.Series({
        "ret_1": 1, "ret_3": 1, "ret_5": 1, "range_pct": 1,
        "body_pct": 1, "close_location": 1, "vol_ratio_20": 2,
        "range_ratio_20": 2, "rs_1": 1, "rs_3": 1,
        "market_ret_1": 1, "market_ret_3": 1,
    })
    result = score_event(row, cfg)
    assert result["calibration_status"].startswith("incompatible_profile:")
    assert result["direction"] == 0


def test_fixed_barrier_fitter_refuses_live_exit_policy_label():
    x = _fixture()
    with pytest.raises(ValueError, match="matching simulator"):
        fit_v12_calibration(
            x.iloc[:160], horizon_seconds=30, bins=8,
            exit_policy_id="live_v12_profit_lock_l5_flip_v1",
        )
