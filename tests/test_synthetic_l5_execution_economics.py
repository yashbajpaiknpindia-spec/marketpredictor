from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from research.scalper.synthetic_l5_lab_v3 import (  # noqa: E402
    SyntheticConfig,
    _aggregate_metrics,
    _execution_pnl_pct,
)


def test_execution_net_charges_both_observed_half_spreads_and_round_trip_friction():
    cfg = SyntheticConfig(
        round_trip_cost_pct=0.1363,
        entry_slippage_pct=0.015,
        adverse_selection_pct=0.020,
    )
    gross, net = _execution_pnl_pct(100.0, 101.0, 1, 0.10, 0.20, cfg)
    # 1.00 gross - 0.1363 fee - 0.150 spread - 0.015 slippage - 0.020 adverse selection.
    assert abs(gross - 1.0) < 1e-9
    assert abs(net - 0.6787) < 1e-9


def test_execution_pnl_is_symmetric_for_a_short_in_a_down_move():
    cfg = SyntheticConfig(
        round_trip_cost_pct=0.1363,
        entry_slippage_pct=0.015,
        adverse_selection_pct=0.020,
    )
    gross, net = _execution_pnl_pct(100.0, 99.0, -1, 0.10, 0.20, cfg)
    assert abs(gross - 1.0) < 1e-9
    assert abs(net - 0.6787) < 1e-9


def test_pooled_profit_factor_uses_combined_winners_and_losers_not_average_stream_pfs():
    stream_a = {
        "trades": 2, "win_rate_pct": 50.0, "avg_net_pct": 0.25,
        "sum_net_pct": 0.5, "net_profit_pct_points": 1.0,
        "net_loss_pct_points": 0.5, "profit_factor": 2.0,
    }
    stream_b = {
        "trades": 2, "win_rate_pct": 50.0, "avg_net_pct": -0.95,
        "sum_net_pct": -1.9, "net_profit_pct_points": 0.1,
        "net_loss_pct_points": 2.0, "profit_factor": 0.05,
    }
    combined = _aggregate_metrics([stream_a, stream_b])
    assert abs(combined["profit_factor"] - (1.1 / 2.5)) < 1e-9
    assert abs(combined["profit_factor"] - ((2.0 + 0.05) / 2.0)) > 0.1
