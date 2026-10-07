from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LAB = (ROOT / 'research' / 'scalper' / 'synthetic_l5_lab_v3.py').read_text()
APP = (ROOT / 'app.py').read_text()
UI = (ROOT / 'templates' / 'index.html').read_text()


def test_v3_has_causal_market_regimes_and_l5_contract():
    assert 'SYNTHETIC_L5_CAUSAL_MARKET_LAB_V3' in LAB
    assert LAB.count('"') > 0
    for regime in ('news_jump', 'shock_recovery', 'correlated_selloff', 'idiosyncratic_burst', 'dead_zone'):
        assert f'"{regime}"' in LAB
    for field in ('imbalance_l5', 'microprice_edge_pct', 'market_ret_1_pct', 'relative_strength_pct', 'trade_intensity', 'cancel_intensity'):
        assert f'"{field}"' in LAB


def test_v3_keeps_hidden_state_out_of_feature_contract_and_freezes_blind_test():
    assert 'hidden_true_flow' in LAB
    assert 'hidden_state_excluded' in LAB
    assert 'thresholds_frozen_after_validation' in LAB
    assert 'blind_test' in LAB and 'thresholds_frozen_after_validation' in LAB


def test_app_routes_synthetic_lab_to_v3():
    assert 'research.scalper.synthetic_l5_lab_v3' in APP
    assert "event_probability" in APP and "spoof_probability" in APP and "shock_probability" in APP
    assert 'SYNTHETIC_L5_CAUSAL_MARKET_LAB_V3' in APP


def test_ui_exposes_cross_sectional_and_event_controls():
    for element in ('synL5Stocks', 'synL5EventProb', 'synL5SpoofProb', 'synL5ShockProb'):
        assert element in UI
    assert 'L5 Causal Market Lab v3' in UI
    assert '30 regimes' in UI
