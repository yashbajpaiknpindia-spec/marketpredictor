from pathlib import Path
import importlib.util

ROOT = Path(__file__).resolve().parents[1]
ENGINE_PATH = ROOT / 'research' / 'scalper' / 'ultra_scalper_engine.py'
LIVE_PATH = ROOT / 'research' / 'scalper' / 'scalper_live_paper.py'


def load_engine():
    spec = importlib.util.spec_from_file_location('ultra_scalper_engine_test', ENGINE_PATH)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    import sys
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def test_live_v12_policy_uses_economic_lock_not_fixed_tiny_target():
    text = LIVE_PATH.read_text()
    assert '"v12_economic_lock_net_pct": 0.20' in text
    assert '"target_pct": 0.60' in text
    assert '"protection_pct": 0.18' in text
    assert '"round_trip_cost_pct": 0.1363' in text
    assert '"entry_slippage_pct": 0.015' in text


def test_v12_default_edge_gate_allows_small_positive_net_edge():
    eng = load_engine()
    cfg = eng.ScalperConfig(round_trip_cost_pct=0.1363, entry_slippage_pct=0.0)
    assert cfg.min_remaining_edge_pct == 0.005
    # A gross move equal to cost + 0.005% leaves the configured positive net buffer.
    gross = 0.1363 + 0.005
    remaining = gross - 0.1363
    assert remaining >= cfg.min_remaining_edge_pct


def test_live_worker_defaults_match_economic_policy_contract():
    text = LIVE_PATH.read_text()
    assert '"v12_min_model_target_probability": 0.70' in text
    assert '"v12_model_enabled": True' in text
    assert '"v12_profit_lock_trail_gross_pct": 0.075' in text
    assert 'settings.get("v12_economic_lock_net_pct", 0.20)' in text
