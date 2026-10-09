from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LIVE = (ROOT / 'research' / 'scalper' / 'scalper_live_paper.py').read_text()
REPLAY = (ROOT / 'research' / 'scalper' / 'l5_replay_lab.py').read_text()


def test_live_trail_arm_threshold_is_explicit_and_configurable():
    assert 'trail_arm_net = float(settings.get("v12_profit_lock_arm_net_pct", 0.075))' in LIVE
    assert 'if net_now >= trail_arm_net:' in LIVE
    assert 'if net_now >= lock_net:' not in LIVE


def test_replay_can_evaluate_immediate_trail_as_a_separate_variant():
    assert 'if net_now > 0.0:' in REPLAY
    assert 'if net_now >= economic_lock_net_pct:' not in REPLAY
