from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LIVE = (ROOT / 'research' / 'scalper' / 'scalper_live_paper.py').read_text()
REPLAY = (ROOT / 'research' / 'scalper' / 'l5_replay_lab.py').read_text()


def test_live_trail_arms_on_first_net_positive_move():
    assert 'trail_arm_net = 0.0' in LIVE
    assert 'if net_now > trail_arm_net:' in LIVE
    assert 'if net_now >= lock_net:' not in LIVE


def test_replay_trail_arms_on_first_net_positive_move():
    assert 'if net_now > 0.0:' in REPLAY
    assert 'if net_now >= economic_lock_net_pct:' not in REPLAY
