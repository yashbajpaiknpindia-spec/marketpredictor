from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_l5_replay_endpoints_and_ui_exist():
    app = (ROOT / 'app.py').read_text()
    ui = (ROOT / 'templates' / 'index.html').read_text()
    mod = (ROOT / 'research' / 'scalper' / 'l5_replay_lab.py').read_text()
    assert "/api/scalper/l5/calibration" in app
    assert "/api/scalper/l5/replay/<int:session_id>" in app
    assert 'scL5ReplayLab' in ui
    assert 'runScalperL5Replay' in ui
    assert 'runScalperL5Calibration' in ui
    assert 'TRUE_L5_REPLAY' in mod
    assert 'EMPIRICAL_L5_PROXY' in mod


def test_capital_safe_default_and_cadence_guard():
    worker = (ROOT / 'research' / 'scalper' / 'scalper_live_paper.py').read_text()
    assert 'paper_capital_inr' in worker
    assert 'capital_limit' in worker
    assert 'interval - work_seconds' in worker
