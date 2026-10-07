from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / 'app.py').read_text(encoding='utf-8')
WORKER = (ROOT / 'research' / 'scalper' / 'scalper_live_paper.py').read_text(encoding='utf-8')
HTML = (ROOT / 'templates' / 'index.html').read_text(encoding='utf-8')


def test_live_diagnostics_are_persisted_and_exposed():
    for field in ('raw_direction_count','edge_pass_count','score_pass_count','l2_agreement_count','entry_reject_count'):
        assert field in APP
        assert field in WORKER
    assert 'id="scLiveRawCandidates"' in HTML
    assert 'id="scLiveFinalSignals"' in HTML


def test_snapshot_gate_diagnostics_are_stored_and_exported():
    for field in ('raw_direction','raw_confidence','edge_pass','score_pass','l2_pass','rejection_reason'):
        assert field in APP
        assert field in WORKER
    assert 'rejection_reason' in HTML or 'api/scalper/live/export/snapshots.csv' in HTML


def test_scalper_exports_are_present():
    assert "@app.get('/api/scalper/live/export/snapshots.csv')" in APP
    assert "@app.get('/api/scalper/live/export/trades.csv')" in APP
    assert "@app.get('/api/scalper/live/export/session.json')" in APP
    assert 'Export full 5-level snapshots' in HTML
    assert 'Export paper trades CSV' in HTML
    assert 'Export latest session JSON' in HTML


def test_engine_returns_gate_diagnostics():
    sys.path.insert(0, str(ROOT))
    from research.scalper.ultra_scalper_engine import ScalperConfig, score_event
    import pandas as pd
    row = pd.Series({
        'ret_1': 0.10, 'ret_3': 0.40, 'ret_5': 0.50, 'range_pct': 0.20, 'body_pct': 0.08,
        'close_location': 0.9, 'vol_ratio_20': 2.0, 'range_ratio_20': 1.8, 'vwap_distance_pct': 0.1,
        'rs_1': 0.12, 'rs_3': 0.30, 'market_ret_1': 0.05, 'market_ret_3': 0.10,
        'ofi': 0.2, 'imbalance_l5': 0.3, 'microprice_edge': 0.02,
    })
    result = score_event(row, ScalperConfig())
    for key in ('raw_direction','raw_confidence','edge_pass','score_pass','l2_pass','rejection_reason','micro_signal'):
        assert key in result
