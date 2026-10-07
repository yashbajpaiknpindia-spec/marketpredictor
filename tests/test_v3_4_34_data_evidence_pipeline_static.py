from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / 'app.py').read_text(encoding='utf-8')
INDEX = (ROOT / 'templates' / 'index.html').read_text(encoding='utf-8')
EVID = (ROOT / 'templates' / 'market_data_evidence.html').read_text(encoding='utf-8')


def test_market_data_acquisition_controls_live_on_export_data_page():
    assert 'id="canonicalMarketDataLibraryCard"' in INDEX
    assert 'id="mdPeriod"' in INDEX
    assert 'Calendar year' in INDEX and '6 months' in INDEX
    assert 'id="mdStartBtn"' in INDEX
    assert "'/api/market-data/start'" in INDEX
    assert 'Verified cells are reused' in INDEX or 'Existing verified cells will be reused' in INDEX


def test_strategy_evidence_page_has_no_acquisition_controls():
    assert 'Strategy Evidence & Decision Audit' in EVID
    assert '/api/evidence/snapshots' in EVID
    assert '/api/evidence/decision-audit' in EVID
    assert '/api/market-data/start' not in EVID
    assert 'Download / repair gaps' not in EVID


def test_raw_dataset_rapid_path_is_exact_and_does_not_acquire_data():
    start = APP.index("@app.route('/api/market-data/jobs/<int:job_id>/rapid-scan', methods=['POST'])")
    end = APP.index("@app.route('/api/evidence/snapshots', methods=['GET'])", start)
    block = APP[start:end]
    assert "_market_data_audit_matrix" in block
    assert "expected_dataset_fingerprint=job.get('dataset_fingerprint')" in block
    assert "data_mode='REAL'" in block
    assert 'raw_dataset_run' in block
    assert 'strategy_mutation' in block


def test_strong_evidence_requires_positive_edge_not_only_sample_size():
    start = APP.index('def _rapid_evidence_gate(result: Dict[str,Any])')
    end = APP.index('def _persist_rapid_evidence_snapshot', start)
    block = APP[start:end]
    assert 'system_avg_net_pct' in block
    assert 'system_profit_factor' in block
    assert 'positive_context_cells' in block
    assert "strong_ok=minimum_ok" in block
    assert "positive_edge" in block
    assert "120 strong" in EVID


def test_decision_audit_has_expected_vs_actual_and_management_telemetry():
    assert 'actual_profit_factor' in APP
    assert 'pf_drift' in APP
    assert 'smart_diagnosis' in APP
    assert 'profit_protection_armed' in APP
    assert 'trailing_stop_activated' in APP
    assert 'Next blind experiment' in EVID
    assert 'Management' in EVID
