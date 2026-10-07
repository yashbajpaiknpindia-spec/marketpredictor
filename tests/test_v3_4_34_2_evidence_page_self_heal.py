from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / 'app.py').read_text(encoding='utf-8')


def test_evidence_page_self_heals_legacy_schema_before_reads():
    assert 'def _ensure_evidence_page_schema()' in APP
    assert 'ALTER TABLE strategy_evidence_snapshots ADD COLUMN IF NOT EXISTS dataset_fingerprint' in APP
    helper=APP[APP.index('def _ensure_evidence_page_schema()'):APP.index('# ---------------------------------------------------------------------------\n# UNIFIED REAL MARKET-DATA LIBRARY', APP.index('def _ensure_evidence_page_schema()'))]
    assert "'side':\"VARCHAR(20) NOT NULL DEFAULT 'ALL'\"" in helper
    assert "ALTER TABLE intraday_replay_trades ADD COLUMN IF NOT EXISTS side" in APP
    assert 'information_schema.tables' in APP


def test_evidence_apis_call_self_heal():
    for fn in ('evidence_snapshots_endpoint()', 'evidence_snapshot_detail_endpoint(snapshot_id:int):',
               'evidence_active_endpoint()', 'evidence_activate_endpoint(snapshot_id:int):',
               'evidence_report_endpoint()', 'evidence_decision_audit_endpoint()'):
        start = APP.index('def ' + fn)
        snippet = APP[start:start + 250]
        assert '_ensure_evidence_page_schema()' in snippet


def test_decision_audit_gracefully_returns_empty_when_replay_tables_absent():
    start = APP.index('def evidence_decision_audit_endpoint():')
    end = APP.index("@app.route('/api/intraday-replay/start'", start)
    block = APP[start:end]
    assert "intraday_replay_trades','intraday_replay_runs" in block
    assert "'trade_count':0" in block
