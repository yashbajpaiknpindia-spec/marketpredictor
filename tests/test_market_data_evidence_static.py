from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / 'app.py').read_text(encoding='utf-8')
HTML = (ROOT / 'templates' / 'market_data_evidence.html').read_text(encoding='utf-8')
INDEX = (ROOT / 'templates' / 'index.html').read_text(encoding='utf-8')


def test_market_data_library_has_period_controls_and_strict_real_gate():
    assert "'/api/market-data/plan'" in APP
    assert "'/api/market-data/start'" in APP
    assert "year_" in APP
    assert "rapid_requires_100pct':True" in APP
    assert "status='completed' if all_complete" in APP
    assert "provider_fetch_allowed':False" in APP


def test_market_data_records_symbol_resolution_and_coverage_diagnostics():
    assert 'market_data_symbol_map' in APP
    assert 'market_data_coverage' in APP
    assert 'SYMBOL_UNRESOLVED' in APP
    assert 'RESOLVED_BY_ISIN' in APP
    assert 'issue_counts' in APP
    assert 'checksum' in APP


def test_market_data_library_is_exposed_and_rapid_uses_same_dataset():
    assert 'Strategy Evidence' in INDEX
    assert "window.location.href='/market-data'" in INDEX
    assert "'/api/market-data/jobs/<int:job_id>/rapid-scan'" in APP
    assert 'expected_dataset_fingerprint' in APP
    assert 'Raw Rapid' in INDEX


def test_evidence_page_captures_context_and_path_exit_metrics():
    for label in ('Evidence registry','Regime','Volatility','Time','Signals','Win %','PF','MFE / MAE','Profit→Loss','Stop→No Recover'):
        assert label in HTML
    assert 'path_exit_signals' in APP
    assert 'PATH_AWARE_SHADOW_V1' in APP
    assert 'profit_reached_but_lost_pct' in APP
    assert 'stop_first_never_recovered_pct' in APP


def test_rapid_ui_contains_no_synthetic_mode():
    start=INDEX.find('id="rapidForensicsView"')
    end=INDEX.find('id="historyView"')
    rapid=INDEX[start:end] if start>=0 and end>start else INDEX
    assert 'SYNTHETIC' not in rapid
    assert 'synthetic market' not in rapid.lower()
    assert "data_mode:'REAL',cache_only:true" in INDEX


def test_evidence_report_download_endpoints_and_ui_controls_exist():
    assert "@app.route('/api/evidence/report', methods=['GET'])" in APP
    assert "format=html" in APP and "format=zip" in APP
    assert 'decision_audit_summary.csv' in APP
    assert 'decision_audit_trades.csv' in APP
    assert 'strategy_evidence.csv' in APP
    assert '⬇️ Download evidence report' in HTML
    assert 'downloadSelectedReport' in HTML
    assert 'marketpredictor_evidence_snapshot_' in APP


def test_replay_trade_schema_migrates_side_for_decision_audit():
    assert "ALTER TABLE intraday_replay_trades ADD COLUMN IF NOT EXISTS side VARCHAR(10)" in APP
