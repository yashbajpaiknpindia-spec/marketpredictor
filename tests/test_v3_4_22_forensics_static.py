from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / 'app.py').read_text(encoding='utf-8')
HTML = (ROOT / 'templates' / 'index.html').read_text(encoding='utf-8')
BACKUP = (ROOT / 'app_data_backup.py').read_text(encoding='utf-8')


def test_trade_schema_has_money_and_management_telemetry():
    required = [
        'invested_value', 'exit_value', 'holding_minutes',
        'profit_protection_enabled', 'profit_protection_armed',
        'trailing_stop_activated', 'trailing_stop_price_at_exit',
        'management_state', 'management_reason', 'management_version'
    ]
    for name in required:
        assert f'ADD COLUMN IF NOT EXISTS {name}' in APP
        assert name in APP[APP.find('def _replay_insert_trade'):APP.find('def _replay_insert_capital_snapshot')]


def test_trade_insert_columns_match_placeholders():
    start = APP.find('def _replay_insert_trade')
    end = APP.find('def _replay_insert_capital_snapshot', start)
    block = APP[start:end]
    mcols = re.search(r'\((run_id, ticker.*?management_version)\)', block, re.S)
    mvals = re.search(r'VALUES \((.*?)\)', block, re.S)
    assert mcols and mvals
    assert len(mcols.group(1).split(',')) == len(mvals.group(1).split(','))
    assert len(mvals.group(1).split(',')) == 55


def test_forensics_and_strategy_evidence_routes_exist():
    assert "@app.route('/api/intraday-replay/<int:run_id>/trades'" in APP
    assert "@app.route('/api/intraday-replay/strategy-evidence/<path:strategy_id>'" in APP
    assert '_replay_logical_group_ids' in APP


def test_replay_ui_has_separate_pages_and_pagination():
    for marker in ('irPageOverview', 'irPageTrades', 'irPageEvidence', 'irPageRuns'):
        assert f'id="{marker}"' in HTML
    assert 'irLoadForensics' in HTML
    assert 'irForensicsPage' in HTML
    assert 'irLoadStrategyEvidence' in HTML
    assert 't.invested_value' in HTML and 't.exit_value' in HTML and 't.net_pnl' in HTML


def test_full_export_ui_uses_async_backup():
    assert "startFullAppDataBackup()" in HTML
    assert 'Download UNCAPPED ZIP' not in HTML
    assert 'window.location.href = `/api/export/all' not in HTML
    # PostgreSQL COPY SQL must be rendered with the active cursor object.
    assert 'copy_sql.as_string(cur)' in BACKUP
    assert 'copy_sql.as_string(conn)' not in BACKUP


def test_legacy_management_is_explicitly_unknown():
    assert "UNKNOWN_LEGACY" in APP
    assert 'Replay predates per-trade management telemetry.' in APP


def test_status_label_does_not_claim_auto_bench_for_generic_negative_results():
    assert "poor:      { icon: '🔴', label: 'Negative measured performance' }" in HTML
