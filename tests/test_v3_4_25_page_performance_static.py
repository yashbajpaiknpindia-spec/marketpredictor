from pathlib import Path
from collections import Counter

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / 'app.py').read_text(encoding='utf-8')
HTML = (ROOT / 'templates' / 'index.html').read_text(encoding='utf-8')


def test_strategy_dashboard_is_a_separate_page():
    assert 'id="tabStrategyDashboard"' in HTML
    assert 'id="strategyDashboardView"' in HTML
    assert 'id="strategyDashboardCard"' in HTML
    assert "if (tab === 'strategyDashboard')" in HTML
    # Dashboard should not be auto-loaded with the heavy intraday engine page.
    assert "loadStrategyDashboard()).catch(e => showPageLoaderError('intradayView'" not in HTML


def test_trade_history_fast_path_does_not_force_exit_reconciliation():
    assert "refresh_exits = str(request.args.get('refresh_exits','0')).lower()" in APP
    assert 'def get_trade_history_dashboard(' in APP and 'refresh_exits: bool = False' in APP
    block = APP[APP.index("@app.route('/api/trade-history', methods=['GET'])"):APP.index("@app.route('/api/trade-history', methods=['DELETE'])")]
    assert 'refresh_exits=refresh_exits' in block
    assert "loadTradeHistory(false)" in HTML
    assert "loadTradeHistory(true)" in HTML


def test_export_manifest_has_lightweight_and_summary_paths():
    block = APP[APP.index("@app.route('/api/export/manifest', methods=['GET'])"):APP.index("@app.route('/api/export/all', methods=['GET'])")]
    assert "request.args.get('summary','0')" in block
    assert 'summaries = _fetch_export_summaries(cur) if include_summary else {}' in block
    assert "fetch('/api/export/manifest?summary=0')" in HTML
    assert "fetch('/api/export/manifest?summary=1')" in HTML


def test_calendar_is_all_month_inventory_and_preserves_light_green_export_state():
    assert 'inventory_daily_coverage' in APP
    assert 'id="irCoverageCalendar"' in HTML
    assert 'replay-coverage-months' in HTML
    assert 'light green = Export Data stored locally' in HTML
    assert 'Export Data · local reuse' in HTML
    assert '1m→5m' in HTML
