from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / 'app.py').read_text()
HTML = (ROOT / 'templates' / 'index.html').read_text()


def test_extended_replay_child_opts_into_extended_lookback():
    assert 'allow_extended_lookback: bool = False' in APP
    assert 'lookback_days = INTRADAY_REPLAY_EXTENDED_LOOKBACK_DAYS if allow_extended_lookback else INTRADAY_REPLAY_MAX_LOOKBACK_DAYS' in APP
    assert 'allow_extended_lookback=True' in APP
    # Regression: the exact first 15-day child of the failed Jul→Sep replay
    # must be valid only when the extended lookback is explicitly enabled.


def test_db_bootstrap_diagnostics_removed_from_top_and_collapsed_in_export():
    top = HTML.split('<div class="main">', 1)[0]
    assert '<div aria-live="polite" class="db-notice" id="dbNotice"' not in top
    assert '<div class="db-bootstrap-panel" id="dbBootstrapPanel"' not in top
    export_pos = HTML.index('<div id="exportView"')
    details_pos = HTML.index('<details class="db-bootstrap-details" id="dbBootstrapDetails">')
    assert details_pos > export_pos
    assert "summary::after{content:'Show'" in HTML
    assert "db-bootstrap-details[open] summary::after{content:'Hide'" in HTML


def test_replay_interval_default_is_one_minute():
    assert "INTRADAY_DATA_INTERVAL = os.environ.get('INTRADAY_DATA_INTERVAL', '1m')" in APP
