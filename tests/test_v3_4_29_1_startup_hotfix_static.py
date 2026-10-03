from pathlib import Path
import ast
import re

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / 'app.py'
HTML = ROOT / 'templates' / 'index.html'

app_text = APP.read_text(encoding='utf-8')
html_text = HTML.read_text(encoding='utf-8')


def test_app_parses_after_startup_hotfix():
    ast.parse(app_text)


def test_database_backed_api_fails_fast_until_schema_ready():
    assert "if not _DB_READY.is_set():" in app_text
    assert "'code': 'database_initializing'" in app_text
    assert "resp.headers['Retry-After'] = '2'" in app_text


def test_frontend_has_bounded_fetch_helper_and_intraday_retry():
    assert 'async function fetchJsonWithTimeout' in html_text
    assert "fetchJsonWithTimeout('/api/intraday'" in html_text
    assert 'intradayLoadInFlight' in html_text
    assert "e.code === 'database_initializing' || e.status === 503" in html_text


def test_position_guard_waits_for_db_ready():
    assert "if (!dbWasReady) return;" in html_text
    assert "fetchJsonWithTimeout('/api/position-guard/status'" in html_text


def test_new_backend_route_guard_has_status_exceptions():
    guard = app_text[app_text.index("@app.before_request"):app_text.index("def iter_chunks")]
    for token in ("request.path == '/healthz'", "request.path == '/api/system-status'", "request.path == '/api/db-status'", "request.path == '/api/market-status'"):
        assert token in guard
