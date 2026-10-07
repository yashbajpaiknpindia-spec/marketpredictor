from pathlib import Path
import ast

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / "app.py"
HTML = ROOT / "templates" / "index.html"
APP_TEXT = APP.read_text(encoding="utf-8")
HTML_TEXT = HTML.read_text(encoding="utf-8")


def test_app_parses_with_bootstrap_observability():
    ast.parse(APP_TEXT)


def test_single_bootstrap_status_contract_exists():
    assert "@app.get('/api/db-bootstrap-status')" in APP_TEXT
    assert "def _db_bootstrap_payload()" in APP_TEXT
    assert "'bootstrap_contract': 'db-bootstrap-v1'" in APP_TEXT


def test_explicit_bootstrap_signals_exist():
    for token in ("'READY'", "'MIGRATING'", "'STALLED'", "'ERROR'", "'CONNECTING'", "'DISABLED'"):
        assert token in APP_TEXT
    assert "'current_step': _DB_SCHEMA_CURRENT_STEP" in APP_TEXT
    assert "'schema_elapsed_seconds'" in APP_TEXT
    assert "'last_error': _DB_SCHEMA_LAST_ERROR" in APP_TEXT


def test_readiness_endpoint_is_distinct_from_liveness():
    assert "@app.get('/readyz')" in APP_TEXT
    assert "status = 200 if payload['db_ready'] else 503" in APP_TEXT


def test_bootstrap_endpoints_bypass_api_schema_guard():
    guard = APP_TEXT[APP_TEXT.index('@app.before_request'):APP_TEXT.index('def iter_chunks')]
    assert "request.path == '/readyz'" in guard
    assert "request.path == '/api/db-bootstrap-status'" in guard


def test_frontend_surfaces_bootstrap_signal_and_report():
    assert "window.__lastDbBootstrapSignal" in HTML_TEXT
    assert "'[DB_BOOTSTRAP]'" in HTML_TEXT
    assert "/api/db-bootstrap-status" in HTML_TEXT
