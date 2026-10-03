from pathlib import Path
import ast
import datetime
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
APP_TEXT = (ROOT / 'app.py').read_text(encoding='utf-8')


def _extract(name):
    tree = ast.parse(APP_TEXT)
    for n in tree.body:
        if isinstance(n, ast.FunctionDef) and n.name == name:
            return n
    raise AssertionError(name)


def _payload_env():
    env = {
        'Dict': dict,
        'Any': object,
        'datetime': datetime,
        'threading': threading,
        '_DB_READY': threading.Event(),
        '_DB_STATUS': 'connected',
        '_DB_PHASE': 'schema_pending',
        '_DB_SCHEMA_LAST_ATTEMPT_AT': None,
        '_DB_SCHEMA_LAST_PROGRESS_AT': None,
        '_DB_SCHEMA_WAITING_FOR_MIGRATOR': False,
        '_DB_SCHEMA_LOCK_HELD': False,
        '_DB_SCHEMA_MODE': None,
        '_DB_SCHEMA_ATTEMPTS': 0,
        '_DB_SCHEMA_TERMINAL_ERROR': False,
        '_DB_SCHEMA_ERROR': None,
        '_DB_SCHEMA_CURRENT_STEP': 0,
        '_DB_SCHEMA_CURRENT_SQL': None,
        '_DB_SCHEMA_STEP_STARTED_AT': None,
        '_DB_SCHEMA_ERROR_AT': None,
        '_DB_SCHEMA_LAST_ERROR_STEP': None,
        '_DB_SCHEMA_LAST_ERROR': None,
        '_DB_CONNECT_MS': 12.0,
        '_DB_SCHEMA_MS': None,
        '_DB_CONNECTED': threading.Event(),
        '_DB_CONNECTED_AT': None,
        '_DB_READY_AT': None,
        '_DB_BOOTSTRAP_THREAD': None,
        '_DB_BOOTSTRAP_START_COUNT': 0,
        '_DB_BOOTSTRAP_STARTED_AT': None,
        '_DB_BOOTSTRAP_LAST_EXIT_AT': None,
        '_DB_BOOTSTRAP_LAST_CRASH': None,
        '_DB_BOOTSTRAP_START_STALL_SECONDS': 10.0,
        'DB_SCHEMA_MAX_ATTEMPTS': 3,
        'DB_SCHEMA_RETRY_SECONDS': 15.0,
        'DB_SCHEMA_VERSION': 'test-schema',
        '_DB_SCHEMA_STALL_SECONDS': 45.0,
        'utc_now_naive': lambda: datetime.datetime.utcnow(),
    }
    env['_DB_CONNECTED'].set()
    exec(compile(ast.Module(body=[_extract('_db_bootstrap_payload')], type_ignores=[]), '<payload>', 'exec'), env)
    return env


def test_actual_payload_does_not_call_connected_database_migrating_when_no_schema_attempt_started():
    env = _payload_env()
    out = env['_db_bootstrap_payload']()
    assert out['signal'] == 'CHECKING'
    assert out['status'] == 'connected'
    assert out['phase'] == 'schema_pending'
    assert out['schema_attempts'] == 0
    assert out['schema_started_at'] is None


def test_actual_payload_reports_real_migration_once_attempt_started():
    env = _payload_env()
    env['_DB_STATUS'] = 'migrating'
    env['_DB_PHASE'] = 'schema_migration'
    env['_DB_SCHEMA_ATTEMPTS'] = 1
    env['_DB_SCHEMA_MODE'] = 'migration'
    env['_DB_SCHEMA_CURRENT_STEP'] = 7
    now = datetime.datetime.utcnow().isoformat()
    env['_DB_SCHEMA_LAST_ATTEMPT_AT'] = now
    env['_DB_SCHEMA_LAST_PROGRESS_AT'] = now
    out = env['_db_bootstrap_payload']()
    assert out['signal'] == 'MIGRATING'
    assert out['current_step'] == 7


def test_actual_payload_reports_waiting_for_other_migrator():
    env = _payload_env()
    env['_DB_STATUS'] = 'migrating'
    env['_DB_PHASE'] = 'schema_migration_waiting_lock'
    env['_DB_SCHEMA_WAITING_FOR_MIGRATOR'] = True
    out = env['_db_bootstrap_payload']()
    assert out['signal'] == 'WAITING_FOR_MIGRATOR'


def test_actual_payload_reports_terminal_schema_error():
    env = _payload_env()
    env['_DB_STATUS'] = 'schema_error'
    env['_DB_PHASE'] = 'schema_migration_terminal'
    env['_DB_SCHEMA_TERMINAL_ERROR'] = True
    env['_DB_SCHEMA_ERROR'] = 'synthetic migration failure'
    out = env['_db_bootstrap_payload']()
    assert out['signal'] == 'ERROR'
    assert out['error'] == 'synthetic migration failure'


def test_ui_and_api_contract_exposes_checking_state_for_connected_pending_schema():
    html = (ROOT / 'templates' / 'index.html').read_text(encoding='utf-8')
    assert "signal==='CHECKING'||signal==='CONNECTED'" in html
    assert "Schema worker starting…" in html
