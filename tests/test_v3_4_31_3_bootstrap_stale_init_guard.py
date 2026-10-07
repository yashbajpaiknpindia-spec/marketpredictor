from pathlib import Path
import ast

ROOT = Path(__file__).resolve().parents[1]
APP_TEXT = (ROOT / "app.py").read_text(encoding="utf-8")


def _extract(name):
    tree = ast.parse(APP_TEXT)
    for n in tree.body:
        if isinstance(n, ast.FunctionDef) and n.name == name:
            return n
    raise AssertionError(name)


def test_stale_init_started_is_not_a_second_worker_gate():
    block = APP_TEXT[APP_TEXT.index("def _initialize_database_background"):APP_TEXT.index("def _database_bootstrap_runner")]
    assert "if _DB_INIT_STARTED:" not in block
    assert "_DB_INIT_STARTED = True" in block


def test_synthetic_worker_reaches_schema_when_init_started_was_pre_set():
    import threading
    import datetime
    import time
    from typing import Optional

    ready = threading.Event(); connected = threading.Event(); init_lock = threading.Lock()
    env = {
        'Optional': Optional, 'datetime': datetime, 'time': time, 'threading': threading,
        'DATABASE_URL': 'synthetic://postgres',
        '_DB_READY': ready, '_DB_CONNECTED': connected, '_DB_INIT_LOCK': init_lock,
        '_DB_INIT_STARTED': True,  # exact stale state from the production failure
        '_DB_INIT_ERROR': None, '_DB_SCHEMA_ERROR': None, '_DB_STATUS': 'connected',
        '_DB_PHASE': 'schema_pending', '_DB_INIT_ATTEMPTS': 0, '_DB_SCHEMA_ATTEMPTS': 0,
        '_DB_LAST_ATTEMPT_AT': None, '_DB_CONNECTED_AT': 'synthetic', '_DB_READY_AT': None,
        '_DB_SCHEMA_LAST_ATTEMPT_AT': None, '_DB_CONNECT_MS': 1.0, '_DB_SCHEMA_MS': None,
        '_DB_SCHEMA_MODE': None, '_DB_SCHEMA_CURRENT_STEP': 0, '_DB_SCHEMA_CURRENT_SQL': None,
        '_DB_SCHEMA_STEP_STARTED_AT': None, '_DB_SCHEMA_LAST_PROGRESS_AT': None,
        '_DB_SCHEMA_ERROR_AT': None, '_DB_SCHEMA_LAST_ERROR_STEP': None, '_DB_SCHEMA_LAST_ERROR': None,
        '_DB_SCHEMA_TERMINAL_ERROR': False, '_DB_SCHEMA_LOCK_HELD': False,
        '_DB_SCHEMA_WAITING_FOR_MIGRATOR': False, 'DB_SCHEMA_MAX_ATTEMPTS': 3,
        'DB_SCHEMA_RETRY_SECONDS': 0.0, 'DB_SCHEMA_RETRY_MAX_SECONDS': 0.0, 'print': lambda *a, **k: None,
        '_refresh_database_url_from_env': lambda: False, '_db_connect_timeout_seconds': lambda: 5,
        '_open_direct_db_connection': lambda timeout_seconds=None: type('Conn', (), {'close': lambda self: None})(),
        '_db_session_limits': lambda conn: None, '_db_schema_session_limits': lambda conn: None,
 '_try_schema_advisory_lock': lambda conn: True,
        '_schema_fast_path_ready': lambda conn: True, '_schema_state_upsert': lambda conn: None,
        '_release_schema_advisory_lock': lambda conn: None, '_diagnose_schema_blocker': lambda conn: None,
        'init_db': lambda conn: None,
    }
    class DiagnosticConnection:
        def __init__(self, conn): self.conn=conn
    env['_DiagnosticConnection'] = DiagnosticConnection
    original_sleep=time.sleep; time.sleep=lambda _: None
    try:
        node=_extract('_initialize_database_background')
        exec(compile(ast.Module(body=[node], type_ignores=[]), '<synthetic>', 'exec'), env)
        env['_initialize_database_background']()
    finally:
        time.sleep=original_sleep
    assert env['_DB_SCHEMA_ATTEMPTS'] == 1
    assert env['_DB_READY'].is_set()
    assert env['_DB_SCHEMA_MODE'] == 'fast_path'
