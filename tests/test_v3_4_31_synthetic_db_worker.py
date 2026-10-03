from pathlib import Path
import ast
import datetime
import threading
import time
from typing import Optional

ROOT = Path(__file__).resolve().parents[1]
APP_TEXT = (ROOT / 'app.py').read_text(encoding='utf-8')


def _extract(name):
    tree = ast.parse(APP_TEXT)
    for n in tree.body:
        if isinstance(n, ast.FunctionDef) and n.name == name:
            return n
    raise AssertionError(name)


def _base_env():
    ready = threading.Event(); connected = threading.Event(); init_lock = threading.Lock()
    env = {
        'Optional': Optional, 'datetime': datetime, 'time': time, 'threading': threading,
        'DATABASE_URL': 'synthetic://postgres',
        '_DB_READY': ready, '_DB_CONNECTED': connected, '_DB_INIT_LOCK': init_lock,
        '_DB_INIT_STARTED': False, '_DB_INIT_ERROR': None, '_DB_SCHEMA_ERROR': None,
        '_DB_STATUS': 'connecting', '_DB_PHASE': 'network_connect',
        '_DB_INIT_ATTEMPTS': 0, '_DB_SCHEMA_ATTEMPTS': 0, '_DB_LAST_ATTEMPT_AT': None,
        '_DB_CONNECTED_AT': None, '_DB_READY_AT': None, '_DB_SCHEMA_LAST_ATTEMPT_AT': None,
        '_DB_CONNECT_MS': None, '_DB_SCHEMA_MS': None, '_DB_SCHEMA_MODE': None,
        '_DB_SCHEMA_CURRENT_STEP': 0, '_DB_SCHEMA_CURRENT_SQL': None,
        '_DB_SCHEMA_STEP_STARTED_AT': None, '_DB_SCHEMA_LAST_PROGRESS_AT': None,
        '_DB_SCHEMA_ERROR_AT': None, '_DB_SCHEMA_LAST_ERROR_STEP': None,
        '_DB_SCHEMA_LAST_ERROR': None, '_DB_SCHEMA_TERMINAL_ERROR': False,
        '_DB_SCHEMA_LOCK_HELD': False, '_DB_SCHEMA_WAITING_FOR_MIGRATOR': False,
        'DB_SCHEMA_MAX_ATTEMPTS': 3, 'DB_SCHEMA_RETRY_SECONDS': 0.0,
        '_open_direct_db_connection': lambda: object(), '_db_session_limits': lambda conn: None,
        '_ensure_db_pool': lambda: None, '_schema_fast_path_ready': lambda conn: True,
        '_schema_state_upsert': lambda conn: None, 'init_db': lambda conn: None,
        '_diagnose_schema_blocker': lambda conn: None, '_release_schema_advisory_lock': lambda conn: None,
    }
    class FakeConn:
        def close(self): pass
    env['_open_direct_db_connection'] = lambda: FakeConn()
    env['print'] = lambda *a, **k: None
    class DiagnosticConnection:
        def __init__(self, conn): self.conn = conn
    env['_DiagnosticConnection'] = DiagnosticConnection
    return env


def _run_worker(env, lock_results, fast_path=True, init_failure=None):
    lock_iter = iter(lock_results)
    env['_try_schema_advisory_lock'] = lambda conn: next(lock_iter)
    env['_schema_fast_path_ready'] = lambda conn: fast_path
    if init_failure is None:
        env['init_db'] = lambda conn: None
    else:
        def failing(conn):
            raise RuntimeError(init_failure)
        env['init_db'] = failing
    original_sleep = env['time'].sleep
    env['time'].sleep = lambda _: None
    try:
        fn = _extract('_initialize_database_background')
        exec(compile(ast.Module(body=[fn], type_ignores=[]), '<worker>', 'exec'), env)
        env['_initialize_database_background']()
    finally:
        env['time'].sleep = original_sleep
    return env


def test_synthetic_existing_schema_fast_path_reaches_ready_once():
    env = _run_worker(_base_env(), [True], fast_path=True)
    assert env['_DB_READY'].is_set()
    assert env['_DB_STATUS'] == 'ready'
    assert env['_DB_SCHEMA_ATTEMPTS'] == 1
    assert env['_DB_SCHEMA_MODE'] == 'fast_path'


def test_synthetic_second_instance_waits_then_reaches_ready_without_consuming_attempts():
    env = _run_worker(_base_env(), [False, True], fast_path=True)
    assert env['_DB_READY'].is_set()
    assert env['_DB_STATUS'] == 'ready'
    assert env['_DB_SCHEMA_ATTEMPTS'] == 1
    assert env['_DB_SCHEMA_WAITING_FOR_MIGRATOR'] is False


def test_synthetic_migration_failure_is_bounded_and_terminal():
    env = _run_worker(_base_env(), [True, True, True], fast_path=False, init_failure='synthetic DDL failure')
    assert not env['_DB_READY'].is_set()
    assert env['_DB_SCHEMA_ATTEMPTS'] == 3
    assert env['_DB_SCHEMA_TERMINAL_ERROR'] is True
    assert env['_DB_STATUS'] == 'schema_error'
    assert 'synthetic DDL failure' in env['_DB_SCHEMA_LAST_ERROR']
