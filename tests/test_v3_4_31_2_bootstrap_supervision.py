from pathlib import Path
import ast
import datetime
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / 'app.py'
APP_TEXT = APP.read_text(encoding='utf-8')


def _extract(*names):
    tree = ast.parse(APP_TEXT)
    found = {}
    for n in tree.body:
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name in names:
            found[n.name] = n
    missing = [n for n in names if n not in found]
    assert not missing, missing
    return found


def _exec_fn(name, env):
    node = _extract(name)[name]
    exec(compile(ast.Module(body=[node], type_ignores=[]), f'<{name}>', 'exec'), env)
    return env[name]


def test_bootstrap_supervisor_is_idempotent_and_request_visible():
    assert 'def _ensure_database_bootstrap_worker' in APP_TEXT
    assert "_ensure_database_bootstrap_worker('module_import')" in APP_TEXT
    assert "_ensure_database_bootstrap_worker('db_bootstrap_status')" in APP_TEXT
    assert "_ensure_database_bootstrap_worker(f'request:{request.path}')" in APP_TEXT
    assert "'bootstrap_worker_alive'" in APP_TEXT


def test_bootstrap_runner_converts_unexpected_worker_crash_to_terminal_error():
    ready = threading.Event()
    lock = threading.Lock()
    env = {
        'datetime': datetime,
        'threading': threading,
        '_initialize_database_background': lambda: (_ for _ in ()).throw(RuntimeError('synthetic worker crash')),
        '_DB_BOOTSTRAP_LAST_EXIT_AT': None,
        '_DB_BOOTSTRAP_LAST_CRASH': None,
        '_DB_INIT_STARTED': True,
        '_DB_SCHEMA_ERROR': None,
        '_DB_SCHEMA_TERMINAL_ERROR': False,
        '_DB_STATUS': 'connected',
        '_DB_PHASE': 'schema_pending',
        '_DB_READY': ready,
        '_DB_INIT_LOCK': lock,
        'print': lambda *a, **k: None,
    }
    fn = _exec_fn('_database_bootstrap_runner', env)
    fn()
    assert env['_DB_SCHEMA_TERMINAL_ERROR'] is True
    assert env['_DB_STATUS'] == 'schema_error'
    assert env['_DB_PHASE'] == 'bootstrap_worker_crashed'
    assert 'synthetic worker crash' in env['_DB_BOOTSTRAP_LAST_CRASH']
    assert env['_DB_INIT_STARTED'] is False
    assert env['_DB_BOOTSTRAP_LAST_EXIT_AT'] is not None


def test_bootstrap_supervisor_starts_a_synthetic_db_worker():
    ready = threading.Event()
    start_lock = threading.Lock()
    env = {
        'DATABASE_URL': 'synthetic://postgres',
        '_DB_READY': ready,
        '_DB_SCHEMA_TERMINAL_ERROR': False,
        '_DB_BOOTSTRAP_THREAD': None,
        '_DB_BOOTSTRAP_START_LOCK': start_lock,
        '_DB_BOOTSTRAP_START_COUNT': 0,
        '_DB_BOOTSTRAP_STARTED_AT': None,
        '_DB_BOOTSTRAP_LAST_CRASH': None,
        '_DB_BOOTSTRAP_MAX_STARTS': 3,
        'threading': threading,
        'datetime': datetime,
        'print': lambda *a, **k: None,
    }

    def fake_runner():
        ready.set()

    env['_database_bootstrap_runner'] = fake_runner
    fn = _exec_fn('_ensure_database_bootstrap_worker', env)
    assert fn('synthetic_test') is True
    thread = env['_DB_BOOTSTRAP_THREAD']
    assert thread is not None
    thread.join(timeout=1.0)
    assert not thread.is_alive()
    assert ready.is_set()
    assert env['_DB_BOOTSTRAP_START_COUNT'] == 1
    assert env['_DB_BOOTSTRAP_STARTED_AT'] is not None


def test_status_contract_exposes_worker_liveness_not_just_schema_attempts():
    for token in (
        "'bootstrap_worker_alive'",
        "'bootstrap_worker_start_count'",
        "'bootstrap_worker_started_at'",
        "'bootstrap_worker_last_exit_at'",
        "'bootstrap_worker_last_crash'",
    ):
        assert token in APP_TEXT


def test_module_import_path_no_longer_calls_thread_constructor_directly():
    assert "threading.Thread(target=_initialize_database_background, name='db-init', daemon=True).start()" not in APP_TEXT
    assert "_ensure_database_bootstrap_worker('module_import')" in APP_TEXT


def test_schema_worker_does_not_need_request_pool_and_handles_already_connected_state():
    # This reproduces the production handoff: an HTTP probe may set DB_CONNECTED
    # before the schema worker gets CPU time. The worker must still consume a schema
    # attempt and reach READY using its dedicated direct connection.
    import importlib.util
    worker_path = ROOT / 'tests' / 'test_v3_4_31_synthetic_db_worker.py'
    spec = importlib.util.spec_from_file_location('synthetic_db_worker_test', worker_path)
    worker_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(worker_mod)
    _base_env, _run_worker = worker_mod._base_env, worker_mod._run_worker
    env = _base_env()
    env['_DB_CONNECTED'].set()
    env['_DB_STATUS'] = 'connected'
    env['_DB_PHASE'] = 'schema_pending'
    env = _run_worker(env, [True], fast_path=True)
    assert env['_DB_SCHEMA_ATTEMPTS'] == 1
    assert env['_DB_READY'].is_set()
    assert env['_DB_SCHEMA_MODE'] == 'fast_path'
    # The worker source must not reintroduce a pool warm-up dependency.
    worker_text = APP_TEXT[APP_TEXT.index('def _initialize_database_background'):APP_TEXT.index('def _database_bootstrap_runner')]
    assert '_ensure_db_pool()' not in worker_text
