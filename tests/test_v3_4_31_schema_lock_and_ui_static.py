from pathlib import Path
import ast

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / 'app.py'
HTML = ROOT / 'templates' / 'index.html'
APP_TEXT = APP.read_text(encoding='utf-8')
HTML_TEXT = HTML.read_text(encoding='utf-8')


def test_schema_bootstrap_has_cluster_lock_and_bounded_attempts():
    assert "DB_SCHEMA_ADVISORY_LOCK_KEY = 'marketpredictor:schema-bootstrap:v1'" in APP_TEXT
    assert 'DB_SCHEMA_MAX_ATTEMPTS' in APP_TEXT
    assert 'pg_try_advisory_lock' in APP_TEXT
    assert 'pg_advisory_unlock' in APP_TEXT
    assert 'if _DB_SCHEMA_ATTEMPTS >= DB_SCHEMA_MAX_ATTEMPTS:' in APP_TEXT
    assert "_DB_PHASE = 'schema_migration_terminal'" in APP_TEXT


def test_schema_bootstrap_waits_when_another_instance_owns_lock():
    bg = APP_TEXT[APP_TEXT.index('def _initialize_database_background'):]
    assert "_DB_SCHEMA_MODE = 'waiting_for_migrator'" in bg
    assert "status=WAITING_FOR_MIGRATOR" in bg
    assert "time.sleep(2.0)" in bg
    assert 'if _schema_fast_path_ready(conn):' in bg


def test_fast_path_marker_write_is_under_advisory_lock():
    bg = APP_TEXT[APP_TEXT.index('def _initialize_database_background'):]
    assert bg.index("lock_held = True") < bg.index('if _schema_fast_path_ready(conn):')
    assert bg.index('if _schema_fast_path_ready(conn):') < bg.index('_schema_state_upsert(conn)')


def test_bootstrap_ui_is_always_visible_and_reports_step():
    assert 'id="dbBootstrapPanel"' in HTML_TEXT
    assert 'id="dbBootstrapSignal"' in HTML_TEXT
    assert 'id="dbBootstrapProgress"' in HTML_TEXT
    assert 'id="dbBootstrapElapsed"' in HTML_TEXT
    assert 'id="dbBootstrapAttempts"' in HTML_TEXT
    assert 'copyDbBootstrapReport' in HTML_TEXT
    for signal in ('READY', 'MIGRATING', 'WAITING_FOR_MIGRATOR', 'STALLED', 'ERROR'):
        assert signal in HTML_TEXT


def test_advisory_lock_helper_locally_simulates_two_instances():
    tree = ast.parse(APP_TEXT)
    wanted = {'_try_schema_advisory_lock', '_release_schema_advisory_lock'}
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in wanted]
    ns = {'DB_SCHEMA_ADVISORY_LOCK_KEY': 'marketpredictor:schema-bootstrap:v1'}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(APP), 'exec'), ns)

    class Cursor:
        def __init__(self, acquire):
            self.acquire = acquire
            self.executed = []
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def execute(self, sql, params=None): self.executed.append((sql, params))
        def fetchone(self): return (self.acquire,)

    class Conn:
        def __init__(self, acquire): self.cursor_obj = Cursor(acquire)
        def cursor(self): return self.cursor_obj

    first = Conn(True)
    second = Conn(False)
    assert ns['_try_schema_advisory_lock'](first) is True
    assert ns['_try_schema_advisory_lock'](second) is False
    ns['_release_schema_advisory_lock'](first)
    assert 'pg_advisory_unlock' in first.cursor_obj.executed[-1][0]
