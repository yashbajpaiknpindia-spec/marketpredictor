from pathlib import Path
import ast

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / 'app.py'
TEXT = APP.read_text(encoding='utf-8')


def test_app_parses_after_schema_fastpath_fix():
    ast.parse(TEXT)


def test_schema_fastpath_contract_exists():
    assert "DB_SCHEMA_STATE_TABLE = 'marketpredictor_schema_state'" in TEXT
    assert "def _schema_fast_path_ready(conn)" in TEXT
    assert "def _schema_state_upsert(conn)" in TEXT
    assert "DB_SCHEMA_VERSION = '3.4.34.16-self-healing-bootstrap'" in TEXT
    assert "information_schema.tables" in TEXT
    assert "information_schema.columns" in TEXT


def test_schema_fastpath_runs_before_full_init():
    bg = TEXT[TEXT.index('def _initialize_database_background'):]
    assert "if _schema_fast_path_ready(conn):" in bg
    assert "init_db(diag_conn)" in bg
    assert bg.index("if _schema_fast_path_ready(conn):") < bg.index("init_db(diag_conn)")


def test_full_migration_stamps_schema_version():
    bg = TEXT[TEXT.index('def _initialize_database_background'):]
    assert "_schema_state_upsert(conn)" in bg
    assert bg.index("init_db(diag_conn)") < bg.rindex("_schema_state_upsert(conn)")


def test_schema_batching_removed_from_boot_path():
    assert 'DATABASE_SCHEMA_BATCH_SIZE' not in TEXT
    assert 'schema batching enabled' not in TEXT


def test_schema_errors_attempt_blocker_diagnostics():
    bg = TEXT[TEXT.index('def _initialize_database_background'):]
    assert "def _diagnose_schema_blocker(conn)" in TEXT
    assert "_diagnose_schema_blocker(conn)" in bg


def test_fastpath_adopts_existing_complete_schema_without_full_migration():
    tree = ast.parse(TEXT)
    names = {
        'DB_SCHEMA_STATE_TABLE', 'DB_SCHEMA_VERSION', 'DB_SCHEMA_REQUIRED_TABLES',
        'DB_SCHEMA_REQUIRED_COLUMNS', '_schema_state_upsert', '_schema_fast_path_ready'
    }
    nodes = []
    for node in tree.body:
        if isinstance(node, (ast.Assign, ast.FunctionDef)):
            target_names = set()
            if isinstance(node, ast.Assign):
                for t in node.targets:
                    if isinstance(t, ast.Name):
                        target_names.add(t.id)
            else:
                target_names.add(node.name)
            if target_names & names:
                nodes.append(node)
    ns = {}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(APP), 'exec'), ns)

    class Cursor:
        def __init__(self, responses):
            self.responses = iter(responses)
            self.executed = []
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def execute(self, sql, params=None):
            self.executed.append((str(sql), params))
        def fetchone(self): return next(self.responses)
        def fetchall(self): return next(self.responses)

    required_cols = ns['DB_SCHEMA_REQUIRED_COLUMNS']
    cursor = Cursor([
        (False,),
        (len(ns['DB_SCHEMA_REQUIRED_TABLES']),),
        list(required_cols),
        None,
    ])

    class Conn:
        def __init__(self): self.commit_calls = 0; self.c = cursor
        def cursor(self): return self.c
        def commit(self): self.commit_calls += 1
        def rollback(self): raise AssertionError('rollback should not occur')

    conn = Conn()
    assert ns['_schema_fast_path_ready'](conn) is True
    assert conn.commit_calls == 0
    assert not any('CREATE TABLE IF NOT EXISTS marketpredictor_schema_state' in sql for sql, _ in cursor.executed)
