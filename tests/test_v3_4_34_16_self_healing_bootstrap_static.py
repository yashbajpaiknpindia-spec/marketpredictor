from pathlib import Path
import ast

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / 'app.py'
TEXT = APP.read_text(encoding='utf-8')


def test_schema_bootstrap_is_fail_open_by_default():
    assert "DATABASE_SCHEMA_FAIL_CLOSED" in TEXT
    assert "os.environ.get('DATABASE_SCHEMA_MAX_ATTEMPTS', '0')" in TEXT
    assert 'DB_SCHEMA_MAX_ATTEMPTS = 0' in TEXT


def test_schema_worker_uses_unbounded_migration_session():
    assert 'def _db_schema_session_limits(conn)' in TEXT
    assert "SET statement_timeout = 0" in TEXT
    assert "SET lock_timeout = 0" in TEXT
    assert "SET idle_in_transaction_session_timeout = 0" in TEXT
    block = TEXT[TEXT.index('def _initialize_database_background'):TEXT.index('def _database_bootstrap_runner')]
    assert '_db_schema_session_limits(conn)' in block
    assert "_open_direct_db_connection(timeout_seconds=max(15, _db_connect_timeout_seconds()))" in block


def test_live_worker_cannot_be_marked_stalled_before_schema_attempt():
    block = TEXT[TEXT.index('def _ensure_database_bootstrap_worker'):TEXT.index('# Start immediately after the bootstrap functions are defined.')]
    assert 'bootstrap_worker_stalled_before_schema' not in block
    assert 'if _DB_BOOTSTRAP_THREAD is not None and _DB_BOOTSTRAP_THREAD.is_alive():' in block
    assert 'return True' in block


def test_unbounded_schema_retry_phase_is_explicit():
    block = TEXT[TEXT.index('def _initialize_database_background'):TEXT.index('def _database_bootstrap_runner')]
    assert "_DB_PHASE = 'schema_migration_retrying'" in block
    assert "print(f'[DB_BOOTSTRAP] status=RETRYING phase=schema_migration_retrying" in block
    assert 'DB_SCHEMA_RETRY_MAX_SECONDS' in block


def test_bootstrap_supervisor_recovers_from_unexpected_worker_exception():
    block = TEXT[TEXT.index('def _database_bootstrap_runner'):TEXT.index('def _ensure_database_bootstrap_worker')]
    assert 'while not _DB_READY.is_set()' in block
    assert "_DB_PHASE = 'bootstrap_worker_crashed_retrying'" in block
    assert '_DB_SCHEMA_TERMINAL_ERROR = False' in block
    assert '_DB_INIT_STARTED = False' in block


def test_ready_error_path_does_not_report_transient_schema_failure_as_terminal():
    start = TEXT.index('def _database_ready_or_error')
    end = TEXT.index('def _db_bootstrap_payload')
    block = TEXT[start:end]
    assert "and not _DB_SCHEMA_TERMINAL_ERROR" in block
    assert 'schema bootstrap is retrying automatically' in block
    assert 'schema bootstrap is still converging' in block


def test_bootstrap_payload_only_emits_schema_error_when_terminal():
    start = TEXT.index('def _db_bootstrap_payload')
    end = TEXT.index("@app.get('/api/db-bootstrap-status')")
    block = TEXT[start:end]
    assert "elif _DB_STATUS == 'schema_error' and _DB_SCHEMA_TERMINAL_ERROR:" in block
    assert "_DB_PHASE in ('schema_migration_retrying', 'bootstrap_worker_crashed_retrying')" in block
    assert 'worker_alive = bool(_DB_BOOTSTRAP_THREAD is not None and _DB_BOOTSTRAP_THREAD.is_alive())' in block
