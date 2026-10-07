from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "app.py").read_text(encoding="utf-8")


def test_compatibility_mode_exists_and_is_before_global_lock_path():
    assert "def _schema_compatibility_ready(conn)" in APP
    assert "ready_compatibility" in APP
    assert "_background_schema_compatibility_repair" in APP
    compat = APP.index("if not _schema_fast_path_ready(conn) and _schema_compatibility_ready(conn):")
    lock = APP.index("if not _try_schema_advisory_lock(conn):")
    assert compat < lock


def test_compatibility_delta_is_additive_only():
    start = APP.index("def _apply_schema_compatibility_delta(conn)")
    end = APP.index("def _log_advisory_lock_holders(conn)")
    block = APP[start:end]
    assert "CREATE TABLE IF NOT EXISTS" in block
    assert "ALTER TABLE" in block
    assert "DELETE FROM" not in block
    assert "TRUNCATE" not in block
    assert "UPDATE " not in block
    assert "INSERT INTO" not in block


def test_lock_holder_diagnostics_exist():
    assert "def _log_advisory_lock_holders(conn)" in APP
    assert "pg_locks" in APP
    assert "lock_holder pid=" in APP


def test_schema_waiting_does_not_reconnect_every_two_seconds():
    assert "time.sleep(2.0)" not in APP[APP.index("if not _try_schema_advisory_lock(conn):"):APP.index("lock_held = True", APP.index("if not _try_schema_advisory_lock(conn):"))]
    assert "time.sleep(min(15.0, max(5.0, DB_SCHEMA_RETRY_SECONDS)))" in APP
