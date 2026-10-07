from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / 'app.py').read_text(encoding='utf-8')


def test_live_schema_check_is_read_only():
    start = APP.index('def _ensure_scalper_live_schema(conn)')
    end = APP.index('# Legacy DDL list retained only as an explicit maintenance artifact', start)
    fn = APP[start:end]
    assert '_scalper_schema_contract_present(conn)' in fn
    assert 'CREATE TABLE IF NOT EXISTS' not in fn
    assert 'cur.execute("ALTER TABLE' not in fn
    assert 'cur.execute("""ALTER TABLE' not in fn
    assert 'INSERT INTO scalper_live_settings' not in fn


def test_live_schema_contract_names_all_scalper_tables():
    start = APP.index('_SCALPER_SCHEMA_REQUIRED_COLUMNS =')
    end = APP.index('def _scalper_schema_contract_present', start)
    contract = APP[start:end]
    for table in ('scalper_live_settings', 'scalper_live_sessions', 'scalper_live_snapshots', 'scalper_paper_trades'):
        assert f"'{table}':" in contract


def test_live_status_checks_readiness_before_latest_session_query():
    pos_status = APP.index('def scalper_live_status_endpoint')
    pos_query = APP.index('SELECT * FROM scalper_live_sessions', pos_status)
    pos_ensure = APP.index('_ensure_scalper_live_schema(conn)', pos_status)
    assert pos_ensure < pos_query
    assert "state['scalper_schema_ready'] = True" in APP
    assert "state['data_store_error']" in APP


def test_scalper_schema_does_not_use_malformed_default_now():
    assert 'DEFAULT NOW",' not in APP
    assert 'DEFAULT NOW()' in APP
