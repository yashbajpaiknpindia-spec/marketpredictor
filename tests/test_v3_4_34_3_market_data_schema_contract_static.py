from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / 'app.py').read_text(encoding='utf-8')


def test_market_data_jobs_is_part_of_fast_path_schema_contract():
    assert "DB_SCHEMA_VERSION = '3.4.34.16-self-healing-bootstrap'" in APP
    assert "'market_data_jobs'" in APP
    assert "('market_data_jobs', 'dataset_fingerprint')" in APP
    assert "('market_data_jobs', 'diagnostics')" in APP
    assert "'market_data_coverage', 'market_data_jobs'" in APP or "'market_data_coverage', 'market_data_jobs'," in APP


def test_market_data_jobs_endpoint_has_idempotent_schema_guard():
    start = APP.index('def _ensure_market_data_jobs_schema()')
    end = APP.index("@app.route('/api/evidence/snapshots'", start)
    block = APP[start:end]
    assert 'CREATE TABLE IF NOT EXISTS market_data_jobs' in block
    assert "'dataset_fingerprint': 'VARCHAR(64)'" in block
    assert 'ALTER TABLE market_data_jobs ADD COLUMN IF NOT EXISTS {col} {typ}' in block
    assert 'def market_data_jobs_endpoint()' in block
    assert 'if not _ensure_market_data_jobs_schema()' in block


def test_start_and_job_routes_are_guarded():
    for fn in (
        'def market_data_start_endpoint():',
        'def market_data_jobs_endpoint():',
        'def market_data_job_status_endpoint(job_id:int):',
        'def market_data_job_stop_endpoint(job_id:int):',
        'def market_data_job_coverage_endpoint(job_id:int):',
        'def market_data_job_rapid_scan_endpoint(job_id:int):',
    ):
        start = APP.index(fn)
        snippet = APP[start:start + 420]
        assert '_ensure_market_data_jobs_schema()' in snippet
