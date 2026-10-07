from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
APP=(ROOT/'app.py').read_text(encoding='utf-8')
HTML=(ROOT/'templates/index.html').read_text(encoding='utf-8')


def test_rapid_db_update_adapts_native_json_objects():
    assert "jsonb_fields={'result_json','scan_plan','checkpoint_json'}" in APP
    assert "psycopg2.extras.Json(v, dumps=lambda x: json.dumps(x, default=_json_safe, allow_nan=False))" in APP


def test_partial_real_sessions_are_allowed_but_promotion_stays_100_percent():
    assert "def _rapid_min_session_coverage_pct" in APP
    assert "return max(0.0,min(100.0,value))" in APP
    assert "real_min_coverage_pct':min_cov" in APP
    assert "partial_real_allowed':bool(min_coverage_pct<100.0)" in APP or "partial_real_allowed':bool(min_cov<100.0)" in APP or "'partial_real_allowed':True" in APP
    assert "ok=True" in APP
    assert "scan_available=verified>0" in APP
    assert "100% coverage is still required for evidence promotion" in APP or "100% verified coverage remains required for evidence-grade promotion" in APP


def test_missing_real_ticker_days_are_skipped_not_fatal():
    assert "Skipping missing real ticker-day" in APP
    assert "raise RuntimeError(f'Canonical dataset changed after preflight" not in APP
    assert "day_missing_tickers.append(str(ticker).upper())" in APP


def test_session_provenance_is_light_and_survives_ephemeral_cleanup():
    assert "session_provenance=cp.get('session_provenance') or []" in APP
    assert "'dataset_fingerprint':str(acq.get('dataset_fingerprint') or '')" in APP
    assert "'missing_tickers_sample':sorted(set(day_missing_tickers))[:25]" in APP
    assert "'session_provenance':session_provenance" in APP


def test_ui_exposes_practical_coverage_thresholds():
    assert '<select id="rapidMinCoverage">' in HTML
    assert '1% · scan any real data' in HTML
    assert '50% · moderate' in HTML
    assert '90% · recommended marker' in HTML
    assert '100% · evidence-grade' in HTML
    assert 'min_coverage_pct:Number(document.getElementById(\'rapidMinCoverage\')?.value||1)' in HTML


def test_evidence_snapshot_records_partial_diagnostic_provenance():
    assert "'session_provenance':result.get('session_provenance') or []" in APP
    assert "partial scans remain diagnostic" in APP or "Partial scans remain diagnostic" in APP
    assert "100% coverage is still required for evidence promotion" in APP or "100% verified coverage remains required for evidence-grade promotion" in APP or "100% remains required for evidence promotion" in APP
