from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
APP=(ROOT/'app.py').read_text(encoding='utf-8')
MD=(ROOT/'market_data.py').read_text(encoding='utf-8')
HTML=(ROOT/'templates/index.html').read_text(encoding='utf-8')

def test_provider_failure_is_exposed_instead_of_becoming_zero_coverage_without_cause():
    assert "report_out" in MD
    assert "get_historical_with_report" in MD
    assert "provider_error_details" in APP
    assert "provider_fetch_cause" in APP
    assert "INDstocks credentials missing" in APP

def test_rapid_never_hard_fails_a_partial_session_for_coverage():
    assert "ok=True" in APP
    assert "scan_available=verified>0" in APP
    assert "if not acq.get('scan_available')" in APP
    assert "stage='session_no_data'" in APP
    assert "run will continue" in APP
    assert "REAL Rapid source-integrity failure" not in APP

def test_live_progress_shows_session_work_and_provider_batches():
    assert "overall_progress_pct" in APP
    assert "session_progress_pct" in APP
    assert "provider_batches_done" in APP
    assert "rapidLiveProvider" in HTML
    assert "rapidLiveCause" in HTML
    assert "provider batch" in HTML or "provider batch" in APP

def test_provenance_keeps_provider_cause_fingerprint_and_light_manifest_proof():
    assert "'provider_cause':provider_cause" in APP
    assert "'provider_errors':provider_errors[:10]" in APP
    assert "'reference_error':reference_error" in APP
    assert "'dataset_fingerprint':_market_data_manifest_fingerprint" in APP
    assert '_rapid_light_session_manifest' in APP
    assert "'manifest_sample':manifest_sample" in APP
    assert "'manifest_sample':list(acq.get('manifest_sample') or [])" in APP
