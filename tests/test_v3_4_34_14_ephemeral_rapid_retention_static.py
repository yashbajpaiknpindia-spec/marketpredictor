from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
APP=(ROOT/'app.py').read_text()
HTML=(ROOT/'templates/index.html').read_text()


def test_default_ephemeral_policy_is_explicit_and_selectable():
    assert "'raw_data_policy':raw_policy" in APP
    assert "raw_policy=str(raw_data_policy or 'EPHEMERAL_DELETE_AFTER_SESSION')" in APP
    assert "KEEP_TESTED_DATA" in APP
    assert "EPHEMERAL_DELETE_AFTER_SESSION" in APP


def test_cleanup_is_after_durable_checkpoint_and_idempotent_on_restart():
    assert "session_checkpoint_saved=_rapid_write_run" in APP
    assert "if not session_checkpoint_saved:" in APP
    assert "pending_ephemeral_cleanup_cells" in APP
    assert "Resuming safe cleanup" in APP
    assert "_rapid_stream_delete_created_session_candles" in APP


def test_only_new_candle_cells_are_eligible_for_ephemeral_deletion():
    assert "_rapid_stream_new_candle_cells" in APP
    assert "WHERE c.ticker IS NULL" in APP
    assert "DELETE FROM intraday_replay_candle_cache" in APP
    assert "Do NOT delete market_data_coverage here" in APP
    assert "retained_coverage_rows" in APP


def test_evidence_provenance_survives_raw_candle_deletion():
    assert "'dataset_fingerprint':_market_data_manifest_fingerprint(market,'1m',universe,[day])" in APP
    assert "stream_dataset_fingerprints" in APP
    assert "config['dataset_fingerprint']=hashlib.sha256" in APP
    assert "session_dataset_fingerprints" in APP


def test_restart_launcher_uses_current_worker_signature():
    assert "42,'balanced'" not in APP
    assert "float(row.get('notional_per_signal') or plan.get('notional_per_signal_inr') or 10000.0),cp)" in APP


def test_resource_estimate_distinguishes_temporary_and_retained_storage():
    assert "estimated_peak_session_market_storage_bytes" in APP
    assert "estimated_retained_raw_market_storage_bytes" in APP
    assert "estimated_database_after_bytes" in APP
    assert "raw_data_policy" in APP
    assert "Peak temporary candle storage" in HTML
    assert "Raw candles retained after run" in HTML


def test_rapid_ui_defaults_to_delete_after_session_but_allows_keep():
    assert '<select id="rapidRawDataPolicy"' in HTML
    assert 'Delete tested candles after each session' in HTML
    assert 'Keep tested candles' in HTML
