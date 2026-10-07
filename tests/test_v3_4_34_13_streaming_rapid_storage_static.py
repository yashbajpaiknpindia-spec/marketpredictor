from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
APP=(ROOT/'app.py').read_text()
HTML=(ROOT/'templates/index.html').read_text()
STORAGE=(ROOT/'templates/storage.html').read_text()


def test_rapid_stream_helpers_and_estimate_exist():
    assert '_rapid_stream_estimate' in APP
    assert '_rapid_stream_acquire_session' in APP
    assert '/api/rapid-forensics/estimate' in APP
    assert "'execution_mode':'STREAM_ACQUIRE_SCAN'" in APP
    assert "'stream_acquire_scan':True" in APP


def test_rapid_stream_acquires_one_day_then_scans_and_checkpoints():
    assert 'acq=_rapid_stream_acquire_session(run_id,market,universe,day,stop_event,min_coverage_pct' in APP
    assert 'session_index=day_index' in APP
    assert "'stream_phase':'acquiring_session'" in APP
    assert 'Session {processed_sessions+1}/{len(dates)} · scanning available real data for {day}' in APP
    assert "download_replay_universe_history_cached(primary,day,day,'1m'" in APP


def test_rapid_start_uses_parent_parallel_scheduler():
    assert '_rapid_parallel_coordinator' in APP
    assert 'args=(parent_id,market,scheduled_selected,universe,lanes,batch_size,nps,stop)' in APP


def test_delete_all_scope_and_confirmation():
    assert '/api/storage/delete-all-data' in APP
    assert 'DELETE ALL MARKET AND RESEARCH DATA' in APP
    for table in ('intraday_replay_candle_cache','market_data_coverage','market_data_symbol_map','market_data_reference_cache','market_data_jobs','historical_export_archive_chunks'):
        assert f"DELETE FROM {table}" in APP
    assert "'trade_orders','trade_positions','intraday_settings','trading_settings','strategy_signal_stats','storage_cleanup_log'" in APP


def test_rapid_ui_shows_stream_resource_estimate():
    assert 'rapidResourceEstimate' in HTML
    assert '/api/rapid-forensics/estimate' in HTML
    assert 'Estimated peak web memory' in HTML
    assert 'Current missing-data storage if candles are kept' in HTML


def test_storage_ui_has_delete_all_control():
    assert 'Delete all market & research data' in STORAGE
    assert 'DELETE ALL MARKET AND RESEARCH DATA' in STORAGE


def test_rapid_ephemeral_retention_policy_and_cleanup_are_durable():
    assert "EPHEMERAL_DELETE_AFTER_SESSION" in APP
    assert "KEEP_TESTED_DATA" in APP
    assert "_rapid_stream_delete_created_session_candles" in APP
    assert "pending_ephemeral_cleanup_cells" in APP
    assert "session_checkpoint_saved=_rapid_write_run" in APP
    assert "if not session_checkpoint_saved:" in APP
    assert "temporary session candles deleted after each successful checkpoint" in APP


def test_rapid_restart_launcher_matches_worker_signature():
    assert "42,'balanced'" not in APP
    assert "float(row.get('notional_per_signal') or plan.get('notional_per_signal_inr') or 10000.0),cp)" in APP


def test_rapid_session_fingerprints_survive_raw_data_cleanup():
    assert "stream_dataset_fingerprints=cp.get('stream_dataset_fingerprints') or {}" in APP
    assert "'dataset_fingerprint':_market_data_manifest_fingerprint(market,'1m',universe,[day])" in APP
    assert "config['dataset_fingerprint']=hashlib.sha256" in APP
    assert "config['session_dataset_fingerprints']=dict(stream_dataset_fingerprints)" in APP


def test_rapid_ui_has_ephemeral_default_and_keep_option():
    assert 'rapidRawDataPolicy' in HTML
    assert 'Delete tested candles after each session' in HTML
    assert 'Keep tested candles' in HTML
    assert "raw_data_policy:document.getElementById('rapidRawDataPolicy')?.value||'EPHEMERAL_DELETE_AFTER_SESSION'" in HTML
    assert 'Peak temporary candle storage' in HTML
