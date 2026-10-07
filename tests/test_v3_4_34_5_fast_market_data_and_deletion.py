from pathlib import Path

APP = Path(__file__).resolve().parents[1] / 'app.py'
HTML = Path(__file__).resolve().parents[1] / 'templates' / 'index.html'

def test_market_data_fast_inventory_contract():
    src = APP.read_text()
    assert 'def _market_data_fast_coverage(' in src
    assert "'fast':True" in src
    assert "'needs_deep_audit':bool(suspect or cached_unverified)" in src
    assert "jsonb_array_length(c.candles)=%s" in src

def test_market_data_plan_is_fast_and_1m_explicit():
    src = APP.read_text()
    assert "def market_data_plan_endpoint()" in src
    assert "fast=_market_data_fast_coverage(market,interval,universe,dates,include_cells=False)" in src
    assert "'fast_inventory':True" in src
    assert 'Rapid/evidence acquisition uses canonical 1-minute data' in src

def test_market_data_progress_callback_and_eta():
    src = APP.read_text()
    assert 'progress_callback: Optional[Callable[[int,int,int],None]]' in src
    assert 'progress_callback(completed_batches,total_batches_est' in src
    assert "eta_seconds=round(eta,1)" in src
    html = HTML.read_text()
    assert 'id="mdProgressWrap"' in html
    assert 'id="mdProgressBar"' in html
    assert 'function mdRenderProgress' in html
    assert "setInterval(()=>mdPoll(id),1800)" in html

def test_deletion_integrity_watch_and_cascade():
    src = APP.read_text()
    assert 'def _data_integrity_audit_db(' in src
    assert "@app.route('/api/data-integrity/audit'" in src
    assert 'DELETE FROM strategy_evidence_rows WHERE snapshot_id=ANY(%s)' in src
    assert 'DELETE FROM strategy_evidence_snapshots WHERE id=ANY(%s)' in src
    assert "_log_deletion_integrity('rapid_forensics',root_id,conn)" in src
    assert "_log_deletion_integrity('intraday_replay',run_id)" in src
    assert "_log_deletion_integrity('trade_history', ids)" in src

def test_replay_cache_delete_invalidates_market_data_coverage():
    src = APP.read_text()
    assert "CACHE_PURGED_BY_REPLAY" in src
    assert "UPDATE market_data_coverage SET status='MISSING'" in src


def test_recent_stored_session_presets_and_cache_first_mapping():
    src = APP.read_text()
    assert 'def _market_data_recent_stored_sessions(' in src
    assert "mode in ('latest','latest_stored','recent','latest_stored_sessions')" in src
    assert "cached_symbol_map" in src
    assert "source':'provider_resolution'" in src

def test_stale_market_data_jobs_are_reconciled_and_resumable():
    src = APP.read_text()
    assert 'def _market_data_reconcile_stale_jobs(' in src
    assert "status='stalled'" in src
    assert "@app.route('/api/market-data/jobs/<int:job_id>/resume'" in src

def test_storage_page_and_safe_cleanup_contract():
    src = APP.read_text()
    page = (APP.parent / 'templates' / 'storage.html').read_text()
    assert "@app.route('/storage')" in src
    assert "@app.route('/api/storage/overview'" in src
    assert "@app.route('/api/storage/delete'" in src
    assert "intraday_replay_candle_cache" in page
    assert "DELETE RESEARCH DATA" in page
    assert "canonical market candles" in page
    assert 'MARKETPREDICTOR_DB_STORAGE_LIMIT_GB' in src

def test_export_page_offers_latest_stored_session_windows_and_storage_link():
    html = HTML.read_text()
    assert 'latest_3' in html
    assert 'Latest 3 available sessions' in html
    assert 'id="mdRecentRangeNote"' in html
    assert 'href="/storage"' in html


def test_exact_recent_session_selector_is_date_list_based():
    app = Path("app.py").read_text(encoding="utf-8")
    assert "def _market_data_select_dates_for_request" in app
    assert "return recent[0], recent[-1], f'latest_{len(recent)}_sessions', list(recent)" in app
    assert "dates=_market_data_select_dates_for_request" in app
    assert "selected_dates" in app


def test_storage_has_page_component_mapping_and_protected_canonical_store():
    html = Path("templates/storage.html").read_text(encoding="utf-8")
    assert "intraday_replay_candle_cache" in html
    assert "Application page / component" in html
    assert "Canonical market data" in html
    assert "PROTECTED" in html


def test_recent_session_ui_and_stale_reconcile_are_explicit():
    html=Path("templates/index.html").read_text(encoding="utf-8")
    app=Path("app.py").read_text(encoding="utf-8")
    assert 'Latest 2 available sessions' in html
    assert 'Latest 3 available sessions' in html
    assert 'selected_dates' in html
    assert 'heartbeat' in html
    assert '_market_data_reconcile_stale_jobs()' in app
