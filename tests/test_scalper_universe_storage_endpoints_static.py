from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / 'app.py').read_text(encoding='utf-8')
LIVE = (ROOT / 'research' / 'scalper' / 'scalper_live_paper.py').read_text(encoding='utf-8')
CLIENT = (ROOT / 'indstocks_client.py').read_text(encoding='utf-8')
HTML = (ROOT / 'templates' / 'index.html').read_text(encoding='utf-8')


def test_scalper_uses_authoritative_large_midcap_250_and_is_fail_closed():
    assert 'fetch_and_cache_large_mid_cap_allowlist()' in APP
    assert "raise RuntimeError('Nifty LargeMidcap 250 universe is unavailable." in APP
    assert 'SCALPER_MAX_UNIVERSE = 250' in APP
    assert 'nifty_largemidcap_250_with_liquid_priority' in APP


def test_scalper_quote_endpoints_match_documented_paths_and_use_250_batch():
    assert 'def get_full_quote' in CLIENT
    assert '"/market/quotes/full"' in CLIENT
    assert 'def get_market_depth' in CLIENT
    assert '"/market/quotes/mkt"' in CLIENT
    assert 'def get_ltp' in CLIENT
    assert '"/market/quotes/ltp"' in CLIENT
    assert "SCALPER_QUOTE_BATCH_SIZE = 250" in APP
    assert 'SCALPER_QUOTE_BATCH_SIZE))), SCALPER_MAX_UNIVERSE, 250)' in APP
    assert 'interval = max(1.0' in LIVE
    assert '_MIN_GAP_SECONDS = float(os.environ.get("INDSTOCKS_MIN_REQUEST_GAP", "0.21"))' in CLIENT


def test_storage_estimator_and_session_delete_routes_exist():
    assert "@app.get('/api/scalper/live/estimate')" in APP
    assert "@app.get('/api/scalper/live/sessions')" in APP
    assert "@app.delete('/api/scalper/live/session/<int:session_id>')" in APP
    assert "ON DELETE CASCADE" in APP
    assert 'pg_total_relation_size' in APP
    assert 'estimated_total_snapshot_rows' in APP
    assert 'estimated_total_db_bytes' in APP
    assert 'quote_daily_limit' in APP


def test_live_ui_has_20_to_250_choices_estimator_and_session_delete():
    for value in ('20', '40', '50', '75', '100', '150', '200', '250'):
        assert f'<option value="{value}"' in HTML
    assert 'scalperRefreshLiveEstimate' in HTML
    assert 'scLiveEstimateRows' in HTML
    assert 'scLiveEstimateStorage' in HTML
    assert 'loadScalperSessions' in HTML
    assert 'deleteScalperSession' in HTML
    assert '/api/scalper/live/session/${id}' in HTML


def test_scalper_schema_fast_path_is_read_only_and_request_safe():
    assert '_SCALPER_SCHEMA_LOCAL_READY = False' in APP
    assert '_SCALPER_SCHEMA_LOCAL_LOCK = threading.Lock()' in APP
    assert 'information_schema.columns' in APP
    start = APP.index('def _ensure_scalper_live_schema(conn)')
    end = APP.index('# Legacy DDL list retained only as an explicit maintenance artifact', start)
    fn = APP[start:end]
    assert 'cur.execute("CREATE TABLE' not in fn
    assert 'cur.execute("ALTER TABLE' not in fn
    assert 'INSERT INTO scalper_live_settings' not in fn
    assert '_SCALPER_SCHEMA_LOCAL_READY = True' in fn

def test_scalper_legacy_ddl_has_250_default_but_is_not_in_request_path():
    assert 'universe_limit INT NOT NULL DEFAULT 250' in APP
    assert '_SCALPER_LEGACY_DDL' in APP
    start = APP.index('def _ensure_scalper_live_schema(conn)')
    end = APP.index('# Legacy DDL list retained only as an explicit maintenance artifact', start)
    request_path = APP[start:end]
    assert 'CREATE TABLE IF NOT EXISTS' not in request_path
    assert 'INSERT INTO scalper_live_settings' not in request_path


def test_scalper_settings_are_upserted_and_round_trip_verified():
    assert 'INSERT INTO scalper_live_settings (id,' in APP
    assert 'ON CONFLICT (id) DO UPDATE SET' in APP
    assert 'Scalper settings round-trip mismatch' in APP
    assert "merged['universe_limit'] = max(20, min(int(merged.get('universe_limit') or 250), SCALPER_MAX_UNIVERSE))" in APP

def test_scalper_storage_endpoints_avoid_full_snapshot_and_trade_table_counts():
    start = APP.index("@app.get('/api/scalper/live/sessions')")
    end = APP.index("@app.delete('/api/scalper/live/session/<int:session_id>')", start)
    fn = APP[start:end]
    assert 'COUNT(*) FROM scalper_live_snapshots' not in fn
    assert 'COUNT(*) FROM scalper_paper_trades' not in fn
    assert 's.stored_snapshot_count AS snapshot_rows' in fn

def test_scalper_estimator_resolves_real_selected_count():
    start = APP.index('def _scalper_estimate(')
    end = APP.index("@app.get('/api/scalper/live/universe')", start)
    fn = APP[start:end]
    assert '_scalper_get_universe(requested_n)' in fn
    assert 'requested_universe_count' in fn
    assert 'resolved_universe_count' in fn
