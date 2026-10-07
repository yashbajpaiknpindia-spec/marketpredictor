from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / 'app.py').read_text(encoding='utf-8')
HTML = (ROOT / 'templates' / 'index.html').read_text(encoding='utf-8')


def test_real_cache_mode_is_authoritative_and_blocks_synthetic_mixing():
    assert "if bool(cache_only):\n        mode='REAL'" in APP
    assert "if mode=='REAL':\n        cache_only=True" in APP
    assert "source_policy':'DATABASE_CACHE_OR_VERIFIED_EXPORT_ONLY' if mode=='REAL' else 'SYNTHETIC_IN_MEMORY'" in APP
    assert "_hydrate_replay_cache_from_historical_exports(" in APP
    assert "No synthetic fallback or provider download is allowed." in APP


def test_real_worker_keeps_source_integrity_without_hard_failing_partial_sessions():
    assert "if data_mode=='REAL':" in APP
    assert "source_counts.get('synthetic_ticker_days')" in APP
    assert "Partial real-data runs are valid diagnostics" in APP
    assert "100% is enforced only by the downstream evidence-promotion gate" in APP


def test_real_cache_reference_context_is_provider_free():
    assert "provider_fetch_allowed':False" in APP
    assert "allow_provider_fetch=False" in APP
    assert "def download_replay_index_history_cached" in APP
    assert "def download_replay_sector_index_history_cached" in APP
    assert "def download_replay_universe_daily_history_cached" in APP


def test_live_exit_snapshot_is_limited_to_post_entry_data():
    assert "def fetch_intraday_snapshot(ticker: str, market: Optional[str] = None, since_at: Optional[Any] = None)" in APP
    assert "hist=hist[hist.index>=cut]" in APP
    assert "fetch_intraday_snapshot(pos['ticker'], pos.get('market'), since_at=pos.get('opened_at'))" in APP


def test_ui_declares_verified_real_mode_and_sends_cache_only():
    assert 'Verified real 1m dataset' in HTML
    assert "data_mode:'REAL',cache_only:true" in HTML
    assert 'SYNTHETIC' not in HTML
