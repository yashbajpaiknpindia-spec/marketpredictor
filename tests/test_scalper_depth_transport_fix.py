from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / 'app.py').read_text(encoding='utf-8')
LIVE = (ROOT / 'research' / 'scalper' / 'scalper_live_paper.py').read_text(encoding='utf-8')
CLIENT = (ROOT / 'indstocks_client.py').read_text(encoding='utf-8')


def test_live_fetch_uses_dedicated_depth_endpoint():
    block = APP[APP.index('def _scalper_fetch_quotes'):APP.index('def _scalper_create_session')]
    assert 'get_market_depth(missing)' in block
    assert 'get_full_quote(chunk)' in block
    assert "row['market_depth'] = {'depth':" in block


def test_depth_data_quality_gate_and_metrics_exist():
    assert 'blocked_no_l5_depth' in LIVE
    assert 'depth_valid_count' in LIVE
    assert 'depth_zero_count' in LIVE
    assert 'depth_coverage_pct' in LIVE
    assert 'depth_endpoint_ok' in LIVE


def test_rest_depth_poll_is_rate_safe():
    assert 'interval = max(1.0' in LIVE
    assert 'INDSTOCKS_MIN_REQUEST_GAP' in CLIENT


def test_provider_test_reports_real_depth_levels():
    block = APP[APP.index("@app.post('/api/scalper/live/test-provider')"):APP.index('def _load_v2_frozen_model')]
    assert "'depth_levels_received': direct_levels" in block
    assert 'get_market_depth_diagnostic(code)' in block
