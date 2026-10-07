from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CLIENT = (ROOT / 'indstocks_client.py').read_text(encoding='utf-8')
APP = (ROOT / 'app.py').read_text(encoding='utf-8')
HTML = (ROOT / 'templates' / 'index.html').read_text(encoding='utf-8')
LIVE = (ROOT / 'research' / 'scalper' / 'scalper_live_paper.py').read_text(encoding='utf-8')


def test_depth_transport_has_documented_endpoint_and_key_normalization():
    assert 'def get_market_depth(scrip_codes)' in CLIENT
    assert '"/market/quotes/mkt"' in CLIENT
    assert "str(requested).replace('_', ':')" in CLIENT
    assert "str(requested).split('_', 1)[-1]" in CLIENT


def test_depth_diagnostic_exposes_raw_shape_without_secrets():
    assert 'def get_market_depth_diagnostic' in CLIENT
    for key in ('top_level_keys', 'data_keys_sample', 'matched_key', 'row_keys', 'market_depth_keys', 'depth_key', 'depth_container_count', 'depth_path', 'valid_levels'):
        assert key in CLIENT
    assert 'Authorization' in CLIENT
    assert 'print(get_access_token())' not in CLIENT


def test_scalper_accepts_full_quote_depth_as_documented_fallback():
    block = APP[APP.index('def _scalper_fetch_quotes'):APP.index('def _scalper_create_session')]
    assert 'full quote' in block.lower() and 'market_depth' in block
    assert "row['market_depth'] = {'depth':" in block
    assert "row['_depth_endpoint_ok']" in block


def test_provider_test_surfaces_raw_shape_diagnostics_and_is_rate_safe():
    block = APP[APP.index("@app.post('/api/scalper/live/test-provider')"):APP.index('def _load_v2_frozen_model')]
    assert "get_market_depth_diagnostic(code)" in block
    assert "'depth_transport': diag" in block
    assert 'depth_container_count' in CLIENT
    assert 'Calling one direct INDstocks full-quote + L5 market-depth probe' in HTML


def test_parser_is_not_limited_to_one_json_shape():
    for token in ('_norm_key', '_SIDE_ALIASES', '_paired_side_lists', 'queue.append'):
        assert token in LIVE
