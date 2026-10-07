from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HTML = (ROOT / 'templates' / 'index.html').read_text(encoding='utf-8')


def test_scalper_and_intraday_are_separate_views():
    scalper_start = HTML.index('<div id="scalperView"')
    intraday_start = HTML.index('<div id="intradayView"')
    assert scalper_start < intraday_start
    scalper_block = HTML[scalper_start:intraday_start]
    assert 'id="scalperRealTestsBody"' in scalper_block
    assert 'id="scLiveQualityStatus"' in scalper_block
    assert 'id="scLiveDiagnostics"' in scalper_block
    assert 'Run full synthetic market lab' not in scalper_block
    assert 'Capital &amp; P&amp;L breakdown' not in scalper_block
    assert 'Position Guard' not in scalper_block
    assert 'Claude min approval confidence' not in scalper_block


def test_switch_tab_loads_only_the_selected_engine():
    assert "if (tab === 'scalper')" in HTML
    assert "loadScalperResearch()" in HTML
    assert "loadScalperLive()" in HTML
    assert "if (tab === 'intraday') { Promise.resolve(loadIntraday()).catch(e => showPageLoaderError('intradayView', e)); setTimeout(capInitDefaults, 600); }" in HTML
    assert "switchTab('scalper');" in HTML
