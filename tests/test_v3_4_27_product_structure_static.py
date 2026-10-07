from pathlib import Path
from bs4 import BeautifulSoup

ROOT=Path(__file__).resolve().parents[1]
APP=(ROOT/'app.py').read_text(encoding='utf-8')
HTML=(ROOT/'templates'/'index.html').read_text(encoding='utf-8')

def test_fresh_db_creates_candle_cache_before_index():
    table=APP.index('CREATE TABLE IF NOT EXISTS intraday_replay_candle_cache')
    idx=APP.index('CREATE INDEX IF NOT EXISTS idx_intraday_replay_candle_coverage')
    assert table < idx
    assert 'def _ensure_replay_storage_schema' in APP

def test_hidden_navigation_and_copilot():
    soup=BeautifulSoup(HTML,'html.parser')
    tabs=soup.select('.tabs .tab-btn')
    ids={x.get('id') for x in tabs}
    assert 'tabScanner' not in ids
    assert 'tabAccuracy' not in ids
    assert 'tabCosts' not in ids
    assert 'tabAICopilot' in ids
    assert 'tabMore' in ids
    menu=soup.find(id='moreMenu')
    text=menu.get_text(' ', strip=True)
    for label in ('AI Scanner','Track Record','API Costs','Research Lab','Trading Automation'):
        assert label in text
    assert soup.find(id='aiCopilotView') is not None

def test_copilot_is_read_only_and_context_bound():
    assert "@app.route('/api/ai-copilot/context'" in APP
    assert "@app.route('/api/ai-copilot/ask'" in APP
    assert 'never place orders' in APP
    assert 'ai_copilot_events' in APP

def test_risk_defaults_are_conservative_and_explicit():
    assert "TRADING_RISK_PER_TRADE_PCT', '0.25'" in APP
    assert "TRADING_DAILY_LOSS_LIMIT_PCT', '1.25'" in APP
    assert 'Risk per trade %' in HTML
    assert '0.25% of capital' in HTML

def test_primary_title_no_longer_calls_site_ai_scanner():
    assert '<title>MarketPredictor · Research & Replay</title>' in HTML
    assert '<h1>MarketPredictor</h1>' in HTML
