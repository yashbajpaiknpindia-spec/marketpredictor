from pathlib import Path
from bs4 import BeautifulSoup

ROOT=Path(__file__).resolve().parents[1]
HTML=ROOT/'templates'/'index.html'
APP=ROOT/'app.py'

def test_page_views_are_not_nested_under_intraday_replay():
    soup=BeautifulSoup(HTML.read_text(), 'html.parser')
    main=soup.find('div', class_='main')
    assert main is not None
    replay=main.find(id='intradayReplayView', recursive=False)
    assert replay is not None
    for page_id in ('strategyDashboardView','historyView','exportView'):
        page=main.find(id=page_id, recursive=False)
        assert page is not None, page_id
        assert page.parent is main, f'{page_id} unexpectedly nested under {page.parent.get("id")}'
        assert not replay.find(id=page_id), f'{page_id} still nested in replay view'

def test_independent_inventory_endpoint_and_ui():
    app=APP.read_text()
    html=HTML.read_text()
    assert "@app.route('/api/data-inventory/coverage'" in app
    assert '_build_stored_data_inventory' in app
    assert 'id="storedDataInventoryCard"' in html
    assert 'id="dataInventoryCalendar"' in html
    assert 'loadStoredDataInventory()' in html
    assert 'This is an independent data-availability calendar' in html
    assert 'not linked to the Replay window' in html

def test_holiday_legend_and_independent_range_summary_exist():
    html=HTML.read_text()
    assert 'replay-coverage-dot.holiday' in html
    assert 'Market holiday' in html
    assert 'latest verified trading data' in html
    assert 'Jump to month' in html
