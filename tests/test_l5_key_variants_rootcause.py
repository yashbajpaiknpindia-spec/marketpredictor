import sys
sys.path.insert(0, '/mnt/data/l5_rootcause_fix')
from research.scalper.scalper_live_paper import extract_depth_levels


def check(payload):
    ex = extract_depth_levels(payload)
    assert ex['valid_levels'] == 5, (ex['valid_levels'], ex['shape'])
    return ex


def test_documented_shape():
    depth = {'depth': [
        {'buy': {'quantity': str(10+i), 'price': str(100-i*0.01)}, 'sell': {'quantity': str(20+i), 'price': str(100.1+i*0.01)}}
        for i in range(5)
    ]}
    assert check({'market_depth': depth})['bids'][0]['price'] == 100.0


def test_camelcase_and_capitalized_shape():
    payload = {'market_depth': {'Depth': [
        {'Buy': {'Quantity': '10', 'Price': '100'}, 'Sell': {'Quantity': '12', 'Price': '100.1'}} for _ in range(5)
    ]}}
    assert check(payload)['asks'][0]['qty'] == 12


def test_nested_unknown_wrapper_shape():
    levels = [{'BID': {'QTY': '10', 'PX': '100'}, 'ASK': {'QTY': '20', 'PX': '100.1'}} for _ in range(5)]
    payload = {'market_depth': {'some_provider_wrapper': {'BOOK_DATA': {'depthLevels': levels}}}}
    assert check(payload)['shape'] != 'none'


def test_keyed_levels_shape():
    levels = {str(i): {'buy': {'qty': '10', 'price': str(100-i*.01)}, 'sell': {'qty': '11', 'price': str(100.1+i*.01)}} for i in range(1,6)}
    assert check({'market_depth': {'depth': levels}})['valid_levels'] == 5
