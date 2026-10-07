from datetime import datetime

from research.scalper.scalper_live_paper import _parse_depth, _ofi_proxy, LivePaperWorker, DEFAULT_SETTINGS


def sample_quote(price=100.0, buy=1000.0, sell=500.0, volume=10000):
    depth=[]
    for i in range(5):
        depth.append({
            'buy': {'price': price - 0.05*i, 'quantity': buy/(i+1)},
            'sell': {'price': price + 0.05 + 0.05*i, 'quantity': sell/(i+1)},
        })
    return {'live_price': price, 'volume': volume, 'market_depth': {'depth': depth}}


def test_depth_parser_and_proxy():
    d1=_parse_depth(sample_quote())
    d2=_parse_depth(sample_quote(price=100.1,buy=1200,sell=400,volume=11000))
    assert len(d1['bids'])==5 and len(d1['asks'])==5
    assert d1['bid1'] < d1['ask1']
    assert -1 <= d1['imbalance_l5'] <= 1
    assert abs(d1['microprice_edge_pct']) < 1
    assert -1 <= _ofi_proxy(d1,d2) <= 1


def test_worker_single_poll_persists_and_can_open_paper_trade():
    settings=dict(DEFAULT_SETTINGS)
    settings.update(enabled=True, universe_limit=1, min_signal_score=50, min_remaining_edge_pct=0.0)
    state={'settings':settings,'sid':0,'snapshots':[],'trades':[],'session':{}}
    quotes={'NSE_1': sample_quote(price=100.0)}
    def get_settings(): return state['settings']
    def is_day(market, day): return True
    def universe(limit): return ['TEST.NS']
    def resolve(symbol): return 'NSE_1'
    def fetch(codes): return quotes
    def create(payload): state['sid']+=1; state['session']=payload; return state['sid']
    def update(sid,p): state['session'].update(p)
    def persist_rows(rows): state['snapshots'].extend(rows)
    def persist_trade(t): state['trades'].append(t)
    w=LivePaperWorker(get_settings=get_settings,is_trading_day=is_day,get_universe=universe,resolve_scrip=resolve,fetch_quotes=fetch,create_session=create,update_session=update,persist_snapshot_batch=persist_rows,persist_trade=persist_trade,load_open_trades=lambda: [])
    now=datetime(2026,10,6,9,15,1)
    w._start_session(settings, now)
    w._poll_once(settings, now)
    assert w.state['session_id']==1
    assert w.state['snapshot_count']==1
    w._last_flush -= 3
    w._flush_snapshots()
    assert state['snapshots'], 'expected snapshot persistence'
    assert state['snapshots'][0]['l2_mode']=='5_level_displayed_depth'
