from portfolio_engine import AllocationConfig, allocate_candidates, rank_candidates


def _c(ticker, sid, edge, notional):
    return {'ticker': ticker, 'strategy_id': sid, 'side': 'LONG',
            'expected_net_edge_pct': edge, 'strategy_confidence': 80,
            'id': f'{ticker}-{sid}', 'notional': notional}


def test_rank_is_net_edge_first():
    rows = rank_candidates([_c('A','s1',0.10,100000), _c('B','s2',0.40,100000)])
    assert [r['ticker'] for r in rows] == ['B','A']


def test_allocator_funds_best_edges_first_under_capital():
    cs = [_c('A','s1',0.40,120000), _c('B','s2',0.30,90000), _c('C','s3',0.20,70000)]
    plans = {(c['ticker'], c['strategy_id'], 'LONG'):{'estimated_value':c['notional'], 'quantity':1, 'limit_price':c['notional']} for c in cs}
    out = allocate_candidates(cs, plans, AllocationConfig(capital=200000))
    assert [x['candidate']['ticker'] for x in out['selected']] == ['A', 'C']
    assert any(x['ticker']=='B' and x['portfolio_reject_reason']=='capital_insufficient' for x in out['rejected'])


def test_allocator_rejects_missing_edge():
    c={'ticker':'A','strategy_id':'s1','side':'LONG','expected_net_edge_pct':None}
    p={('A','s1','LONG'):{'estimated_value':1000,'quantity':1,'limit_price':1000}}
    out=allocate_candidates([c],p,AllocationConfig(capital=200000))
    assert not out['selected']
    assert out['rejected'][0]['portfolio_reject_reason']=='missing_expected_net_edge'
