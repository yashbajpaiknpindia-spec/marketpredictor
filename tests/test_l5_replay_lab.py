import pandas as pd
from research.scalper.l5_replay_lab import L5ReplayConfig, l5_signal_summary, replay_recorded_decisions, make_empirical_l5_library, empirical_proxy_contract


def sample_rows():
    rows=[]
    for sec in range(0,121,10):
        for ticker,px,side in [('A',100+0.03*sec/10,1),('B',100-0.03*sec/10,-1)]:
            rows.append({
                'session_id': 1, 'captured_at': f'2026-10-07 10:{sec//60:02d}:{sec%60:02d}',
                'ticker':ticker,'ltp':px,'volume':1000+sec,'spread_pct':0.01,
                'imbalance_l1':0.6*side,'imbalance_l5':0.5*side,'microprice_edge_pct':0.01*side,
                'ofi_proxy':0.1*side,'book_pressure':0.4*side,'depth_total_qty':1000,
                'l2_pass':True,'signal_direction':side,'edge_pass':True,'score_pass':True,
                'remaining_edge_pct':0.05,'signal_confidence':90,'raw_direction':side,'raw_confidence':90,
                'bid_prices':[99.9]*5,'bid_qtys':[100]*5,'ask_prices':[100.1]*5,'ask_qtys':[100]*5
            })
    return pd.DataFrame(rows)


def test_calibration_and_proxy_contract():
    s=sample_rows()
    r=l5_signal_summary(s)
    assert r['ok'] and r['rows'] == len(s)
    lib=make_empirical_l5_library(s)
    assert len(lib)==len(s) and 'depth_log' in lib
    c=empirical_proxy_contract(); assert c['evidence_class']=='EMPIRICAL_L5_PROXY' and not c['historical_l5_truth']


def test_exact_replay_capital_constrained():
    s=sample_rows()
    cfg=L5ReplayConfig(capital_inr=200000,position_notional_inr=200000,max_open_positions=1,min_net_edge_pct=0.0)
    r=replay_recorded_decisions(s,cfg)
    assert r['ok'] and r['evidence_class']=='TRUE_L5_REPLAY'
    assert r['metrics']['max_open_positions']==1
