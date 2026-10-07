"""Behavioral regression tests without Flask/database/network startup."""
import ast
import datetime
import math
import re
import sys
from pathlib import Path
from typing import *
import numpy as np
import pandas as pd
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import strategies
import sector_data
import v3_2_engine
from replay_execution import bar_exit, attributed_candidates

def harness():
    ns = dict(globals())
    ns.update({n: getattr(strategies,n) for n in dir(strategies) if not n.startswith('__')})
    ns.update(INTRADAY_DATA_INTERVAL='1m', INTRADAY_MULTI_STRATEGY_ENABLED=True,
              INTRADAY_DYNAMIC_UNIVERSE_ENABLED=True, INTRADAY_FAST_ADAPT_ENABLED=False,
              INTRADAY_DAILY_RISK_BUDGET_PCT=2, INTRADAY_MAX_CONCURRENT_RISK_PCT=1,
              INTRADAY_RISK_BUDGET_RESERVE_PCT=40, INTRADAY_MAX_STOP_LOSS_PCT=.6,
              INTRADAY_MIN_STOP_LOSS_PCT=.2, INTRADAY_MIN_RISK_REWARD=1.75, INTRADAY_QUICK_TARGET_PCT=.8)
    names = {'fetch_intraday_candidate_snapshot','build_replay_candidate_row','replay_scan_universe',
             'attach_intraday_strategy_engine','extract_intraday_strategy_features',
             '_attach_relative_strength_feature','_attach_support_resistance_features','_attach_sector_context',
             '_intraday_strategy_execution_mode','_strategy_tournament_candidate','_safe_float',
             'score_intraday_prescan_candidate','apply_intraday_price_focus_filter','normalize_history_frame',
             'compute_ema','compute_rsi','compute_macd','get_intraday_interval_minutes',
             'IntradayRiskBudget','compute_strategy_recording_integrity','build_intraday_backtest_snapshot',
             'compute_intraday_risk_plan','_candidate_side'}
    tree = ast.parse((ROOT/'app.py').read_text())
    for node in tree.body:
        if isinstance(node,(ast.FunctionDef,ast.ClassDef)) and node.name in names:
            exec(compile(ast.Module(body=[node],type_ignores=[]),str(ROOT/'app.py'),'exec'),ns)
    return ns

def test_stop_direction_and_price():
    assert bar_exit({'Open':101,'High':102,'Low':100},103,100.5)==('STOP_LOSS_HIT',100.5,'STOP_ONLY')
    assert bar_exit({'Open':99,'High':102,'Low':98},103,100.5)[1]==99
    assert bar_exit({'Open':100,'High':101,'Low':98},98,102,'SHORT')==('TARGET_HIT',98,'TARGET_ONLY')
    assert bar_exit({'Open':104,'High':105,'Low':98},98,102,'SHORT')==('STOP_LOSS_HIT',104,'BOTH_TOUCHED_SAME_BAR')

def test_no_snapshot_can_become_a_trade():
    assert attributed_candidates([{'ticker':'ABC','last':100}],True)==[]
    assert attributed_candidates([{'ticker':'ABC','last':100}],False)==[]

def test_real_strategy_scan_and_no_future_data():
    ns=harness()
    idx=pd.date_range('2026-09-21 09:15',periods=65,freq='min')
    close=np.linspace(100,100.3,65); close[40:]=np.linspace(100.35,101,25)
    df=pd.DataFrame({'Open':close-.02,'High':close+.025,'Low':close-.025,'Close':close,'Volume':[1000]*40+[3000]*25},index=idx)
    settings={'market':'IN','_strategy_raw_mode':True,'max_candidates':1,'_sim_now':idx[50].to_pydatetime()}
    result=ns['replay_scan_universe']({'ABC':{'df':df,'idx':50}},['ABC'],settings,'IN',{}, {},set(),False,idx[50].to_pydatetime())
    assert sum(a['evaluated'] for a in result['strategy_audit'].values()) == 40, result
    assert result['eligible'], result
    assert all(c.get('strategy_id') and c.get('strategy_name') for c in result['eligible'])
    assert all(c.get('strategy_target_pct') and c.get('strategy_stop_pct') for c in result['eligible'])
    df2=df.copy();df2.iloc[51:,df2.columns.get_loc('Close')]=10000
    result2=ns['replay_scan_universe']({'ABC':{'df':df2,'idx':50}},['ABC'],settings,'IN',{}, {},set(),False,idx[50].to_pydatetime())
    assert result['strategy_audit']==result2['strategy_audit']
    assert [(c['strategy_id'],c['last']) for c in result['eligible']]==[(c['strategy_id'],c['last']) for c in result2['eligible']]

def test_budget_and_historical_attribution():
    ns=harness(); b=ns['IntradayRiskBudget'](200000,2,1,40,True)
    b.commit(1500);assert not b.check(600)['ok']; b.release(1500); assert b.check(600)['ok']
    b.commit(2000); b.release(2000);assert not b.check(600)['ok']
    integrity=ns['compute_strategy_recording_integrity']([{'strategy_id':None}],{}, {})
    assert integrity['status']=='ATTRIBUTION_GAP'

def test_wide_opening_range_cannot_override_risk_ceiling():
    ns=harness()
    for side in ('LONG','SHORT'):
        c={'last':100,'side':side,'strategy_id':'orb_15','strategy_target_pct':.8,
           'strategy_stop_pct':.5,'opening_high':110,'opening_low':90}
        plan=ns['compute_intraday_risk_plan'](c,{'max_stop_loss_pct':.6})
        assert abs(plan['entry']-plan['stop']) <= .50001, plan

if __name__=='__main__':
    for name, fn in list(globals().items()):
        if name.startswith('test_'):
            fn();print('PASS',name)
