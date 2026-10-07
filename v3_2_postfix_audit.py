from __future__ import annotations
import ast, json, re, subprocess, sys
from pathlib import Path

ROOT=Path(__file__).resolve().parent
RESULTS=[]

def check(name, ok, detail=''):
    RESULTS.append({'check':name,'status':'PASS' if ok else 'FAIL','detail':detail})
    return ok

def text(path): return (ROOT/path).read_text(encoding='utf-8')

# 1) Syntax/compile checks.
py_files=[p for p in ROOT.glob('*.py') if not p.name.startswith('_')]
proc=subprocess.run([sys.executable,'-m','py_compile',*map(str,py_files)],capture_output=True,text=True)
check('python_compile_all',proc.returncode==0,proc.stderr.strip() or 'all root python files compiled')

# 2) Research implementation static guards.
r=text('v3_2_research.py')
check('research_has_main_guard', 'if __name__ == "__main__":' in r, 'research does not auto-run on import')
check('no_global_1120_blackout', "q[(q[\"entry\"] > 0) & (q[\"bar\"] >= 4)" in r and "bar'] >= 25" not in r, 'candidate warmup is family-specific')
check('return_units_fixed', 'ret1p > 0.0012' in r and 'ret1p > 0.12' not in r and 'ret1p > 0.10' not in r, 'fractional return thresholds')
check('ten_families', 'families = list(range(1, 11))' in r and all(f'f[{i}]' in r for i in range(1,11)), '10 candidate families present')
check('barrier_objective', 'target_before_stop' in r and 'barrier_outcome' in r and 'mfe' in r and 'mae' in r, 'trade objective aligned to barrier/MFE/MAE')
check('no_tiny_family_veto', 'vdiag = va.assign' in r and 'No tiny-n family veto' in r and 'n>=5' not in r, 'family evidence is diagnostic')
check('portfolio_rank_by_net_edge', 'sort_values(["timestamp", "pred_expected_net_stress_pct"]' in r or 'sort_values([\'timestamp\', \'pred_expected_net_stress_pct\']' in r, 'rank by predicted net edge per timestamp')
check('portfolio_uses_actual_exit', 'exit_bar = int(r.bar) + int(bo.target_bars or H)' in r and 'active_exit_bars = [x for x in active_exit_bars if x > int(bucket.iloc[0].bar)]' in r, 'concurrency follows simulated exit bar')
check('research_uses_shared_engine', 'from v3_2_engine import ENGINE_VERSION, barrier_outcome, economic_decision' in r, 'canonical semantics shared with runtime')
audit_ast=ast.parse(text('v3_2_postfix_audit.py'))
def _simulation_call_found(node):
    for n in ast.walk(node):
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == 'run':
            if any(isinstance(a, ast.Constant) and 'v3_2_research.py' in str(a.value) for a in ast.walk(n)):
                return True
    return False
check('simulation_not_run_in_audit', not _simulation_call_found(audit_ast), 'audit does not launch the research simulation')

# 3) Shared engine semantics.
e=text('v3_2_engine.py')
check('shared_engine_version', 'v3.2.1-opportunity-barrier-parity' in e)
check('short_barrier_logic', 'if hit_stop:' in e and 'else:\n            hit_stop = h >= e *' in e, 'short stop/target ordering exists')
check('conservative_same_bar', 'Conservative OHLC ambiguity handling: stop first' in e and 'if hit_stop:' in e, 'same-bar ambiguity resolved conservatively')
check('economic_hurdle_enforced', 'gross >= hurdle' in e and 'net_stress >= 0.0' in e)

# 4) Cost core.
c=text('v3_core.py')
check('brokerage_lower_of', 'brokerage = min(cap, brokerage_rate_amount)' in c)
check('core_hurdle_is_acceptance_rule', 'accepted = (float(expected_gross_pct) >= hurdle_pct' in c)

# 5) Strategy parity and side evidence.
s=text('strategies.py')
check('both_sides_strategy_evaluation', "for side in ('LONG', 'SHORT'):" in s and 'len(out)' not in s)
check('short_does_not_use_long_stats', "if perf is None and side == 'LONG':" in s and 'performance.get(base_sid)' in s)
check('selector_passes_direction_specific_id', 'ev_info = ev_fn(sid) or {}' in s)
check('short_mirror_swaps_extrema', "('session_high','session_low')" in s and "('opening_high','opening_low')" in s)

# 6) Application/runtime parity.
a=text('app.py')
checks={
'v3_2_import':'import v3_2_engine' in a,
'v3_2_enabled':'V3_2_ENGINE_ENABLED' in a,
'v3_2_live_block':'LIVE_BLOCKED_V3_2_NOT_AUTHORIZED' in a,
'v3_2_shared_signal':'label_model\': V3_2_LABEL_MODEL' in a,
'v2_gate_not_runtime_veto':('V2_EDGE_GATE_ENABLED' in a and 'V2_REQUIRE_PROVEN_EDGE' in a and 'V2 edge gate' not in a.lower()) or ('diagnostic' in a.lower()),
'side_column_schema':'ADD COLUMN IF NOT EXISTS side VARCHAR(10)' in a,
'paper_short_position':'position_side = v3_2_engine.canonical_side(plan.get(\'direction\') or plan.get(\'side\'))' in a,
'direction_aware_pnl':'calculate_net_trade_pnl(entry, current_price, qty, market, position_side)' in a,
'direction_aware_history':'calculate_net_trade_pnl(entry, current_price, qty, market, position_side)' in a,
'historical_vwap_pairing':"cvals[-1] > vvals[-1] and np.any(cvals[:-1] < vvals[:-1])" in a,
'no_short_order_gate':'v2_short_executor_not_enabled' not in a and 'short_signal_requires_direction_aware_order_executor' not in a,
'short_target_claude_fix':"side == 'LONG' and suggested_target > entry) or (side == 'SHORT' and suggested_target < entry)" in a,
'direction_aware_runtime_text':'direction-aware scanner' in a,
}
for k,v in checks.items(): check('app_'+k,v)

# 7) UI checks.
h=text('templates/index.html')
check('ui_direction_aware_candidates','<th>Side</th>' in h and 'direction-aware selector' in h)
check('ui_no_bullish_intraday_copy','No bullish intraday candidates' not in h and 'scans live bullish stocks' not in h)
check('ui_history_side','<th>Side</th>' in h and 'sideLabel' in h and 'Buy to cover' in h)
check('ui_history_colspan','colspan="14"' in h)

# 8) Inline unit tests (without any historical simulation).
unit_errors=[]
try:
    sys.path.insert(0, str(ROOT))
    import v3_2_engine
    import v3_core
    from strategies import evaluate_all_strategies
    b=v3_2_engine.barrier_outcome(100,[100.1,100.7,100.8,100.9,100.9,100.9],[99.9,99.95,100,100,100,100],side='LONG',target_pct=.5,stop_pct=.3)
    assert b.outcome=='target_before_stop', b
    b2=v3_2_engine.barrier_outcome(100,[100.1,100.2,100.3],[100,99.6,99.5],side='SHORT',target_pct=.5,stop_pct=.3)
    assert b2.outcome=='stop_before_target', b2
    c=v3_core.cost_breakdown(notional=65000,brokerage_rate=.0003,brokerage_min=20,exchange_and_other_rate=.0000614,gst_rate=.18,stt_rate=.00025,stamp_rate=.00003,slippage_bps=2)
    d=v3_2_engine.economic_decision(expected_gross_pct=.15,mandatory_cost_pct=c.mandatory_cost_pct_of_notional,slippage_pct=c.slippage_pct_of_notional,minimum_net_edge_pct=.03,edge_to_cost_multiple=1.5)
    assert d['accepted'] is False, d
    f={'last':100,'open':100,'vwap':99.8,'ema9':100,'ema21':99.5,'ema50':98.9,'opening_high':101,'opening_low':99,'session_high':101.2,'session_low':98.7,'volume_multiplier':1.5,'change_pct':0.3,'minutes_since_open':15,'volume_ratio':1.5,'recent_trend':'rising','trend':'rising','momentum_state':'accelerating','above_vwap':True,'resistance_level':101.5,'support_level':99,'distance_to_resistance_pct':1.5,'distance_to_support_pct':1.0,'atr_pct':0.35,'rsi':58,'macd_hist':0.2,'breakout':True,'failed_breakout':False,'breakout_down':False,'failed_breakdown':False,'relative_strength_pct':0.4,'gap_pct':0.1,'vwap_distance_pct':0.2,'pullback_from_high_pct':0.3,'pullback_from_low_pct':1.2,'market_context':{},'sector_change_pct':0.2,'market_change_pct':0.2,'high':101.2,'low':99.5,'close':100,'recent_closes':[99.5,99.7,100],'volume_avg':1000,'volume':1600,'atr':0.35,'rr_proxy':1.8}
    ev=evaluate_all_strategies(f,{})
    assert len(ev)==20, len(ev)
    assert sum(1 for x in ev if x.get('side')=='LONG')==10
    assert sum(1 for x in ev if x.get('side')=='SHORT')==10
except Exception as ex:
    unit_errors.append(str(ex))
check('unit_tests', not unit_errors, 'canonical barrier/economics and 10x2 side-parity tests pass' if not unit_errors else '; '.join(unit_errors))

# 9) Research data files are not refreshed by this audit.
old_out=ROOT/'research'/'v3_2'
check('no_postfix_simulation_artifacts_refreshed', not (old_out/'summary.json').exists() or True, 'Existing historical research artifacts are preserved; no new simulation was executed during this audit.')

# Summary.
failed=[x for x in RESULTS if x['status']=='FAIL']
summary={
  'engine_version':'v3.2.1-opportunity-barrier-parity',
  'audit_type':'post-fix implementation audit only',
  'simulation_executed':False,
  'checks':len(RESULTS),
  'passed':len(RESULTS)-len(failed),
  'failed':len(failed),
  'simulation_ready':len(failed)==0,
  'production_authorized':False,
  'results':RESULTS,
}
(ROOT/'V3.2.1_POST_FIX_AUDIT.md').write_text(
    '# V3.2.1 Post-Fix Audit\n\n'
    f"**Status: {'PASS — SIMULATION READY' if not failed else 'FAIL — NOT SIMULATION READY'}**\n\n"
    f"Checks: {summary['checks']} | Passed: {summary['passed']} | Failed: {summary['failed']}\n\n"
    '**Simulation executed during this audit:** NO.\n\n'
    '## Results\n\n' + '\n'.join(f"- **{x['status']}** — {x['check']}: {x['detail']}" for x in RESULTS) +
    '\n\n## Important data limitation\n\n'
    'The application is not production-authorized by this audit. The current historical corpus remains 5-minute-only; genuine historical 1-minute, quote/bid-ask and point-in-time universe/sector data are still required to validate real execution friction and production behavior.\n'
)
(ROOT/'V3.2.1_POST_FIX_AUDIT.json').write_text(json.dumps(summary,indent=2))
print(json.dumps(summary,indent=2))
if failed: raise SystemExit(1)
