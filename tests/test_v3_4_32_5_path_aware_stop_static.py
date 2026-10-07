from pathlib import Path
import ast

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / 'app.py').read_text(encoding='utf-8')
RAPID = (ROOT / 'rapid_forensics.py').read_text(encoding='utf-8')
HTML = (ROOT / 'templates' / 'index.html').read_text(encoding='utf-8')


def _func(src, name):
    tree = ast.parse(src)
    return next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)


def test_path_aware_replay_helper_exists_and_is_used():
    _func(APP, 'resolve_path_aware_replay_bar_exit')
    assert 'O→L→H→C' in APP
    assert 'O→H→L→C' in APP
    assert APP.count('resolve_path_aware_replay_bar_exit(') >= 3


def test_protection_can_lock_true_net_break_even_after_activation():
    assert 'protection_target_net = min_net_to_protect if research_trail_only else 0.0' in APP
    assert '_minimum_net_protection_price(entry, qty, market_code, position_side, protection_target_net)' in APP
    assert 'path-aware-v2' in APP


def test_live_exit_uses_threshold_fill_instead_of_late_current_price():
    assert 'recommended_exit_price' in APP
    assert "recommended_exit_price_source': 'threshold'" in APP
    assert "exit_price = float(meta.get('recommended_exit_price') or snap.get('last') or last)" in APP
    assert "'interval_seconds': 5" in APP


def test_rapid_stop_is_cooperative_and_never_rebuilds_tables_during_stop_wait():
    assert 'stop_check=_rapid_should_stop' in APP
    assert 'if r.get(\'aborted\'):' in APP
    assert 'stop_requested' in APP
    assert 'rapidStopWatch(runId)' in HTML
    assert 'clearInterval(rapidForensicsPollTimer)' in HTML
    assert 'The UI will wait for the final checkpoint without rebuilding the result tables.' in HTML


def test_rapid_forensics_has_no_synthetic_generator_and_declares_real_only():
    assert 'synthetic_ticker_day' not in RAPID
    assert '_synthetic_regime_params' not in RAPID
    assert '_synthetic_intraday_context' not in RAPID
    assert 'REAL-data-only' in APP
    assert 'VERIFIED_REAL_DATASET_ONLY' in APP


def test_intrabar_path_order_and_same_bar_protection_can_be_exercised_without_flask():
    """Execute the small historical path resolver in isolation with a stub exit engine.

    This keeps the regression test dependency-light: the container does not need the
    full Flask/Postgres stack just to verify that a +0.40% activation followed by a
    reversal in the SAME candle is actually processed in sequence.
    """
    tree = ast.parse(APP)
    wanted = {'_path_model_points', '_path_snapshot_for_point', 'resolve_path_aware_replay_bar_exit'}
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in wanted]
    module = ast.Module(body=nodes, type_ignores=[])
    from typing import Optional, Tuple, List, Dict, Any
    import datetime
    ns = {'Optional': Optional, 'Tuple': Tuple, 'List': List, 'Dict': Dict, 'Any': Any, 'math': __import__('math'), 'datetime': datetime}
    # Keep annotations from requiring runtime typing objects in this miniature harness.
    import textwrap
    code = compile(module, '<path_helpers>', 'exec')
    exec(code, ns)

    def side(pos):
        return 'LONG'

    def exit_stub(pos, settings, snap, **kwargs):
        entry = float(pos['entry_price'])
        last = float(snap['last'])
        armed = bool(pos.get('_stub_armed'))
        if not armed and last >= entry * 1.004:
            pos['_stub_armed'] = True
            stop = entry * 1.002
            return None, {'last': last, 'peak': last, 'trough': min(entry, last),
                          'trailing_stop_price': stop, 'profit_protection_armed': True}
        if armed and last <= entry * 1.002:
            stop = entry * 1.002
            return 'TRAILING_PROFIT_PROTECT', {'last': last, 'peak': pos.get('peak_price', last),
                          'trough': min(pos.get('trough_price', entry), last),
                          'trailing_stop_price': stop, 'profit_protection_armed': True}
        return None, {'last': last, 'peak': max(pos.get('peak_price', entry), last),
                      'trough': min(pos.get('trough_price', entry), last),
                      'trailing_stop_price': pos.get('trailing_stop_price'), 'profit_protection_armed': armed}

    ns['_position_side'] = side
    ns['evaluate_profit_protection_exit'] = exit_stub
    bar = {'Open': 100.0, 'High': 101.0, 'Low': 99.0, 'Close': 100.1}
    pts, model = ns['_path_model_points'](bar, 100.0)
    assert model == 'OPEN_LOW_HIGH_CLOSE'
    assert pts == [100.0, 99.0, 101.0, 100.1]

    pos = {'entry_price':100.0, 'target_price':110.0, 'stop_price':98.0,
           'quantity':1, 'side':'LONG', 'peak_price':100.0, 'trough_price':100.0,
           'trailing_stop_price':98.0}
    reason, fill, meta, _ = ns['resolve_path_aware_replay_bar_exit'](
        pos, bar, {}, sim_now=None, ticker='TEST', day_df=None, bar_pos=0)
    # The modeled path reaches +1% and then falls through the newly armed +0.20% trail
    # within the same candle; that exit must be recognized rather than waiting for the close.
    assert reason == 'TRAILING_PROFIT_PROTECT'
    assert round(fill, 3) == 100.2
    assert meta['path_exit_step'] == 3
