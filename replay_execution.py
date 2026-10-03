"""Replay execution rules; no strategy definitions or signal thresholds."""
def bar_exit(bar, target, stop, side='LONG'):
    high, low, op = float(bar['High']), float(bar['Low']), float(bar['Open'])
    short = side == 'SHORT'
    stop_hit = high >= stop if short else low <= stop
    target_hit = low <= target if short else high >= target
    if stop_hit:
        # A gap through an existing stop fills at the worse opening price.
        return 'STOP_LOSS_HIT', max(stop, op) if short else min(stop, op), ('BOTH_TOUCHED_SAME_BAR' if target_hit else 'STOP_ONLY')
    if target_hit:
        return 'TARGET_HIT', target, 'TARGET_ONLY'
    return None, None, None


def attributed_candidates(rows, independent):
    result = []
    for row in rows:
        candidates = row.get('strategy_independent_candidates', []) if independent else [row]
        for candidate in candidates:
            if candidate.get('strategy_id') and candidate.get('strategy_name'):
                result.append(candidate)
    return result
