"""Canonical V3.2.1 semantics shared by research and application runtime.

This module is deliberately dependency-light.  It owns the pieces that must not
silently diverge between backtest/replay/paper/live: side normalization, barrier
outcomes, directional risk prices, and economic hurdle semantics.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, Iterable, Optional, Sequence

ENGINE_VERSION = "v3.2.1-opportunity-barrier-parity"


def canonical_side(value: Any, default: str = "LONG") -> str:
    s = str(value or default).strip().upper()
    if s in {"SELL", "SHORT", "S", "BEAR", "BEARISH"}:
        return "SHORT"
    return "LONG"


def order_side(value: Any, default: str = "LONG") -> str:
    return "SELL" if canonical_side(value, default) == "SHORT" else "BUY"


def gross_return_pct(entry: float, exit_price: float, side: Any = "LONG") -> float:
    e = float(entry or 0.0)
    x = float(exit_price or 0.0)
    if e <= 0:
        return 0.0
    return ((x - e) / e * 100.0) if canonical_side(side) == "LONG" else ((e - x) / e * 100.0)


@dataclass(frozen=True)
class BarrierOutcome:
    outcome: str
    mfe_pct: float
    mae_pct: float
    target_before_stop: bool
    stop_before_target: bool
    target_bars: Optional[int]
    stop_bars: Optional[int]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def barrier_outcome(
    entry: float,
    highs: Sequence[float],
    lows: Sequence[float],
    *,
    side: Any = "LONG",
    target_pct: float = 0.5,
    stop_pct: float = 0.3,
) -> BarrierOutcome:
    """Evaluate a path using conservative same-bar sequencing: stop wins when
    both stop and target are touched in the same bar because OHLC alone does not
    reveal intrabar order.  Real 1m/tick data can later replace this ambiguity.
    """
    e = float(entry or 0.0)
    if e <= 0 or len(highs) != len(lows) or not highs:
        return BarrierOutcome("invalid", 0.0, 0.0, False, False, None, None)
    side_c = canonical_side(side)
    t = abs(float(target_pct or 0.0)) / 100.0
    s = abs(float(stop_pct or 0.0)) / 100.0
    favorable = []
    adverse = []
    for h, l in zip(highs, lows):
        h = float(h)
        l = float(l)
        if side_c == "LONG":
            favorable.append((h / e - 1.0) * 100.0)
            adverse.append((l / e - 1.0) * 100.0)
        else:
            favorable.append((e / l - 1.0) * 100.0 if l > 0 else 0.0)
            adverse.append((e / h - 1.0) * 100.0 if h > 0 else 0.0)

    mfe = max(0.0, max(favorable)) if favorable else 0.0
    mae = min(0.0, min(adverse)) if adverse else 0.0
    target_bars = stop_bars = None
    outcome = "timeout"
    for i, (h, l) in enumerate(zip(highs, lows), start=1):
        if side_c == "LONG":
            hit_stop = l <= e * (1.0 - s)
            hit_target = h >= e * (1.0 + t)
        else:
            hit_stop = h >= e * (1.0 + s)
            hit_target = l <= e * (1.0 - t)
        # Conservative OHLC ambiguity handling: stop first when both are touched.
        if hit_stop:
            stop_bars = i
            outcome = "stop_before_target"
            break
        if hit_target:
            target_bars = i
            outcome = "target_before_stop"
            break
    return BarrierOutcome(
        outcome=outcome,
        mfe_pct=round(mfe, 6),
        mae_pct=round(mae, 6),
        target_before_stop=outcome == "target_before_stop",
        stop_before_target=outcome == "stop_before_target",
        target_bars=target_bars,
        stop_bars=stop_bars,
    )


def directional_prices(entry: float, stop_pct: float, target_pct: float, side: Any = "LONG") -> Dict[str, float]:
    e = float(entry or 0.0)
    sp = abs(float(stop_pct or 0.0)) / 100.0
    tp = abs(float(target_pct or 0.0)) / 100.0
    side_c = canonical_side(side)
    if side_c == "LONG":
        return {"entry": e, "stop": e * (1.0 - sp), "target": e * (1.0 + tp)}
    return {"entry": e, "stop": e * (1.0 + sp), "target": e * (1.0 - tp)}


def minimum_gross_hurdle_pct(
    mandatory_cost_pct: float,
    *,
    minimum_net_edge_pct: float = 0.03,
    edge_to_cost_multiple: float = 1.5,
) -> float:
    cost = max(0.0, float(mandatory_cost_pct or 0.0))
    min_edge = max(0.0, float(minimum_net_edge_pct or 0.0))
    multiple = max(0.0, float(edge_to_cost_multiple or 0.0))
    # Require costs back plus a safety cushion equal to N x mandatory costs,
    # while never accepting less than the absolute minimum net-edge floor.
    return max(min_edge, cost * (1.0 + multiple))


def economic_decision(
    expected_gross_pct: float,
    mandatory_cost_pct: float,
    *,
    slippage_pct: float = 0.0,
    minimum_net_edge_pct: float = 0.03,
    edge_to_cost_multiple: float = 1.5,
    require_hurdle: bool = True,
) -> Dict[str, Any]:
    gross = max(0.0, float(expected_gross_pct or 0.0))
    cost = max(0.0, float(mandatory_cost_pct or 0.0))
    slip = max(0.0, float(slippage_pct or 0.0))
    hurdle = minimum_gross_hurdle_pct(cost, minimum_net_edge_pct=minimum_net_edge_pct, edge_to_cost_multiple=edge_to_cost_multiple)
    net_before = gross - cost
    net_stress = net_before - slip
    accepted = (gross >= hurdle if require_hurdle else net_before >= minimum_net_edge_pct) and net_stress >= 0.0
    return {
        "expected_gross_pct": gross,
        "mandatory_cost_pct": cost,
        "slippage_pct": slip,
        "economic_hurdle_pct": hurdle,
        "expected_net_before_slippage_pct": net_before,
        "expected_net_under_stress_pct": net_stress,
        "accepted": bool(accepted),
        "reason": "economic_edge_cleared" if accepted else "economic_hurdle_not_cleared",
    }
