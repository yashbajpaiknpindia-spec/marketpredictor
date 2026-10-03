"""Shared candidate ranking and finite-capital portfolio allocation.

This module is deliberately pure: no Flask, database, broker, or market-data calls.
Replay, paper, and live feed it the same candidate/plan objects so the decision rules
cannot drift between execution environments.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple


@dataclass(frozen=True)
class AllocationConfig:
    capital: float
    max_positions: Optional[int] = None
    reserve_pct: float = 0.0
    minimum_edge_pct: float = 0.0


def _f(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
        return x if x == x else default
    except (TypeError, ValueError):
        return default


def candidate_edge(candidate: Dict[str, Any]) -> Optional[float]:
    """Return the candidate's historical/context-conditioned NET edge."""
    value = candidate.get("expected_net_edge_pct")
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def rank_candidates(candidates: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Stable highest-net-edge-first ranking.

    Confidence is only a secondary tie-breaker. A high-confidence trade with a
    negative/unknown economic edge must not outrank a positive-net-edge candidate.
    """
    rows = [dict(c) for c in candidates]
    rows.sort(key=lambda c: (
        1 if candidate_edge(c) is not None else 0,
        candidate_edge(c) if candidate_edge(c) is not None else float("-inf"),
        _f(c.get("strategy_confidence"), _f(c.get("score"))),
        str(c.get("strategy_id") or ""),
        str(c.get("ticker") or ""),
    ), reverse=True)
    for i, row in enumerate(rows, 1):
        row["portfolio_rank"] = i
    return rows


def allocate_candidates(
    candidates: Iterable[Dict[str, Any]],
    plans_by_key: Dict[Tuple[str, str, str], Dict[str, Any]],
    config: AllocationConfig,
    existing_notional: float = 0.0,
    existing_positions: int = 0,
) -> Dict[str, Any]:
    """Allocate finite capital chronologically/atomically over ranked candidates.

    `plans_by_key` contains already validated execution plans. Capital is reserved
    using order notional, while expected edge is used only for ranking. This prevents
    the allocator from pretending that an excellent signal can be funded twice.
    """
    ranked = rank_candidates(candidates)
    reserve = max(0.0, min(0.99, _f(config.reserve_pct) / 100.0))
    usable = max(0.0, _f(config.capital) * (1.0 - reserve))
    used = max(0.0, _f(existing_notional))
    open_count = max(0, int(existing_positions or 0))
    selected: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []
    seen_keys = set()

    for candidate in ranked:
        sid = str(candidate.get("strategy_id") or "")
        ticker = str(candidate.get("ticker") or "").upper()
        side = str(candidate.get("side") or candidate.get("direction") or "LONG").upper()
        key = (ticker, sid, side)
        if key in seen_keys:
            candidate["portfolio_reject_reason"] = "duplicate_candidate"
            rejected.append(candidate)
            continue
        seen_keys.add(key)

        edge = candidate_edge(candidate)
        if edge is None:
            candidate["portfolio_reject_reason"] = "missing_expected_net_edge"
            rejected.append(candidate)
            continue
        if edge <= _f(config.minimum_edge_pct):
            candidate["portfolio_reject_reason"] = "expected_net_edge_below_portfolio_floor"
            rejected.append(candidate)
            continue
        if config.max_positions is not None and open_count >= int(config.max_positions):
            candidate["portfolio_reject_reason"] = "portfolio_position_limit"
            rejected.append(candidate)
            continue

        plan = plans_by_key.get(key)
        if not plan:
            candidate["portfolio_reject_reason"] = "execution_plan_unavailable"
            rejected.append(candidate)
            continue
        notional = abs(_f(plan.get("estimated_value"), _f(plan.get("quantity")) * _f(plan.get("limit_price"))))
        if notional <= 0:
            candidate["portfolio_reject_reason"] = "zero_notional"
            rejected.append(candidate)
            continue
        if used + notional > usable + 1e-9:
            candidate["portfolio_reject_reason"] = "capital_insufficient"
            candidate["portfolio_required_capital"] = round(notional, 2)
            candidate["portfolio_available_capital"] = round(max(0.0, usable - used), 2)
            rejected.append(candidate)
            continue

        candidate["portfolio_selected"] = True
        candidate["portfolio_allocated_notional"] = round(notional, 2)
        candidate["portfolio_available_after"] = round(max(0.0, usable - used - notional), 2)
        selected.append({"candidate": candidate, "plan": plan})
        used += notional
        open_count += 1

    return {
        "capital": round(_f(config.capital), 2),
        "usable_capital": round(usable, 2),
        "existing_notional": round(max(0.0, _f(existing_notional)), 2),
        "allocated_notional": round(max(0.0, used - max(0.0, _f(existing_notional))), 2),
        "available_capital": round(max(0.0, usable - used), 2),
        "selected": selected,
        "rejected": rejected,
        "ranked_candidates": ranked,
    }
