"""UltraScalp synthetic L5 market laboratory, v2.

This is a controlled research environment, not historical NSE data.  The
market generator exposes only observable information to the engine: LTP,
volume, spread and five displayed bid/ask levels.  Hidden flow, true regime
state and future returns are generator-only variables and are never included
in the model feature vector.

The lab is deliberately harder than the earlier toy generator:
- 25 market/event regimes, including no-edge and deceptive regimes.
- Dynamic L5 queues with cancellations, replenishment and aggressive flow.
- Variable spread/liquidity, transient spoof-like imbalance and real imbalance.
- Session-randomized noise, persistence, volatility, depth and flow strength.
- Causal train/validation/test split by whole sessions.
- Validation-only threshold selection; the final test is frozen/blind.
- Point-in-time entry with configurable latency, spread, slippage and friction.
- Edge-based entries/exits; there is no fixed +0.60% target requirement.
- Per-regime test metrics, no-edge participation and latency robustness.

A positive synthetic result is architecture evidence only. It must not be
presented as real-market performance or used to authorize live trading.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple
import math
import os
import json
import threading
import time

import numpy as np
import pandas as pd


# Broad market-condition coverage.  Regime is metadata for evaluation only;
# it is not exposed to the model.
REGIMES: Tuple[str, ...] = (
    "no_edge",
    "weak_edge",
    "moderate_edge",
    "strong_edge",
    "bull_trend",
    "bear_trend",
    "range",
    "chop",
    "trend_accel",
    "trend_decel",
    "vol_expand",
    "vol_compress",
    "reversal",
    "false_breakout",
    "spoof_imbalance",
    "liquidity_shock",
    "open_shock",
    "midday_drift",
    "close_accel",
    "gap_follow",
    "gap_fade",
    "orderflow_flip",
    "thin_book",
    "wide_spread",
    "cross_current",
)

# Every field must be derivable from the visible stream at the current bar.
FEATURES: Tuple[str, ...] = (
    "imbalance_l1",
    "imbalance_l3",
    "imbalance_l5",
    "weighted_imbalance",
    "microprice_edge_pct",
    "imbalance_change",
    "depth_change_pct",
    "spread_pct",
    "ret_1_pct",
    "ret_3_pct",
    "ret_8_pct",
    "realized_vol_pct",
    "volume_z",
    "aggressive_flow",
    "aggressive_flow_change",
    "book_pressure_change",
    "depth_stress",
    "queue_asymmetry",
    "sweep_proxy",
)

EVIDENCE_CLASS = "SYNTHETIC_L5_CONTROLLED_LAB_V2"


@dataclass(frozen=True)
class SyntheticConfig:
    sessions_per_regime: int = 6
    bars_per_session: int = 360
    train_fraction: float = 0.60
    validation_fraction: float = 0.20
    seed: int = 20261007
    levels: int = 5
    stock_count: int = 40

    # Execution economics.
    round_trip_cost_pct: float = 0.1363
    entry_slippage_pct: float = 0.015
    adverse_selection_pct: float = 0.020
    min_net_edge_pct: float = 0.015
    min_probability: float = 0.55
    max_hold_bars: int = 10
    protection_pct: float = 0.18
    max_spread_pct: float = 0.10
    latency_bars: int = 1

    # Robustness / model controls.
    include_no_edge: bool = True
    tune_thresholds_on_validation: bool = True
    min_validation_trades: int = 25
    future_horizon_bars: int = 5
    session_start_price: float = 100.0


@dataclass(frozen=True)
class _SessionParams:
    spread_base_pct: float
    depth_scale: float
    volatility: float
    flow_persistence: float
    edge_strength: float
    impact_strength: float
    cancel_rate: float
    replenish_rate: float
    volume_scale: float


# The tuple is (drift, volatility, edge_strength, persistence, impact_scale,
# spread multiplier, depth multiplier).  These are *generator* parameters.
REGIME_PARAMS: Dict[str, Tuple[float, float, float, float, float, float, float]] = {
    "no_edge": (0.00000, 0.00100, 0.00, 0.18, 0.00, 1.00, 1.00),
    "weak_edge": (0.00005, 0.00105, 0.35, 0.55, 0.35, 1.00, 1.00),
    "moderate_edge": (0.00008, 0.00110, 0.65, 0.68, 0.65, 1.00, 1.00),
    "strong_edge": (0.00010, 0.00110, 0.95, 0.78, 0.95, 0.98, 1.05),
    "bull_trend": (0.00055, 0.00130, 0.75, 0.72, 0.75, 0.98, 1.00),
    "bear_trend": (-0.00055, 0.00130, 0.75, 0.72, 0.75, 0.98, 1.00),
    "range": (0.00000, 0.00055, 0.35, 0.42, 0.28, 1.02, 1.10),
    "chop": (0.00000, 0.00105, 0.18, 0.15, 0.12, 1.08, 0.95),
    "trend_accel": (0.00035, 0.00175, 0.85, 0.80, 0.90, 1.00, 0.95),
    "trend_decel": (0.00005, 0.00120, 0.45, 0.50, 0.45, 1.02, 1.00),
    "vol_expand": (0.00005, 0.00210, 0.70, 0.62, 0.70, 1.04, 0.82),
    "vol_compress": (0.00002, 0.00040, 0.40, 0.60, 0.35, 0.98, 1.15),
    "reversal": (0.00000, 0.00145, 0.70, 0.76, 0.72, 1.04, 0.92),
    "false_breakout": (0.00000, 0.00145, 0.48, 0.48, 0.25, 1.06, 0.90),
    "spoof_imbalance": (0.00000, 0.00110, 0.25, 0.35, 0.12, 1.05, 0.98),
    "liquidity_shock": (0.00000, 0.00240, 0.58, 0.45, 0.65, 1.15, 0.45),
    "open_shock": (0.00020, 0.00280, 0.72, 0.70, 0.78, 1.15, 0.65),
    "midday_drift": (0.00000, 0.00032, 0.22, 0.56, 0.16, 1.00, 1.20),
    "close_accel": (0.00030, 0.00195, 0.74, 0.74, 0.82, 1.08, 0.88),
    "gap_follow": (0.00035, 0.00185, 0.78, 0.76, 0.80, 1.12, 0.82),
    "gap_fade": (-0.00010, 0.00180, 0.65, 0.70, 0.62, 1.12, 0.82),
    "orderflow_flip": (0.00000, 0.00160, 0.76, 0.84, 0.80, 1.04, 0.90),
    "thin_book": (0.00000, 0.00165, 0.60, 0.60, 0.72, 1.12, 0.45),
    "wide_spread": (0.00000, 0.00125, 0.62, 0.58, 0.62, 1.75, 0.82),
    "cross_current": (0.00000, 0.00115, 0.40, 0.28, 0.24, 1.10, 0.95),
}


def _clip01(x: float) -> float:
    return float(np.clip(x, 0.0, 1.0))


def _make_session_params(rng: np.random.Generator, regime: str) -> _SessionParams:
    drift, vol, edge, persistence, impact, spread_mult, depth_mult = REGIME_PARAMS[regime]
    return _SessionParams(
        spread_base_pct=float(np.clip(0.018 * spread_mult * rng.lognormal(0.0, 0.18), 0.006, 0.18)),
        depth_scale=float(np.clip(7800.0 * depth_mult * rng.lognormal(0.0, 0.20), 1800.0, 15000.0)),
        volatility=float(np.clip(vol * rng.lognormal(0.0, 0.18), 0.00020, 0.0038)),
        flow_persistence=float(np.clip(persistence + rng.normal(0, 0.05), 0.05, 0.95)),
        edge_strength=float(np.clip(edge + rng.normal(0, 0.08), 0.0, 1.0)),
        impact_strength=float(np.clip(impact * rng.lognormal(0.0, 0.15), 0.0, 1.2)),
        cancel_rate=float(np.clip(0.05 + rng.uniform(0.0, 0.09) + (0.05 if regime in {"spoof_imbalance", "liquidity_shock", "thin_book"} else 0), 0.02, 0.30)),
        replenish_rate=float(np.clip(0.10 + rng.uniform(0.0, 0.16) - (0.05 if regime in {"liquidity_shock", "thin_book"} else 0), 0.03, 0.45)),
        volume_scale=float(np.clip(rng.lognormal(0.0, 0.25), 0.40, 2.80)),
    )


def _regime_is_edge_bearing(regime: str) -> bool:
    return regime not in {"no_edge", "chop", "spoof_imbalance", "cross_current"}


def _regime_flow_sign(regime: str, i: int, n: int) -> float:
    """Macro/session directional tendency used only by the market generator."""
    if regime in {"bull_trend", "trend_accel", "close_accel", "gap_follow", "strong_edge", "moderate_edge", "weak_edge"}:
        return 1.0
    if regime == "bear_trend":
        return -1.0
    if regime == "gap_fade":
        return -1.0 if i < n * 0.45 else 0.35
    if regime == "reversal":
        return 1.0 if i > n * 0.52 else -1.0
    if regime == "orderflow_flip":
        return -1.0 if i < n * 0.48 else 1.0
    if regime == "false_breakout":
        return 1.0 if 0.24 * n < i < 0.40 * n else -0.20
    if regime == "open_shock":
        return 1.0 if i < n * 0.16 else 0.35
    if regime == "close_accel":
        return 1.0 if i > n * 0.62 else 0.10
    if regime in {"range", "midday_drift", "vol_compress"}:
        return 0.05
    return 0.0


def _hidden_flow_step(
    rng: np.random.Generator,
    regime: str,
    latent: float,
    i: int,
    n: int,
    params: _SessionParams,
) -> float:
    target = _regime_flow_sign(regime, i, n)
    exo = rng.normal(0.0, 0.35)
    if regime == "liquidity_shock" and i in {int(n * 0.28), int(n * 0.29), int(n * 0.30), int(n * 0.70)}:
        exo += float(rng.choice([-2.5, 2.5]))
    if regime == "false_breakout" and int(n * 0.23) <= i < int(n * 0.34):
        target = 1.25
    if regime == "reversal" and i > int(n * 0.50):
        target = 1.0
    if regime == "orderflow_flip" and i > int(n * 0.48):
        target = 1.05
    latent = params.flow_persistence * latent + (1.0 - params.flow_persistence) * target + 0.22 * exo
    return float(np.clip(latent, -3.0, 3.0))


def _update_queues(
    rng: np.random.Generator,
    bids: np.ndarray,
    asks: np.ndarray,
    imbalance: float,
    aggressive_flow: float,
    params: _SessionParams,
    regime: str,
) -> Tuple[np.ndarray, np.ndarray, float]:
    """Apply cancellations/replenishment/marketable flow to the visible L5 book."""
    b = bids.copy(); a = asks.copy()
    # Cancellations are independent noise plus a directional skew.  Replenishment
    # restores queues but does not instantly reveal the hidden state.
    for level in range(len(b)):
        cancel_b = params.cancel_rate * rng.uniform(0.2, 1.0)
        cancel_a = params.cancel_rate * rng.uniform(0.2, 1.0)
        if imbalance > 0:
            cancel_a *= 1.25
            cancel_b *= 0.85
        elif imbalance < 0:
            cancel_b *= 1.25
            cancel_a *= 0.85
        b[level] *= max(0.15, 1.0 - cancel_b)
        a[level] *= max(0.15, 1.0 - cancel_a)

        if rng.random() < params.replenish_rate:
            decay = 1.0 / (1.0 + 0.20 * level)
            b[level] += params.depth_scale * decay * rng.uniform(0.02, 0.11)
            a[level] += params.depth_scale * decay * rng.uniform(0.02, 0.11)

    # Aggressive flow consumes the opposite queue.  This is visible only through
    # the resulting queue/volume changes, not through hidden labels.
    flow = float(np.clip(aggressive_flow, -2.5, 2.5))
    if flow > 0:
        a[0] = max(1.0, a[0] * (1.0 - min(0.75, 0.18 * flow)))
        b[0] += params.depth_scale * min(0.15, 0.025 * flow)
    elif flow < 0:
        b[0] = max(1.0, b[0] * (1.0 - min(0.75, 0.18 * abs(flow))))
        a[0] += params.depth_scale * min(0.15, 0.025 * abs(flow))

    # Some regimes deliberately produce deceptive imbalance that mean-reverts.
    if regime == "spoof_imbalance" and rng.random() < 0.18:
        if rng.random() < 0.5:
            b *= 1.65
        else:
            a *= 1.65
    if regime == "liquidity_shock" and rng.random() < 0.08:
        if rng.random() < 0.5:
            b *= 0.28
        else:
            a *= 0.28

    return np.maximum(b, 1.0), np.maximum(a, 1.0), flow


def _visible_features(
    mid: float,
    spread_pct: float,
    bids: np.ndarray,
    asks: np.ndarray,
    prev_imb5: float,
    prev_depth: float,
    prev_aggr: float,
    returns: Sequence[float],
    volume_window: Sequence[float],
    trade_flow: float,
) -> Dict[str, float]:
    bid_qty = float(bids.sum()); ask_qty = float(asks.sum())
    denom = max(bid_qty + ask_qty, 1e-9)
    imb5 = (bid_qty - ask_qty) / denom
    imb3 = (float(bids[:3].sum()) - float(asks[:3].sum())) / max(float(bids[:3].sum() + asks[:3].sum()), 1e-9)
    imb1 = (float(bids[0]) - float(asks[0])) / max(float(bids[0] + asks[0]), 1e-9)
    weights = np.array([1.0, 0.82, 0.68, 0.56, 0.45], dtype=float)[:len(bids)]
    wbid = float(np.dot(weights, bids)); wask = float(np.dot(weights, asks))
    wimb = (wbid - wask) / max(wbid + wask, 1e-9)
    best_bid = mid * (1.0 - spread_pct / 200.0)
    best_ask = mid * (1.0 + spread_pct / 200.0)
    micro = (best_ask * float(bids[0]) + best_bid * float(asks[0])) / max(float(bids[0] + asks[0]), 1e-9)
    micro_edge = (micro / mid - 1.0) * 100.0
    depth = bid_qty + ask_qty
    depth_change = depth / max(prev_depth, 1e-9) - 1.0
    imb_change = imb5 - prev_imb5
    ret_arr = np.asarray(list(returns), dtype=float)
    r1 = float(ret_arr[-1] if len(ret_arr) else 0.0)
    r3 = float(ret_arr[-3:].sum() if len(ret_arr) else 0.0)
    r8 = float(ret_arr[-8:].sum() if len(ret_arr) else 0.0)
    vol_arr = np.asarray(list(volume_window), dtype=float)
    vol_base = float(vol_arr.mean()) if len(vol_arr) else 1.0
    vol_std = float(vol_arr.std()) if len(vol_arr) else 1.0
    volume_now = float(vol_arr[-1] if len(vol_arr) else 1.0)
    volume_z = (volume_now - vol_base) / max(vol_std, 1e-6)
    depth_stress = abs(imb_change) + abs(depth_change) + abs(trade_flow) * 0.08
    queue_asym = (float(bids[0]) / max(float(asks[0]), 1.0))
    queue_asym = float(np.clip(math.log(max(queue_asym, 1e-6)), -3.0, 3.0))
    sweep_proxy = float(np.clip(abs(trade_flow) * (1.0 + abs(depth_change)) * 0.25, 0.0, 2.0))
    return {
        "imbalance_l1": imb1,
        "imbalance_l3": imb3,
        "imbalance_l5": imb5,
        "weighted_imbalance": wimb,
        "microprice_edge_pct": micro_edge,
        "imbalance_change": imb_change,
        "depth_change_pct": depth_change,
        "spread_pct": spread_pct,
        "ret_1_pct": r1,
        "ret_3_pct": r3,
        "ret_8_pct": r8,
        "realized_vol_pct": float(ret_arr[-12:].std() if len(ret_arr) else 0.0),
        "volume_z": float(np.clip(volume_z, -6.0, 6.0)),
        "aggressive_flow": float(trade_flow),
        "aggressive_flow_change": float(trade_flow - prev_aggr),
        "book_pressure_change": float(imb_change * 0.7 + np.clip(depth_change, -1.0, 1.0) * 0.3),
        "depth_stress": float(np.clip(depth_stress, 0.0, 3.0)),
        "queue_asymmetry": queue_asym,
        "sweep_proxy": sweep_proxy,
    }


def generate_session(
    rng: np.random.Generator,
    regime: str,
    session_id: int,
    cfg: SyntheticConfig,
) -> pd.DataFrame:
    """Generate one session with only visible market-state columns exposed."""
    if regime not in REGIMES:
        raise ValueError(f"Unknown synthetic regime: {regime}")
    params = _make_session_params(rng, regime)
    n = int(cfg.bars_per_session)
    # Different stocks/session seeds prevent identical replay paths.
    ticker = f"S{session_id % max(cfg.stock_count, 1):03d}"
    px = float(cfg.session_start_price * rng.lognormal(0.0, 0.003))
    latent = float(rng.normal(0.0, 0.2))
    bids = np.zeros(cfg.levels, dtype=float)
    asks = np.zeros(cfg.levels, dtype=float)
    for lvl in range(cfg.levels):
        decay = 1.0 / (1.0 + 0.22 * lvl)
        bids[lvl] = max(1.0, params.depth_scale * decay * rng.uniform(0.65, 1.15))
        asks[lvl] = max(1.0, params.depth_scale * decay * rng.uniform(0.65, 1.15))

    prev_imb5 = 0.0
    prev_depth = float(bids.sum() + asks.sum())
    prev_aggr = 0.0
    returns_pct: List[float] = []
    volume_hist: List[float] = []
    rows: List[Dict[str, Any]] = []

    # Regime-specific gap at the opening print; this is not a future label.
    if regime in {"gap_follow", "gap_fade", "open_shock"}:
        px *= float(1.0 + rng.normal(0.0, 0.006))

    for i in range(n):
        latent = _hidden_flow_step(rng, regime, latent, i, n, params)
        true_edge = params.edge_strength
        if regime == "no_edge":
            # Deliberately break the relation between visible book pressure and
            # subsequent returns. The model should learn to stay out.
            true_flow_for_price = rng.normal(0.0, 0.40)
        elif regime == "spoof_imbalance":
            true_flow_for_price = 0.15 * latent + rng.normal(0.0, 0.45)
        elif regime == "cross_current":
            true_flow_for_price = 0.30 * latent + rng.normal(0.0, 0.55)
        else:
            true_flow_for_price = true_edge * latent + rng.normal(0.0, 0.35)

        # Trade intensity.  It is deliberately only indirectly tied to latent flow.
        base_volume = params.volume_scale * (1.0 + 0.65 * abs(latent))
        if regime in {"open_shock", "liquidity_shock", "close_accel", "thin_book"}:
            base_volume *= rng.uniform(1.15, 2.3)
        volume_now = max(0.05, base_volume * rng.lognormal(0.0, 0.28))
        volume_hist.append(volume_now)
        if len(volume_hist) > 30:
            volume_hist.pop(0)

        # Observable aggressor proxy used to drive queues. Hidden truth remains
        # inaccessible to the model.
        aggressive_flow = float(np.clip(0.85 * latent + rng.normal(0.0, 0.95), -3.0, 3.0))
        if regime == "no_edge":
            aggressive_flow = float(rng.normal(0.0, 1.0))
        if regime == "orderflow_flip" and i > int(n * 0.48):
            aggressive_flow *= -1.0

        # A short-lived deceptive imbalance in the visible book.
        visible_imbalance = float(np.tanh(0.65 * aggressive_flow + rng.normal(0.0, 0.70)))
        if regime in {"spoof_imbalance", "false_breakout"} and rng.random() < 0.20:
            visible_imbalance = float(np.clip(visible_imbalance + rng.normal(0.0, 1.25), -1.0, 1.0))
        if regime == "spoof_imbalance" and rng.random() < 0.10:
            visible_imbalance = float(np.clip(-visible_imbalance + rng.normal(0.0, 0.15), -1.0, 1.0))

        bids, asks, queue_flow = _update_queues(
            rng,
            bids,
            asks,
            visible_imbalance,
            aggressive_flow,
            params,
            regime,
        )

        spread_pct = float(params.spread_base_pct * (1.0 + 0.55 * abs(visible_imbalance)))
        if regime == "wide_spread":
            spread_pct = float(np.clip(spread_pct * rng.uniform(1.4, 2.2), 0.02, 0.25))
        if regime == "liquidity_shock" and rng.random() < 0.10:
            spread_pct = float(np.clip(spread_pct * rng.uniform(1.8, 3.2), 0.02, 0.30))

        # Generator-level price process. The model never receives true_flow_for_price.
        return_shock = params.volatility * (0.55 * true_flow_for_price + rng.normal(0.0, 0.95))
        if regime == "false_breakout" and int(n * 0.24) <= i < int(n * 0.34):
            return_shock += 0.00070
        elif regime == "false_breakout" and int(n * 0.34) <= i < int(n * 0.43):
            return_shock -= 0.00120
        if regime == "reversal" and i > int(n * 0.52):
            return_shock += -0.0009 * np.sign(latent if latent else 1.0)
        if regime == "gap_fade" and i < int(n * 0.20):
            return_shock -= 0.0008 * np.sign(latent if latent else 1.0)
        px = max(10.0, px * (1.0 + REGIME_PARAMS[regime][0] + return_shock))
        actual_ret_pct = return_shock * 100.0
        returns_pct.append(actual_ret_pct)
        if len(returns_pct) > 50:
            returns_pct.pop(0)

        feats = _visible_features(
            px,
            spread_pct,
            bids,
            asks,
            prev_imb5,
            prev_depth,
            prev_aggr,
            returns_pct,
            volume_hist,
            queue_flow,
        )
        row: Dict[str, Any] = {
            "session_id": session_id,
            "bar": i,
            "timestamp": i,
            "ticker": ticker,
            "regime": regime,
            "ltp": px,
            "volume": volume_now,
            # Hidden generator state. Never included in FEATURES or API metrics.
            "hidden_state": latent,
            "hidden_true_flow": true_flow_for_price,
            "hidden_edge_strength": true_edge,
        }
        row.update(feats)
        # Preserve 5-level book details for audit/research export. The model is
        # derived from the aggregate observable features above, not these hidden labels.
        for lvl in range(cfg.levels):
            row[f"bid_px_{lvl+1}"] = px * (1.0 - spread_pct / 200.0) - px * 0.00005 * lvl
            row[f"ask_px_{lvl+1}"] = px * (1.0 + spread_pct / 200.0) + px * 0.00005 * lvl
            row[f"bid_qty_{lvl+1}"] = float(bids[lvl])
            row[f"ask_qty_{lvl+1}"] = float(asks[lvl])
        rows.append(row)
        prev_imb5 = feats["imbalance_l5"]
        prev_depth = float(bids.sum() + asks.sum())
        prev_aggr = queue_flow

    df = pd.DataFrame(rows)
    h = int(cfg.future_horizon_bars)
    df["forward_return_pct"] = df["ltp"].shift(-h) / df["ltp"] * 100.0 - 100.0
    # Keep only the label column needed by the model/evaluator. Hidden state stays
    # in the in-memory generator dataframe and is stripped before any result is returned.
    return df


class SyntheticL5Model:
    """Small causal ridge regression model using visible L5-derived features only."""

    def __init__(self, feature_names: Sequence[str]):
        self.feature_names = list(feature_names)
        self.mean: Optional[np.ndarray] = None
        self.std: Optional[np.ndarray] = None
        self.coef: Optional[np.ndarray] = None
        self.intercept: float = 0.0
        self.resid_std: float = 0.15
        self.trained_rows: int = 0

    def fit(self, df: pd.DataFrame) -> None:
        x = df[self.feature_names].astype(float).replace([np.inf, -np.inf], np.nan).fillna(0.0).to_numpy()
        y = df["forward_return_pct"].astype(float).to_numpy()
        self.mean = x.mean(axis=0)
        self.std = x.std(axis=0)
        self.std[self.std < 1e-8] = 1.0
        z = (x - self.mean) / self.std
        lam = 2.5
        a = z.T @ z + lam * np.eye(z.shape[1])
        b = z.T @ y
        self.coef = np.linalg.solve(a, b)
        self.intercept = float(y.mean())
        pred = z @ self.coef + self.intercept
        self.resid_std = float(max(0.02, np.std(y - pred)))
        self.trained_rows = len(df)

    def predict_row(self, row: pd.Series) -> Tuple[float, float]:
        if self.coef is None or self.mean is None or self.std is None:
            return 0.0, 0.5
        x = np.array([float(row.get(c, 0.0) or 0.0) for c in self.feature_names], dtype=float)
        z = (x - self.mean) / self.std
        pred = float(z @ self.coef + self.intercept)
        zprob = pred / max(self.resid_std, 1e-6)
        p_up = 0.5 * (1.0 + math.erf(zprob / math.sqrt(2.0)))
        return pred, float(np.clip(p_up, 0.001, 0.999))


def _feature_frame(df: pd.DataFrame) -> pd.DataFrame:
    return df[list(FEATURES)].replace([np.inf, -np.inf], np.nan).fillna(0.0)


def _compute_metrics(trades: pd.DataFrame, attempted: int, eligible_bars: int, total_bars: int) -> Dict[str, Any]:
    if trades.empty:
        return {
            "trades": 0,
            "win_rate_pct": 0.0,
            "avg_net_pct": 0.0,
            "sum_net_pct": 0.0,
            "profit_factor": None,
            "max_drawdown_pct_points": 0.0,
            "avg_hold_bars": 0.0,
            "target_dependency": False,
            "participation_rate_pct": 0.0,
            "no_trade_quality": None,
        }
    net = trades["net_pct"].astype(float)
    wins = float(net[net > 0].sum())
    losses = float(net[net < 0].sum())
    curve = net.cumsum()
    dd = curve - curve.cummax()
    no_trade_quality = None
    if "regime" in trades.columns:
        noedge = trades[trades["regime"] == "no_edge"]
        no_trade_quality = {
            "trades": int(len(noedge)),
            "participation_pct_of_all_trades": float(len(noedge) / max(len(trades), 1) * 100.0),
            "avg_net_pct": float(noedge["net_pct"].mean()) if not noedge.empty else 0.0,
        }
    return {
        "trades": int(len(trades)),
        "win_rate_pct": float((net > 0).mean() * 100.0),
        "avg_net_pct": float(net.mean()),
        "sum_net_pct": float(net.sum()),
        "profit_factor": float(wins / abs(losses)) if losses else None,
        "max_drawdown_pct_points": float(abs(dd.min())) if len(dd) else 0.0,
        "avg_hold_bars": float(trades["bars"].mean()),
        "edge_gone_exits": int((trades["reason"] == "EDGE_GONE").sum()),
        "protection_exits": int((trades["reason"] == "PROTECTION").sum()),
        "time_exits": int((trades["reason"] == "TIME_EXIT").sum()),
        "session_end_exits": int((trades["reason"] == "SESSION_END").sum()),
        "target_dependency": False,
        "participation_rate_pct": float(attempted / max(eligible_bars, 1) * 100.0),
        "no_trade_quality": no_trade_quality,
    }


def _simulate(
    df: pd.DataFrame,
    model: SyntheticL5Model,
    cfg: SyntheticConfig,
    min_net_edge_pct: float,
    min_probability: float,
    latency_bars: int,
    collect_detail: bool = False,
) -> Dict[str, Any]:
    trades: List[Dict[str, Any]] = []
    attempts = 0
    eligible = 0
    total = 0
    for session_id, g in df.groupby("session_id", sort=False):
        g = g.reset_index(drop=True)
        total += len(g)
        open_trade: Optional[Dict[str, Any]] = None
        for i in range(0, len(g) - 1):
            row = g.iloc[i]
            if open_trade is not None:
                # Entry is executed on a later observable bar. Do not evaluate an
                # exit on the execution bar itself; otherwise the configured
                # protection threshold would immediately include entry friction.
                if i <= int(open_trade["entry_i"]):
                    continue
                pred_now, _ = model.predict_row(row)
                side = int(open_trade["side"])
                gross_now = side * ((float(row.ltp) / open_trade["entry_px"]) - 1.0) * 100.0
                net_now = gross_now - cfg.round_trip_cost_pct
                bars = i - int(open_trade["entry_i"])
                if (
                    net_now <= -cfg.protection_pct
                    or side * pred_now <= 0.0
                    or bars >= cfg.max_hold_bars
                ):
                    reason = "PROTECTION" if net_now <= -cfg.protection_pct else "EDGE_GONE" if side * pred_now <= 0 else "TIME_EXIT"
                    trades.append({
                        "session_id": int(session_id),
                        "regime": str(g.regime.iloc[0]),
                        "side": side,
                        "net_pct": float(net_now),
                        "gross_pct": float(gross_now),
                        "bars": int(bars),
                        "reason": reason,
                    })
                    open_trade = None
                continue

            pred, p_up = model.predict_row(row)
            direction = 1 if pred > 0 else -1
            p_dir = p_up if direction > 0 else 1.0 - p_up
            # All execution economics are known at decision time; no future price
            # is referenced by this gate.
            friction = (
                cfg.round_trip_cost_pct
                + float(row.spread_pct)
                + cfg.entry_slippage_pct
                + cfg.adverse_selection_pct
            )
            expected_net = abs(pred) - friction
            eligible += 1
            if float(row.spread_pct) <= cfg.max_spread_pct and expected_net >= min_net_edge_pct and p_dir >= min_probability:
                attempts += 1
                entry_i = min(i + max(0, int(latency_bars)), len(g) - 1)
                exec_row = g.iloc[entry_i]
                entry_px = float(exec_row.ltp) * (
                    1.0 + direction * (float(exec_row.spread_pct) / 200.0 + cfg.entry_slippage_pct / 100.0 + cfg.adverse_selection_pct / 100.0)
                )
                open_trade = {"side": direction, "entry_px": entry_px, "entry_i": entry_i}

        if open_trade is not None:
            row = g.iloc[-1]
            side = int(open_trade["side"])
            gross_now = side * ((float(row.ltp) / open_trade["entry_px"]) - 1.0) * 100.0
            net_now = gross_now - cfg.round_trip_cost_pct
            trades.append({
                "session_id": int(session_id),
                "regime": str(g.regime.iloc[0]),
                "side": side,
                "net_pct": float(net_now),
                "gross_pct": float(gross_now),
                "bars": int(len(g) - int(open_trade["entry_i"])),
                "reason": "SESSION_END",
            })

    t = pd.DataFrame(trades)
    metrics = _compute_metrics(t, attempts, eligible, total)
    if not collect_detail:
        return metrics
    return {**metrics, "trades_detail": t.to_dict("records")}


def _choose_thresholds(validation: pd.DataFrame, model: SyntheticL5Model, cfg: SyntheticConfig) -> Dict[str, float]:
    """Tune gates on validation only, then freeze them for the blind test."""
    edge_grid = sorted(set([
        max(0.0, cfg.min_net_edge_pct * 0.5),
        cfg.min_net_edge_pct,
        cfg.min_net_edge_pct * 1.5,
        cfg.min_net_edge_pct * 2.0,
        cfg.min_net_edge_pct * 3.0,
    ]))
    prob_grid = sorted(set([
        max(0.50, cfg.min_probability - 0.03),
        cfg.min_probability,
        min(0.90, cfg.min_probability + 0.03),
        min(0.90, cfg.min_probability + 0.06),
    ]))
    best = {"min_net_edge_pct": cfg.min_net_edge_pct, "min_probability": cfg.min_probability, "score": -1e18}
    for edge in edge_grid:
        for prob in prob_grid:
            m = _simulate(validation, model, cfg, edge, prob, cfg.latency_bars, collect_detail=False)
            trades = int(m.get("trades", 0))
            avg = float(m.get("avg_net_pct", 0.0))
            pf = float(m.get("profit_factor") or 0.0)
            if trades < cfg.min_validation_trades:
                continue
            # Favor net expectancy and PF but penalize unstable over-trading.
            score = avg * 100.0 + min(pf, 8.0) * 0.30 - max(0.0, trades - 2500) / 10000.0
            if score > best["score"]:
                best = {"min_net_edge_pct": float(edge), "min_probability": float(prob), "score": float(score)}
    if best["score"] <= -1e17:
        best.pop("score", None)
    else:
        best.pop("score", None)
    return best


def _regime_metrics(test: pd.DataFrame, model: SyntheticL5Model, cfg: SyntheticConfig, thresholds: Dict[str, float]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for regime, g in test.groupby("regime", sort=True):
        sim = _simulate(g, model, cfg, thresholds["min_net_edge_pct"], thresholds["min_probability"], cfg.latency_bars, collect_detail=True)
        out.append({"regime": regime, **{k: v for k, v in sim.items() if k != "trades_detail"}})
    return out


def _latency_robustness(test: pd.DataFrame, model: SyntheticL5Model, cfg: SyntheticConfig, thresholds: Dict[str, float]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for lag in range(0, 4):
        m = _simulate(test, model, cfg, thresholds["min_net_edge_pct"], thresholds["min_probability"], lag, collect_detail=False)
        out.append({"latency_bars": lag, "trades": m.get("trades", 0), "win_rate_pct": m.get("win_rate_pct", 0.0), "avg_net_pct": m.get("avg_net_pct", 0.0), "profit_factor": m.get("profit_factor")})
    return out


def run_experiment(cfg: SyntheticConfig, progress: Optional[Callable[[int, str], None]] = None) -> Dict[str, Any]:
    regimes = REGIMES if cfg.include_no_edge else tuple(r for r in REGIMES if r != "no_edge")
    total_sessions = len(regimes) * int(cfg.sessions_per_regime)
    sessions: List[pd.DataFrame] = []
    sid = 0
    for r_idx, regime in enumerate(regimes):
        for k in range(int(cfg.sessions_per_regime)):
            sessions.append(generate_session(np.random.default_rng(int(cfg.seed + sid * 7919)), regime, sid, cfg))
            sid += 1
            if progress:
                progress(5 + int(30 * sid / max(total_sessions, 1)), f"Generating {regime} · session {k + 1}/{cfg.sessions_per_regime}")
    data = pd.concat(sessions, ignore_index=True)

    # Split by complete sessions *inside every regime*. This keeps the blind
    # test representative of every market condition instead of putting all
    # difficult regimes in one chronological block. Sessions are independent
    # synthetic episodes, so a deterministic session shuffle does not create
    # row-level or within-session leakage.
    split_rng = np.random.default_rng(int(cfg.seed) + 991_001)
    train_ids: set[int] = set()
    valid_ids: set[int] = set()
    test_ids: set[int] = set()
    for regime in regimes:
        ids = list(data.loc[data.regime == regime, 'session_id'].drop_duplicates().astype(int))
        split_rng.shuffle(ids)
        nr = len(ids)
        if nr < 3:
            raise ValueError("sessions_per_regime must be at least 3 so every regime has train, validation and blind-test sessions")
        n_train_r = max(1, int(round(nr * cfg.train_fraction)))
        n_valid_r = max(1, int(round(nr * cfg.validation_fraction)))
        # Preserve at least one completely untouched test session in every regime.
        if n_train_r + n_valid_r >= nr:
            n_train_r = max(1, min(n_train_r, nr - 2))
            n_valid_r = max(1, min(n_valid_r, nr - n_train_r - 1))
        train_ids.update(ids[:n_train_r])
        valid_ids.update(ids[n_train_r:n_train_r + n_valid_r])
        test_ids.update(ids[n_train_r + n_valid_r:])

    train = data[data.session_id.isin(train_ids)].copy().dropna(subset=["forward_return_pct"])
    valid = data[data.session_id.isin(valid_ids)].copy().dropna(subset=["forward_return_pct"])
    test = data[data.session_id.isin(test_ids)].copy().dropna(subset=["forward_return_pct"])

    if progress:
        progress(42, f"Training visible-only L5 model · {len(train):,} rows")
    model = SyntheticL5Model(FEATURES)
    model.fit(train)
    if progress:
        progress(58, f"Tuning entry gates on validation · {len(valid):,} rows")
    thresholds = _choose_thresholds(valid, model, cfg) if cfg.tune_thresholds_on_validation else {
        "min_net_edge_pct": cfg.min_net_edge_pct,
        "min_probability": cfg.min_probability,
    }
    validation_metrics = _simulate(valid, model, cfg, thresholds["min_net_edge_pct"], thresholds["min_probability"], cfg.latency_bars, collect_detail=False)
    if progress:
        progress(72, f"Running frozen blind test · {len(test):,} rows")
    test_metrics = _simulate(test, model, cfg, thresholds["min_net_edge_pct"], thresholds["min_probability"], cfg.latency_bars, collect_detail=True)
    regime_rows = _regime_metrics(test, model, cfg, thresholds)
    latency = _latency_robustness(test, model, cfg, thresholds)
    if progress:
        progress(95, "Finalizing regime coverage, no-edge discipline and robustness")

    # Do not return hidden generator state.  The regime label is evaluation metadata,
    # but all model-facing feature declarations remain visible-only.
    return {
        "ok": True,
        "evidence_class": EVIDENCE_CLASS,
        "warning": "Synthetic laboratory only. Hidden state is generator-only. Results are not NSE historical evidence and do not authorize live trading.",
        "config": asdict(cfg),
        "regimes": list(regimes),
        "regime_count": len(regimes),
        "train_sessions": len(train_ids),
        "validation_sessions": len(valid_ids),
        "test_sessions": len(test_ids),
        "generated_rows": int(len(data)),
        "train_rows": int(len(train)),
        "validation_rows": int(len(valid)),
        "test_rows": int(len(test)),
        "features": list(FEATURES),
        "hidden_state_excluded": True,
        "model": {"trained_rows": model.trained_rows, "residual_std_pct": model.resid_std},
        "thresholds_frozen_after_validation": thresholds,
        "validation_metrics": validation_metrics,
        "test_metrics": {k: v for k, v in test_metrics.items() if k != "trades_detail"},
        "regime_test_metrics": regime_rows,
        "latency_robustness": latency,
        "blind_test": True,
        "target_dependency": False,
    }


_STATE: Dict[str, Any] = {
    "running": False,
    "progress_pct": 0,
    "message": "Idle",
    "run_id": None,
    "result": None,
    "error": None,
    "started_at": None,
    "finished_at": None,
}
_LOCK = threading.RLock()
_THREAD: Optional[threading.Thread] = None


def start_scan(config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    global _THREAD
    with _LOCK:
        if _STATE["running"]:
            return dict(_STATE)
        allowed = set(SyntheticConfig.__dataclass_fields__.keys())
        clean = {k: v for k, v in (config or {}).items() if k in allowed}
        # Basic API-side bounds to stop an accidental Render memory bomb.
        if "sessions_per_regime" in clean:
            clean["sessions_per_regime"] = max(2, min(20, int(clean["sessions_per_regime"])))
        if "bars_per_session" in clean:
            clean["bars_per_session"] = max(120, min(900, int(clean["bars_per_session"])))
        if "levels" in clean:
            clean["levels"] = max(5, min(5, int(clean["levels"])))
        cfg = SyntheticConfig(**clean)
        rid = int(time.time() * 1000)
        _STATE.update({"running": True, "progress_pct": 0, "message": "Starting synthetic L5 market lab", "run_id": rid, "result": None, "error": None, "started_at": time.time(), "finished_at": None})

    def worker() -> None:
        try:
            def pct(p: int, msg: str) -> None:
                with _LOCK:
                    _STATE.update({"progress_pct": int(max(0, min(100, p))), "message": msg})
            result = run_experiment(cfg, pct)
            with _LOCK:
                _STATE.update({"running": False, "progress_pct": 100, "message": "Synthetic blind test complete", "result": result, "finished_at": time.time()})
        except Exception as exc:  # pragma: no cover - status path is tested separately
            with _LOCK:
                _STATE.update({"running": False, "message": "Synthetic lab failed", "error": str(exc)[:1000], "finished_at": time.time()})

    _THREAD = threading.Thread(target=worker, name="synthetic-l5-lab-v2", daemon=True)
    _THREAD.start()
    return dict(_STATE)


def status() -> Dict[str, Any]:
    with _LOCK:
        out = dict(_STATE)
        result = out.get("result") or {}
        out["evidence_class"] = result.get("evidence_class", EVIDENCE_CLASS)
        out["test_metrics"] = result.get("test_metrics")
        out["validation_metrics"] = result.get("validation_metrics")
        out["regime_test_metrics"] = result.get("regime_test_metrics")
        out["latency_robustness"] = result.get("latency_robustness")
        out["blind_test"] = bool(result.get("blind_test", False))
        return out
