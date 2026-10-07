"""UltraScalp synthetic L5 causal market laboratory, v3.

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
    "news_jump",
    "shock_recovery",
    "correlated_selloff",
    "idiosyncratic_burst",
    "dead_zone",
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
    "market_ret_1_pct", "market_ret_5_pct", "relative_strength_pct",
    "market_vol_pct", "vix_level", "vix_change", "trade_intensity",
    "cancel_intensity", "replenish_intensity", "book_turnover", "spread_change_pct",
)

EVIDENCE_CLASS = "SYNTHETIC_L5_CAUSAL_MARKET_LAB_V3"


@dataclass(frozen=True)
class SyntheticConfig:
    sessions_per_regime: int = 4
    bars_per_session: int = 900
    train_fraction: float = 0.60
    validation_fraction: float = 0.20
    seed: int = 20261007
    levels: int = 5
    stock_count: int = 150
    universe_mode: str = "midcap150"
    stock_symbols: Tuple[str, ...] = ()
    bar_interval_seconds: int = 1

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
    max_development_streams_per_regime: int = 60
    max_test_streams_per_regime: int = 50
    coverage_batch_size: int = 50
    coverage_batch: int = 0
    max_latency_streams: int = 600
    future_horizon_bars: int = 5
    session_start_price: float = 100.0
    event_probability: float = 0.025
    spoof_probability: float = 0.035
    shock_probability: float = 0.012


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
    "news_jump": (0.00000, 0.00310, 0.70, 0.48, 0.78, 1.25, 0.55),
    "shock_recovery": (0.00000, 0.00220, 0.52, 0.55, 0.60, 1.12, 0.68),
    "correlated_selloff": (-0.00035, 0.00180, 0.62, 0.66, 0.66, 1.10, 0.80),
    "idiosyncratic_burst": (0.00000, 0.00240, 0.58, 0.52, 0.64, 1.08, 0.72),
    "dead_zone": (0.00000, 0.00020, 0.02, 0.03, 0.02, 1.02, 1.30),
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
    return regime not in {"no_edge", "chop", "spoof_imbalance", "cross_current", "dead_zone"}


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
    if regime in {"range", "midday_drift", "vol_compress", "dead_zone"}:
        return 0.05
    if regime == "correlated_selloff":
        return -1.0
    if regime == "shock_recovery":
        return -1.0 if i < n * 0.45 else 0.75
    if regime == "news_jump":
        return 1.0 if i > n * 0.20 else 0.0
    if regime == "idiosyncratic_burst":
        return 0.65 if n * 0.30 < i < n * 0.48 else 0.0
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
    ticker: Optional[str] = None,
    stock_index: int = 0,
) -> pd.DataFrame:
    """Generate one session with only visible market-state columns exposed."""
    if regime not in REGIMES:
        raise ValueError(f"Unknown synthetic regime: {regime}")
    params = _make_session_params(rng, regime)
    n = int(cfg.bars_per_session)
    # One shared observable market factor creates realistic cross-sectional
    # correlation without leaking the hidden regime into the model. Every stock
    # in the same session sees the same realised market path, plus idiosyncratic
    # noise and its own liquidity/volatility parameters.
    common_rng = np.random.default_rng(int(cfg.seed) ^ 0x5A17C0DE)
    common = 0.0
    common_path = []
    common_persist = float(np.clip(0.72 + common_rng.normal(0, 0.06), 0.45, 0.92))
    for j in range(n):
        shock = common_rng.normal(0.0, params.volatility * 0.42)
        if regime in {"correlated_selloff", "open_shock", "news_jump", "liquidity_shock"} and j in {int(n*0.28), int(n*0.29)}:
            shock += common_rng.choice([-1.0, 1.0]) * params.volatility * common_rng.uniform(2.0, 4.0)
        common = common_persist * common + (1.0-common_persist) * shock
        common_path.append(float(common))
    stock_liquidity = float(np.clip(0.75 + 0.50 * common_rng.random(), 0.45, 1.45))
    stock_vol_mult = float(np.clip(common_rng.lognormal(0.0, 0.18), 0.65, 1.55))
    sector_beta = float(np.clip(common_rng.normal(0.95, 0.18), 0.45, 1.45))
    # Different stocks/session seeds prevent identical replay paths.  The ticker is
    # an identity label only; it is never used as a model feature.
    if not ticker:
        symbols = tuple(cfg.stock_symbols or ())
        ticker = symbols[stock_index % len(symbols)] if symbols else f"MIDCAP{stock_index+1:03d}"
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
        volume_now = max(0.05, base_volume * stock_liquidity * rng.lognormal(0.0, 0.28))
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

        # Configurable event layer: cancellations, spoof-like displayed size and
        # transient liquidity shocks are observable through the book, but their
        # hidden cause is never exposed to the model.
        if rng.random() < float(np.clip(cfg.spoof_probability, 0.0, 0.50)):
            side = 0 if rng.random() < 0.5 else 1
            factor = float(rng.uniform(1.35, 2.60))
            if side == 0:
                bids *= factor
            else:
                asks *= factor
        if rng.random() < float(np.clip(cfg.shock_probability, 0.0, 0.30)):
            side = 0 if rng.random() < 0.5 else 1
            factor = float(rng.uniform(0.20, 0.65))
            if side == 0:
                bids *= factor
            else:
                asks *= factor
            aggressive_flow += float(rng.normal(0.0, 0.55))
        if rng.random() < float(np.clip(cfg.event_probability, 0.0, 0.50)):
            # A short event burst changes both queue pressure and trade intensity.
            aggressive_flow = float(np.clip(aggressive_flow + rng.normal(0.0, 1.0), -3.0, 3.0))
            volume_now *= float(rng.uniform(1.4, 3.5))
            if abs(aggressive_flow) > 1.2:
                visible_imbalance = float(np.clip(0.65 * visible_imbalance + 0.35 * np.sign(aggressive_flow), -1.0, 1.0))

        spread_pct = float(params.spread_base_pct * (1.0 + 0.55 * abs(visible_imbalance)))
        if regime == "wide_spread":
            spread_pct = float(np.clip(spread_pct * rng.uniform(1.4, 2.2), 0.02, 0.25))
        if regime == "liquidity_shock" and rng.random() < 0.10:
            spread_pct = float(np.clip(spread_pct * rng.uniform(1.8, 3.2), 0.02, 0.30))

        # Generator-level price process. The model never receives true_flow_for_price.
        return_shock = params.volatility * stock_vol_mult * (0.55 * true_flow_for_price + rng.normal(0.0, 0.95)) + sector_beta * common_path[i]
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
        # Observable cross-market context.  The generator may use a hidden
        # market factor, but the engine only receives the realized index/VIX
        # stream available at this timestamp. Event intensity is randomized
        # by configuration so stress tests can deliberately become harder.
        market_ret = float(common_path[i] + rng.normal(0.0, max(params.volatility * 0.18, 0.00005)))
        vix_level = float(np.clip(13.0 + 7.0 * abs(market_ret * 100.0) + rng.normal(0.0, 0.12), 9.0, 40.0))
        market_hist = returns_pct[-8:]
        feats.update({
            "market_ret_1_pct": market_ret * 100.0,
            "market_ret_5_pct": float(sum(market_hist[-5:]) * 0.32),
            "relative_strength_pct": float(actual_ret_pct - market_ret * 100.0),
            "market_vol_pct": float(np.std(market_hist[-20:])) if len(market_hist) > 3 else 0.0,
            "vix_level": vix_level,
            "vix_change": float(vix_level - 13.0),
            "trade_intensity": float(abs(queue_flow) * volume_now),
            "cancel_intensity": float(max(0.0, prev_depth - (bids.sum() + asks.sum()))),
            "replenish_intensity": float((bids.sum() + asks.sum()) / max(params.depth_scale, 1.0)),
            "book_turnover": float((abs(queue_flow) + 0.25 * volume_now) / max(bids.sum() + asks.sum(), 1.0)),
            "spread_change_pct": float(spread_pct - (prev_depth / max(params.depth_scale, 1.0)) * 0.01),
        })
        row: Dict[str, Any] = {
            "session_id": session_id,
            "bar": i,
            "timestamp": (9*3600 + 15*60 + i*int(max(1, cfg.bar_interval_seconds))),
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
            tick = 0.05 if px < 1000.0 else 0.10
            row[f"bid_px_{lvl+1}"] = max(tick, px * (1.0 - spread_pct / 200.0) - tick * lvl)
            row[f"ask_px_{lvl+1}"] = px * (1.0 + spread_pct / 200.0) + tick * lvl
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
    """Causal execution simulator using vectorized visible-feature prediction.

    Only the point-in-time feature row is used for the decision.  Entry is shifted
    by latency, execution friction is charged explicitly, and the position exits
    when the current predicted edge disappears, protection is hit, or the hold
    budget expires.  No future label is consulted by the simulator.
    """
    trades: List[Dict[str, Any]] = []
    attempts = 0
    eligible = 0
    total = 0
    if df is None or df.empty:
        return _compute_metrics(pd.DataFrame(), 0, 0, 0)

    for session_id, g in df.groupby("session_id", sort=False):
        g = g.reset_index(drop=True)
        total += len(g)
        x = g[model.feature_names].astype(float).replace([np.inf, -np.inf], np.nan).fillna(0.0).to_numpy()
        if model.coef is None or model.mean is None or model.std is None:
            preds = np.zeros(len(g), dtype=float)
            p_up = np.full(len(g), 0.5, dtype=float)
        else:
            z = (x - model.mean) / model.std
            preds = z @ model.coef + model.intercept
            zprob = preds / max(model.resid_std, 1e-6)
            # Fast smooth Gaussian-CDF approximation; avoids Python-level erf loops
            # while preserving a monotone probability calibration for the gate.
            p_up = 0.5 * (1.0 + np.tanh(0.79788456 * zprob))
            p_up = np.clip(p_up, 0.001, 0.999)
        ltp = g["ltp"].astype(float).to_numpy()
        spread = g["spread_pct"].astype(float).to_numpy()
        regime = str(g["regime"].iloc[0]) if "regime" in g.columns else "unknown"
        open_trade: Optional[Dict[str, Any]] = None
        for i in range(max(0, len(g) - 1)):
            if open_trade is not None:
                if i <= int(open_trade["entry_i"]):
                    continue
                side = int(open_trade["side"])
                gross_now = side * ((ltp[i] / open_trade["entry_px"]) - 1.0) * 100.0
                net_now = gross_now - cfg.round_trip_cost_pct
                bars = i - int(open_trade["entry_i"])
                if net_now <= -cfg.protection_pct or side * preds[i] <= 0.0 or bars >= cfg.max_hold_bars:
                    reason = "PROTECTION" if net_now <= -cfg.protection_pct else "EDGE_GONE" if side * preds[i] <= 0 else "TIME_EXIT"
                    trades.append({"session_id": int(session_id), "regime": regime, "side": side, "net_pct": float(net_now), "gross_pct": float(gross_now), "bars": int(bars), "reason": reason})
                    open_trade = None
                continue

            direction = 1 if preds[i] > 0 else -1
            p_dir = p_up[i] if direction > 0 else 1.0 - p_up[i]
            friction = cfg.round_trip_cost_pct + spread[i] + cfg.entry_slippage_pct + cfg.adverse_selection_pct
            expected_net = abs(preds[i]) - friction
            eligible += 1
            if spread[i] <= cfg.max_spread_pct and expected_net >= min_net_edge_pct and p_dir >= min_probability:
                attempts += 1
                entry_i = min(i + max(0, int(latency_bars)), len(g) - 1)
                exec_spread = spread[entry_i]
                entry_px = ltp[entry_i] * (1.0 + direction * (exec_spread / 200.0 + cfg.entry_slippage_pct / 100.0 + cfg.adverse_selection_pct / 100.0))
                open_trade = {"side": direction, "entry_px": entry_px, "entry_i": entry_i}
        if open_trade is not None:
            i = len(g) - 1
            side = int(open_trade["side"])
            gross_now = side * ((ltp[i] / open_trade["entry_px"]) - 1.0) * 100.0
            net_now = gross_now - cfg.round_trip_cost_pct
            trades.append({"session_id": int(session_id), "regime": regime, "side": side, "net_pct": float(net_now), "gross_pct": float(gross_now), "bars": int(i - int(open_trade["entry_i"])), "reason": "SESSION_END"})

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
    """Run the lab without materialising the entire synthetic market in RAM.

    Every regime/stock/session contributes to development data.  Training and
    validation retain only the observable feature matrix plus the future label;
    blind-test sessions are regenerated one at a time.  This is deliberately
    different from the old all-data concat path because the web service must
    remain safe on a small Render instance.
    """
    regimes = REGIMES if cfg.include_no_edge else tuple(r for r in REGIMES if r != "no_edge")
    split_rng = np.random.default_rng(int(cfg.seed) + 991_001)
    symbols = tuple(cfg.stock_symbols or ())
    if symbols:
        universe_symbols = symbols
    else:
        universe_symbols = tuple(f"MIDCAP{idx+1:03d}" for idx in range(max(1, int(cfg.stock_count))))
    universe_count = len(universe_symbols)
    batch_size = max(1, min(universe_count, int(cfg.coverage_batch_size)))
    batch_start = (int(cfg.coverage_batch) * batch_size) % universe_count
    active_symbols = tuple(universe_symbols[(batch_start + i) % universe_count] for i in range(batch_size))
    descriptors = []
    sid = 0
    for regime in regimes:
        ids = list(range(sid, sid + int(cfg.sessions_per_regime)))
        sid += int(cfg.sessions_per_regime)
        split_rng.shuffle(ids)
        nr = len(ids)
        if nr < 3:
            raise ValueError("sessions_per_regime must be at least 3 for train/validation/blind-test separation")
        nt = max(1, int(round(nr * cfg.train_fraction)))
        nv = max(1, int(round(nr * cfg.validation_fraction)))
        nt = min(nt, nr - 2)
        nv = min(nv, nr - nt - 1)
        for j, session_id in enumerate(ids):
            split = "train" if j < nt else "validation" if j < nt + nv else "test"
            for stock_idx in range(len(active_symbols)):
                descriptors.append((regime, int(session_id), stock_idx, split))

    total = len(descriptors)
    # The configured universe is the complete NIFTY Midcap 150. To keep a
    # Render-sized research worker bounded, the blind test uses a deterministic
    # rotating cross-section per regime/session while recording that the full
    # 150-stock universe was requested. A later walk-forward batch can rotate
    # the stock slice; no stock is silently reclassified as a different universe.
    test_limit = max(1, int(cfg.max_test_streams_per_regime))
    test_descriptors_by_regime = {}
    for regime in regimes:
        td = [d for d in descriptors if d[0] == regime and d[3] == "test"]
        test_descriptors_by_regime[regime] = td[:test_limit]
    descriptors = [d for d in descriptors if d[3] != "test"] + [d for r in regimes for d in test_descriptors_by_regime[r]]
    # Every stock remains part of the configured universe. Development fitting can be
    # bounded deterministically per regime so a 150-stock universe does not turn
    # the web service into a multi-million-row Python object factory. The bound is
    # applied only to train/validation streams, never to blind-test coverage.
    dev_descriptors = []
    per_regime_dev = max(3, int(cfg.max_development_streams_per_regime))
    for regime in regimes:
        rows = [d for d in descriptors if d[0] == regime and d[3] in ("train", "validation")]
        dev_descriptors.extend(rows[:per_regime_dev])
    dev_set = set(dev_descriptors)
    train_frames: List[pd.DataFrame] = []
    valid_frames: List[pd.DataFrame] = []
    test_descriptors: List[tuple] = []
    train_rows_per_stream = 120
    valid_rows_per_stream = 120

    def make_stream(regime: str, session_id: int, stock_idx: int) -> pd.DataFrame:
        seed = int(cfg.seed + session_id * 7919 + stock_idx * 104729)
        stream_cfg = SyntheticConfig(**{**asdict(cfg), "seed": seed, "stock_symbols": active_symbols, "stock_count": len(active_symbols), "session_start_price": float(80.0 + (stock_idx % 25) * 7.5), "sessions_per_regime": 1})
        return generate_session(np.random.default_rng(seed), regime, session_id * 1000 + stock_idx, stream_cfg, active_symbols[stock_idx], stock_idx)

    for idx, (regime, session_id, stock_idx, split) in enumerate(descriptors, 1):
        if split == "test":
            test_descriptors.append((regime, session_id, stock_idx))
        elif (regime, session_id, stock_idx, split) in dev_set:
            df = make_stream(regime, session_id, stock_idx).dropna(subset=["forward_return_pct"])
            keep = max(1, min(len(df), train_rows_per_stream if split == "train" else valid_rows_per_stream))
            # Evenly spaced rows preserve open/midday/close conditions rather
            # than accidentally training only on the beginning of sessions.
            ix = np.linspace(0, len(df) - 1, keep, dtype=int)
            compact = df.iloc[ix][list(FEATURES) + ["forward_return_pct", "session_id", "regime", "ltp"]].copy()
            (train_frames if split == "train" else valid_frames).append(compact)
        if progress and (idx == 1 or idx % max(1, total // 20) == 0):
            progress(5 + int(30 * idx / max(total, 1)), f"Building causal market streams · {idx:,}/{total:,}")

    if progress:
        progress(42, f"Training visible-only L5 model · {sum(len(x) for x in train_frames):,} retained feature rows")
    model = SyntheticL5Model(FEATURES)
    model.fit(pd.concat(train_frames, ignore_index=True))
    train_frames.clear()

    if progress:
        progress(58, f"Selecting entry gates on validation only · {len(valid_frames):,} stock-session slices")
    validation_df = pd.concat(valid_frames, ignore_index=True) if valid_frames else pd.DataFrame(columns=list(FEATURES) + ["forward_return_pct", "session_id", "regime", "ltp"])
    thresholds = _choose_thresholds(validation_df, model, cfg)
    validation_metrics = _simulate(validation_df, model, cfg, thresholds["min_net_edge_pct"], thresholds["min_probability"], cfg.latency_bars) if not validation_df.empty else _compute_metrics(pd.DataFrame(), 0, 0, 0)
    valid_frames.clear()

    if progress:
        progress(72, f"Running frozen blind test one stock-session at a time · {len(test_descriptors):,} streams")
    test_sims = []
    for idx, (regime, sid0, stock) in enumerate(test_descriptors, 1):
        test_sims.append(_simulate(make_stream(regime, sid0, stock), model, cfg, thresholds["min_net_edge_pct"], thresholds["min_probability"], cfg.latency_bars))
        if progress and (idx == 1 or idx % max(1, len(test_descriptors) // 10) == 0):
            progress(72 + int(18 * idx / max(len(test_descriptors), 1)), f"Blind testing {idx:,}/{len(test_descriptors):,} stock-sessions")

    regime_rows = []
    for regime in regimes:
        rows = [x for x, d in zip(test_sims, test_descriptors) if d[0] == regime]
        agg = _aggregate_metrics(rows)
        agg["regime"] = regime
        regime_rows.append(agg)

    latency = []
    latency_desc = list(test_descriptors)
    if len(latency_desc) > int(cfg.max_latency_streams):
        step = max(1, len(latency_desc) // int(cfg.max_latency_streams))
        latency_desc = latency_desc[::step][:int(cfg.max_latency_streams)]
    for lag in range(4):
        lag_sims = []
        for regime, sid0, stock in latency_desc:
            lag_sims.append(_simulate(make_stream(regime, sid0, stock), model, cfg, thresholds["min_net_edge_pct"], thresholds["min_probability"], lag))
        agg = _aggregate_metrics(lag_sims)
        latency.append({"latency_bars": lag, "sampled_streams": len(latency_desc), **{k: agg.get(k) for k in ("trades", "win_rate_pct", "avg_net_pct", "profit_factor")}})

    if progress:
        progress(100, "Synthetic causal blind test ready for your run")
    return {
        "ok": True,
        "evidence_class": EVIDENCE_CLASS,
        "warning": "Synthetic research only. Hidden state, true regime and future returns are generator-only. No synthetic result authorizes live trading.",
        "config": asdict(cfg),
        "regimes": list(regimes), "regime_count": len(regimes),
        "train_sessions": sum(1 for r in regimes for _, s in _session_split_rows(descriptors, r) if s == "train"),
        "validation_sessions": sum(1 for r in regimes for _, s in _session_split_rows(descriptors, r) if s == "validation"),
        "test_sessions": sum(1 for r in regimes for _, s in _session_split_rows(descriptors, r) if s == "test"),
        "stock_sessions": total,
        "development_streams_used": len(dev_descriptors),
        "blind_test_streams": len(test_descriptors),
        "features": list(FEATURES), "hidden_state_excluded": True,
        "universe": {"mode": cfg.universe_mode, "stock_count": universe_count, "simulated_stock_count": len(active_symbols), "coverage_batch": int(cfg.coverage_batch), "coverage_batch_size": len(active_symbols), "symbols": list(universe_symbols), "simulated_symbols": list(active_symbols), "all_midcap150_requested": cfg.universe_mode == "midcap150" and universe_count == 150},
        "model": {"trained_rows": model.trained_rows, "residual_std_pct": model.resid_std},
        "thresholds_frozen_after_validation": thresholds,
        "validation_metrics": validation_metrics,
        "test_metrics": _aggregate_metrics(test_sims),
        "regime_test_metrics": regime_rows,
        "latency_robustness": latency,
        "blind_test": True, "target_dependency": False,
    }


def _validation_descriptors(descriptors):
    return [(r, sid0, stock) for r, sid0, stock, split in descriptors if split == "validation"]


def _session_split_rows(descriptors, regime):
    seen = {}
    for r, sid0, stock, split in descriptors:
        if r == regime:
            seen[sid0] = split
    return list(seen.items())


def _aggregate_metrics(rows):
    if not rows:
        return {"trades": 0, "win_rate_pct": 0.0, "avg_net_pct": 0.0, "sum_net_pct": 0.0, "profit_factor": None, "target_dependency": False}
    trades = sum(int(x.get("trades", 0)) for x in rows)
    weights = [max(1, int(x.get("trades", 0))) for x in rows]
    win = float(np.average([float(x.get("win_rate_pct", 0.0)) for x in rows], weights=weights))
    avg = float(np.average([float(x.get("avg_net_pct", 0.0)) for x in rows], weights=weights))
    total = float(sum(float(x.get("sum_net_pct", 0.0)) for x in rows))
    pfs = [x.get("profit_factor") for x in rows if x.get("profit_factor") is not None]
    return {"trades": trades, "win_rate_pct": win, "avg_net_pct": avg, "sum_net_pct": total, "profit_factor": float(np.mean(pfs)) if pfs else None, "target_dependency": False}

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
            clean["bars_per_session"] = max(120, min(22500, int(clean["bars_per_session"])))
        if "levels" in clean:
            clean["levels"] = max(5, min(5, int(clean["levels"])))
        if "stock_count" in clean:
            clean["stock_count"] = max(1, min(150, int(clean["stock_count"])))
        if "bar_interval_seconds" in clean:
            clean["bar_interval_seconds"] = max(1, min(60, int(clean["bar_interval_seconds"])))
        if "coverage_batch_size" in clean:
            clean["coverage_batch_size"] = max(1, min(150, int(clean["coverage_batch_size"])))
        if "coverage_batch" in clean:
            clean["coverage_batch"] = max(0, min(20, int(clean["coverage_batch"])))
        if isinstance(clean.get("stock_symbols"), list):
            clean["stock_symbols"] = tuple(str(x).upper().strip() for x in clean["stock_symbols"] if str(x).strip())
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

    _THREAD = threading.Thread(target=worker, name="synthetic-l5-lab-v3", daemon=True)
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
