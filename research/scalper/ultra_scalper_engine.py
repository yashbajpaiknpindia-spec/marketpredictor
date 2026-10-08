"""MarketPredictor UltraScalp research/execution engine.

This module contains the event-driven scalper logic developed during the
October 2026 research cycle.  It is intentionally data-honest:

* OHLCV mode uses only bar/market-context information actually available.
* L2 mode can consume genuine order-book fields when a provider supplies them.
* Synthetic L2 research is not used as a substitute for real NSE history.
* Entry timing is freshness-aware and can reject stale/chased signals.
* Trade simulation uses first-touch stop/target semantics and explicit costs.

The module is side-effect-free and can be imported by live/paper/replay code.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, Iterable, Optional, Tuple
import math

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class ScalperConfig:
    target_pct: float = 0.60
    protection_pct: float = 0.18
    max_hold_bars: int = 10
    round_trip_cost_pct: float = 0.1363
    entry_slippage_pct: float = 0.01515
    min_remaining_edge_pct: float = 0.005
    max_entry_lag_bars: int = 2
    freshness_decay_per_bar: float = 0.15
    min_signal_score: float = 65.0
    require_market_confirmation: bool = True
    require_relative_strength: bool = True
    use_l2_when_available: bool = True
    # V12 calibration controls. The legacy confidence remains a score unless a
    # frozen empirical profile is explicitly supplied.
    calibration_profile: Optional[Dict[str, Any]] = None
    require_calibrated_probability: bool = False
    min_calibrated_net_probability: float = 0.10
    max_adverse_probability: float = 0.35
    # Frozen leakage-safe directional/edge model. When supplied, it supersedes the
    # legacy heuristic direction and expected-move estimate for execution decisions.
    v12_model_profile: Optional[Any] = None
    min_model_expected_edge_pct: float = 0.10
    min_model_target_probability: float = 0.70


@dataclass(frozen=True)
class UltraScalpV12Config:
    """Live-paper V12 policy settings. The V12 economic policy uses a meaningful payoff target; positive-after-friction alone is not sufficient for execution."""
    target_net_pct: float = 0.4487
    round_trip_cost_pct: float = 0.1363
    entry_slippage_pct: float = 0.015
    safety_timeout_minutes: int = 30
    entry_latency_bars: int = 1
    profit_lock_enabled: bool = True
    research_probability_candidate: float = 0.10

    @property
    def gross_target_pct(self) -> float:
        return self.target_net_pct + self.round_trip_cost_pct + self.entry_slippage_pct


def v12_config_dict(config: UltraScalpV12Config = UltraScalpV12Config()) -> Dict[str, Any]:
    out = asdict(config)
    out['gross_target_pct'] = config.gross_target_pct
    return out


FEATURE_COLUMNS = [
    "ret_1", "ret_3", "ret_5", "range_pct", "body_pct", "close_location",
    "vol_ratio_20", "range_ratio_20", "vwap_distance_pct", "rs_1", "rs_3",
    "market_ret_1", "market_ret_3", "freshness",
]


def _safe_z(series: pd.Series, window: int = 20) -> pd.Series:
    mean = series.rolling(window, min_periods=max(5, window // 2)).mean()
    std = series.rolling(window, min_periods=max(5, window // 2)).std(ddof=0).replace(0, np.nan)
    return ((series - mean) / std).replace([np.inf, -np.inf], np.nan).fillna(0.0)


def prepare_ohlcv_frame(df: pd.DataFrame, market: Optional[pd.Series] = None) -> pd.DataFrame:
    """Build symmetric, causal OHLCV features; no future values are referenced."""
    x = df.copy()
    x.columns = [str(c).lower() for c in x.columns]
    for c in ("open", "high", "low", "close", "volume"):
        if c not in x:
            raise ValueError(f"Missing required OHLCV column: {c}")
    x = x.sort_index() if not isinstance(x.index, pd.RangeIndex) else x
    close = x["close"].astype(float)
    open_ = x["open"].astype(float)
    high = x["high"].astype(float)
    low = x["low"].astype(float)
    volume = x["volume"].astype(float).clip(lower=0)

    x["ret_1"] = close.pct_change(1) * 100.0
    x["ret_3"] = close.pct_change(3) * 100.0
    x["ret_5"] = close.pct_change(5) * 100.0
    x["range_pct"] = ((high - low) / close.replace(0, np.nan)) * 100.0
    x["body_pct"] = ((close - open_) / open_.replace(0, np.nan)) * 100.0
    x["close_location"] = ((close - low) / (high - low).replace(0, np.nan)).clip(0, 1)
    x["vol_ratio_20"] = volume / volume.rolling(20, min_periods=5).median().replace(0, np.nan)
    x["range_ratio_20"] = x["range_pct"] / x["range_pct"].rolling(20, min_periods=5).median().replace(0, np.nan)
    typical = (high + low + close) / 3.0
    pv = (typical * volume).cumsum()
    vv = volume.cumsum().replace(0, np.nan)
    x["vwap_distance_pct"] = ((close / (pv / vv)) - 1.0) * 100.0

    if market is not None:
        m = market.reindex(x.index).ffill()
        x["market_ret_1"] = m.pct_change(1) * 100.0
        x["market_ret_3"] = m.pct_change(3) * 100.0
        x["rs_1"] = x["ret_1"] - x["market_ret_1"]
        x["rs_3"] = x["ret_3"] - x["market_ret_3"]
    else:
        x["market_ret_1"] = 0.0
        x["market_ret_3"] = 0.0
        x["rs_1"] = x["ret_1"]
        x["rs_3"] = x["ret_3"]

    x["freshness"] = 1.0
    return x.replace([np.inf, -np.inf], np.nan).fillna(0.0)


def _directional_signal(row: pd.Series, config: ScalperConfig) -> Tuple[int, float, Dict[str, float]]:
    """Return +1/-1/0, symmetric score and component diagnostics."""
    momentum = 0.45 * float(row.ret_1) + 0.35 * float(row.ret_3) + 0.20 * float(row.ret_5)
    pressure = float(row.body_pct) + 0.50 * (float(row.close_location) - 0.5)
    activity = max(0.0, float(row.vol_ratio_20) - 1.0) + 0.5 * max(0.0, float(row.range_ratio_20) - 1.0)
    rs = 0.55 * float(row.rs_1) + 0.45 * float(row.rs_3)
    market = 0.60 * float(row.market_ret_1) + 0.40 * float(row.market_ret_3)

    raw = momentum + pressure
    if activity > 0:
        raw += math.copysign(min(1.25, activity), raw if raw else momentum)
    if config.require_relative_strength:
        raw += 0.65 * rs
    if config.require_market_confirmation:
        # Agreement boosts the score; disagreement is a hard-ish penalty.
        raw += 0.40 * math.copysign(abs(market), raw) if raw and market and np.sign(raw) == np.sign(market) else -0.60 * abs(market)

    direction = 1 if raw > 0 else -1 if raw < 0 else 0
    confidence = min(100.0, 50.0 + 18.0 * abs(raw) + 6.0 * abs(rs) + 3.0 * max(0.0, activity))
    if direction == 0 or confidence < config.min_signal_score:
        direction = 0
    parts = {"momentum": float(momentum), "pressure": float(pressure), "activity": float(activity), "relative_strength": float(rs), "market": float(market)}
    return direction, confidence, parts


def score_event(row: pd.Series, config: ScalperConfig = ScalperConfig(), entry_lag_bars: int = 0) -> Dict[str, Any]:
    raw_direction, raw_confidence, parts = _directional_signal(row, config)
    model_direction = None
    model_expected_edge = None
    model_target_probability = None
    model_long_edge = None
    model_short_edge = None
    if config.v12_model_profile is not None:
        try:
            from .v12_models import apply_v12_models
        except ImportError:
            from v12_models import apply_v12_models
        _m = apply_v12_models(pd.DataFrame([row.to_dict()]), config.v12_model_profile).iloc[0]
        model_direction = int(_m.get("v12_model_direction", 0))
        model_expected_edge = float(_m.get("v12_model_expected_net_edge_pct", -999.0))
        model_target_probability = float(_m.get("v12_model_p_direction_target", 0.0))
        model_long_edge = float(_m.get("v12_model_expected_long_net_edge_pct", -999.0))
        model_short_edge = float(_m.get("v12_model_expected_short_net_edge_pct", -999.0))
        # The learned model is the execution authority when loaded. The legacy
        # heuristic remains available as a diagnostic only.
        if (model_direction == 0 or model_expected_edge < config.min_model_expected_edge_pct
                or model_target_probability < config.min_model_target_probability):
            raw_direction = 0
    lag = max(0, int(entry_lag_bars))
    freshness = max(0.0, 1.0 - lag * config.freshness_decay_per_bar)
    confidence = raw_confidence * freshness

    # IMPORTANT: this number is a model score, not a probability. V12 can only
    # call something a probability after applying a profile fit on earlier data.
    calibrated_net_probability = None
    calibrated_adverse_probability = None
    calibration_status = "not_requested"
    if config.calibration_profile:
        try:
            from .v12_calibration import apply_v12_calibration
        except ImportError:
            from v12_calibration import apply_v12_calibration
        score_for_calibration = float(raw_confidence + 8.0 * math.tanh(
            0.45 * float(row.get("ofi_proxy", row.get("ofi", 0.0)) or 0.0)
            + 0.35 * float(row.get("imbalance_l5", row.get("imbalance_l10", 0.0)) or 0.0)
            + 0.20 * float(row.get("microprice_edge_pct", row.get("microprice_edge", 0.0)) or 0.0)
        ))
        p_net, p_adv = apply_v12_calibration([score_for_calibration], config.calibration_profile)
        calibrated_net_probability = float(p_net[0])
        calibrated_adverse_probability = float(p_adv[0])
        calibration_status = "calibrated"
    elif config.require_calibrated_probability:
        calibration_status = "missing_profile"

    # Gross move estimate is conservative: use observed short-term range and activity,
    # then haircut for entry lag.  This is deliberately not a future-return label.
    expected_move = max(0.0, 0.25 * abs(float(row.ret_3)) + 0.20 * float(row.range_pct) + 0.10 * max(0.0, float(row.range_ratio_20) - 1.0))
    consumed = abs(float(row.ret_1)) * lag
    remaining_edge = (model_expected_edge if model_expected_edge is not None else
                      expected_move - consumed - config.round_trip_cost_pct - config.entry_slippage_pct)

    edge_pass = remaining_edge >= config.min_remaining_edge_pct
    score_pass = confidence >= config.min_signal_score
    probability_pass = True
    risk_pass = True
    if config.require_calibrated_probability:
        probability_pass = calibration_status == "calibrated" and calibrated_net_probability is not None and calibrated_net_probability >= config.min_calibrated_net_probability
        risk_pass = calibration_status == "calibrated" and calibrated_adverse_probability is not None and calibrated_adverse_probability <= config.max_adverse_probability
    relative_pass = True
    final_direction = raw_direction if edge_pass and score_pass and probability_pass and risk_pass else 0
    if model_direction is not None:
        final_direction = model_direction if (model_direction != 0 and model_expected_edge is not None and model_expected_edge >= config.min_model_expected_edge_pct and model_target_probability is not None and model_target_probability >= config.min_model_target_probability) else 0
    rejection_reasons = []
    if raw_direction == 0:
        rejection_reasons.append("direction")
    if not edge_pass:
        rejection_reasons.append("remaining_edge")
    if not score_pass:
        rejection_reasons.append("score")
    if config.require_calibrated_probability and not probability_pass:
        rejection_reasons.append("calibration_probability")
    if config.require_calibrated_probability and not risk_pass:
        rejection_reasons.append("adverse_risk")

    if final_direction != 0 and config.require_relative_strength:
        rs_basis = float(row.rs_1) if abs(float(row.rs_1)) >= abs(float(row.market_ret_1)) else float(final_direction)
        relative_pass = np.sign(final_direction) == np.sign(rs_basis)
        if not relative_pass:
            rejection_reasons.append("relative_strength")
            final_direction = 0

    l2_bonus = 0.0
    l2_available = config.use_l2_when_available and any(str(c).lower() in row.index for c in ("ofi", "imbalance_l5", "imbalance_l10", "microprice_edge"))
    l2_pass = True
    micro_signal = 0.0
    if l2_available:
        ofi = float(row.get("ofi", 0.0))
        imb = float(row.get("imbalance_l10", row.get("imbalance_l5", 0.0)))
        micro = float(row.get("microprice_edge", 0.0))
        micro_signal = 0.45 * ofi + 0.35 * imb + 0.20 * micro
        if final_direction and micro_signal and np.sign(micro_signal) != np.sign(final_direction):
            l2_pass = False
            rejection_reasons.append("l2_agreement")
            final_direction = 0
        l2_bonus = abs(micro_signal)
        confidence = min(100.0, confidence + 8.0 * l2_bonus)

    return {
        "direction": int(final_direction),
        "raw_direction": int(raw_direction),
        "side": "LONG" if final_direction > 0 else "SHORT" if final_direction < 0 else "NONE",
        "confidence": float(round(confidence, 4)),
        "raw_confidence": float(round(raw_confidence, 4)),
        "confidence_semantics": "model_score_0_100_not_probability",
        "calibration_status": calibration_status,
        "v12_p_net_positive": None if calibrated_net_probability is None else float(round(calibrated_net_probability, 6)),
        "v12_p_adverse_stop": None if calibrated_adverse_probability is None else float(round(calibrated_adverse_probability, 6)),
        "v12_probability_pass": bool(probability_pass),
        "v12_adverse_risk_pass": bool(risk_pass),
        "expected_move_pct": float(round(expected_move, 6)),
        "remaining_edge_pct": float(round(remaining_edge, 6)),
        "v12_model_direction": model_direction,
        "v12_model_expected_net_edge_pct": model_expected_edge,
        "v12_model_expected_long_net_edge_pct": model_long_edge,
        "v12_model_expected_short_net_edge_pct": model_short_edge,
        "v12_model_target_probability": model_target_probability,
        "entry_lag_bars": lag,
        "edge_pass": bool(edge_pass),
        "score_pass": bool(score_pass),
        "relative_strength_pass": bool(relative_pass),
        "l2_pass": bool(l2_pass),
        "rejection_reason": ",".join(dict.fromkeys(rejection_reasons)) if rejection_reasons else None,
        "l2_available": bool(l2_available),
        "l2_bonus": float(round(l2_bonus, 6)),
        "micro_signal": float(round(micro_signal, 6)),
        "components": {k: round(v, 6) for k, v in parts.items()},
    }


def simulate_first_touch(bars: pd.DataFrame, entry_price: float, side: int, config: ScalperConfig = ScalperConfig()) -> Dict[str, Any]:
    """Simulate the shared V12 economic exit policy without future leakage."""
    if side not in (1, -1):
        return {"exit_reason": "NO_TRADE", "net_pct": 0.0, "gross_pct": 0.0, "bars_held": 0}
    target = entry_price * (1 + side * config.target_pct / 100.0)
    stop = entry_price * (1 - side * config.protection_pct / 100.0)
    cost = config.round_trip_cost_pct + config.entry_slippage_pct
    arm_net = 0.075
    trail_gross = 0.075
    peak_gross = 0.0
    armed = False
    horizon = bars.head(config.max_hold_bars)
    for i, (_, bar) in enumerate(horizon.iterrows(), start=1):
        hi, lo = float(bar.high), float(bar.low)
        target_hit = hi >= target if side > 0 else lo <= target
        stop_hit = lo <= stop if side > 0 else hi >= stop
        if stop_hit:
            return _finalize(-config.protection_pct, "STOP", i, config)
        if target_hit:
            return _finalize(config.target_pct, "TARGET", i, config)
        close = float(bar.close)
        gross = side * ((close / entry_price) - 1.0) * 100.0
        net = gross - cost
        peak_gross = max(peak_gross, gross)
        if net >= arm_net:
            armed = True
        if armed and gross <= peak_gross - trail_gross:
            return _finalize(gross, "V12_EARLY_PROFIT_LOCK", i, config)
    last_close = float(horizon.iloc[-1].close) if len(horizon) else entry_price
    gross = side * ((last_close / entry_price) - 1.0) * 100.0
    return _finalize(gross, "TIME_EXIT", min(config.max_hold_bars, len(horizon)), config)

def _finalize(gross_pct: float, reason: str, bars_held: int, config: ScalperConfig) -> Dict[str, Any]:
    net = gross_pct - config.round_trip_cost_pct - config.entry_slippage_pct
    return {"exit_reason": reason, "gross_pct": float(round(gross_pct, 6)), "net_pct": float(round(net, 6)), "bars_held": int(bars_held)}


def config_dict(config: ScalperConfig = ScalperConfig()) -> Dict[str, Any]:
    return asdict(config)
