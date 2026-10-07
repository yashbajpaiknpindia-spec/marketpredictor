"""V3 Alpha-First core primitives.

Dependency-light research primitives for an intraday NSE/BSE cash-equity engine.
No strategy is considered proven here. Candidate generation is deliberately separated
from alpha prediction, economics, portfolio selection and execution.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, Iterable, List, Optional, Tuple
import math

import numpy as np
import pandas as pd

ENGINE_VERSION = "v3.1.0-data-first-alpha-first"


@dataclass(frozen=True)
class DataContract:
    source: str
    symbol_count: int
    session_count: int
    min_session: str
    max_session: str
    has_1m: bool
    has_5m: bool
    has_index: bool
    has_vix: bool
    has_sector: bool
    has_pit_universe: bool
    has_bid_ask: bool
    has_tick: bool
    quality_status: str
    notes: str


@dataclass(frozen=True)
class Candidate:
    candidate_id: str
    timestamp: str
    session_date: str
    symbol: str
    direction: str
    family: str
    entry_reference: float
    regime: Dict[str, Any]
    features: Dict[str, float]


@dataclass(frozen=True)
class TradePrediction:
    p_up: float
    p_down: float
    p_flat: float
    expected_remaining_move_pct: float
    expected_mfe_pct: float
    expected_mae_pct: float
    p_target_before_stop: float
    expected_hold_bars: float


@dataclass(frozen=True)
class CostBreakdown:
    entry_turnover: float
    exit_turnover: float
    brokerage: float
    taxes_and_fees: float
    mandatory_cost_total: float
    slippage: float
    total_cost_with_slippage: float
    mandatory_cost_pct_of_notional: float
    slippage_pct_of_notional: float


@dataclass(frozen=True)
class EconomicDecision:
    expected_gross_pct: float
    expected_gross_rupees: float
    mandatory_cost_rupees: float
    mandatory_cost_pct: float
    economic_hurdle_pct: float
    expected_net_before_slippage_pct: float
    expected_net_before_slippage_rupees: float
    expected_net_under_stress_pct: float
    accepted: bool
    reason: str


def canonical_symbol(symbol: str) -> str:
    s = str(symbol or "").strip().upper()
    for suffix in (".NS", ".BO"):
        if s.endswith(suffix):
            return s[:-3]
    return s


def ensure_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    x = df.copy()
    rename = {str(c).lower(): c for c in x.columns}
    required = ["open", "high", "low", "close", "volume"]
    if not all(k in rename for k in required):
        raise ValueError(f"Missing OHLCV columns; required={required}")
    out = pd.DataFrame({k.capitalize(): pd.to_numeric(x[rename[k]], errors="coerce") for k in required})
    if isinstance(x.index, pd.DatetimeIndex):
        out.index = x.index
    elif "timestamp" in rename:
        idx = pd.to_datetime(x[rename["timestamp"]], errors="coerce", utc=True)
        out.index = idx
    else:
        raise ValueError("DataFrame needs a DatetimeIndex or timestamp column")
    out = out[~out.index.isna()].sort_index()
    return out.dropna(subset=["Open", "High", "Low", "Close"])


def data_quality_report(df: pd.DataFrame, *, expected_minutes_per_session: int = 375) -> Dict[str, Any]:
    x = ensure_ohlcv(df)
    if x.empty:
        return {"quality_status": "EMPTY", "rows": 0}
    idx = x.index
    dates = pd.Index(idx.date).unique()
    duplicate_count = int(idx.duplicated().sum())
    nonmonotonic = bool(not idx.is_monotonic_increasing)
    invalid_ohlc = int(((x["High"] < x[["Open", "Close"]].max(axis=1)) | (x["Low"] > x[["Open", "Close"]].min(axis=1))).sum())
    negative_volume = int((x["Volume"] < 0).sum())
    return {
        "quality_status": "PASS" if duplicate_count == 0 and not nonmonotonic and invalid_ohlc == 0 and negative_volume == 0 else "FAIL",
        "rows": int(len(x)),
        "sessions": int(len(dates)),
        "first_timestamp": idx.min().isoformat(),
        "last_timestamp": idx.max().isoformat(),
        "duplicate_timestamps": duplicate_count,
        "nonmonotonic": nonmonotonic,
        "invalid_ohlc_rows": invalid_ohlc,
        "negative_volume_rows": negative_volume,
        "expected_minutes_per_session": expected_minutes_per_session,
    }


def session_vwap(df: pd.DataFrame) -> pd.Series:
    x = ensure_ohlcv(df)
    typical = (x["High"] + x["Low"] + x["Close"]) / 3.0
    vol = x["Volume"].fillna(0.0)
    return (typical * vol).cumsum() / vol.cumsum().replace(0, np.nan)


def true_range(df: pd.DataFrame) -> pd.Series:
    x = ensure_ohlcv(df)
    prev = x["Close"].shift(1)
    return pd.concat([
        x["High"] - x["Low"],
        (x["High"] - prev).abs(),
        (x["Low"] - prev).abs(),
    ], axis=1).max(axis=1)


def atr_percent(df: pd.DataFrame, window: int = 14) -> pd.Series:
    x = ensure_ohlcv(df)
    atr = true_range(x).rolling(window, min_periods=2).mean()
    return atr / x["Close"].replace(0, np.nan) * 100.0


def build_forward_labels(df: pd.DataFrame, *, horizon_bars: Iterable[int] = (1, 2, 3, 6, 9, 12)) -> pd.DataFrame:
    """Build causal forward-path labels.

    Row t is the decision point. Entry is the next executable bar open.
    Future data is used only for labels, never features.
    """
    x = ensure_ohlcv(df)
    out = x.copy()
    out["entry_next_open"] = x["Open"].shift(-1)
    for h in horizon_bars:
        # Close after h completed bars from the next executable open.
        out[f"ef{h}"] = x["Close"].shift(-(h + 1)) / out["entry_next_open"] - 1.0
        highs = []
        lows = []
        arr = x.reset_index(drop=True)
        for i in range(len(arr)):
            entry_i = i + 1
            end_i = i + 1 + h
            if end_i >= len(arr):
                highs.append(np.nan); lows.append(np.nan)
                continue
            ep = float(arr.loc[entry_i, "Open"])
            if ep <= 0:
                highs.append(np.nan); lows.append(np.nan)
                continue
            highs.append(float(arr.loc[entry_i:end_i, "High"].max()) / ep - 1.0)
            lows.append(float(arr.loc[entry_i:end_i, "Low"].min()) / ep - 1.0)
        out[f"mfe{h}"] = highs
        out[f"mae{h}"] = lows
    return out


def directional_labels(label_df: pd.DataFrame, horizon: int = 6) -> pd.Series:
    y = pd.to_numeric(label_df[f"ef{horizon}"], errors="coerce")
    return pd.Series(np.where(y > 0, 1, np.where(y < 0, -1, 0)), index=label_df.index, dtype="int8")


def cost_breakdown(*, notional: float, brokerage_rate: float, brokerage_min: float,
                   exchange_and_other_rate: float, gst_rate: float,
                   stt_rate: float, stamp_rate: float, slippage_bps: float) -> CostBreakdown:
    """Generic configurable round-trip cost model.

    Rates are inputs. No broker/tax rate is hard-coded by V3 core.
    """
    n = max(float(notional), 0.0)
    brokerage_rate_amount = n * max(float(brokerage_rate), 0.0)
    # brokerage_min is retained for backward-compatible callers but its historical
    # meaning was incorrect: Indian intraday brokerage is rate-or-cap, whichever
    # is lower (when configured that way).  The V3.2.1 canonical runtime passes the
    # same cap through this argument, so research and runtime now share one formula.
    cap = max(float(brokerage_min), 0.0)
    brokerage = min(cap, brokerage_rate_amount) if cap > 0 and brokerage_rate_amount > 0 else max(cap, brokerage_rate_amount)
    exchange_other = n * max(float(exchange_and_other_rate), 0.0)
    stt = n * max(float(stt_rate), 0.0)
    stamp = n * max(float(stamp_rate), 0.0)
    taxable_base = brokerage + exchange_other
    gst = taxable_base * max(float(gst_rate), 0.0)
    taxes_and_fees = exchange_other + stt + stamp + gst
    mandatory = brokerage + taxes_and_fees
    slippage = n * max(float(slippage_bps), 0.0) / 10000.0
    return CostBreakdown(
        entry_turnover=n,
        exit_turnover=n,
        brokerage=brokerage,
        taxes_and_fees=taxes_and_fees,
        mandatory_cost_total=mandatory,
        slippage=slippage,
        total_cost_with_slippage=mandatory + slippage,
        mandatory_cost_pct_of_notional=(mandatory / n * 100.0) if n else 0.0,
        slippage_pct_of_notional=(slippage / n * 100.0) if n else 0.0,
    )


def economic_gate(*, expected_gross_pct: float, notional: float,
                  cost: CostBreakdown, minimum_net_edge_pct: float = 0.03,
                  minimum_edge_to_cost_multiple: float = 1.5,
                  stress_net_edge_pct: float = 0.0) -> EconomicDecision:
    gross_rupees = max(float(expected_gross_pct), 0.0) / 100.0 * max(float(notional), 0.0)
    mandatory_pct = cost.mandatory_cost_pct_of_notional
    # Expected gross must cover mandatory cost plus a configurable safety multiple.
    hurdle_pct = max(
        float(minimum_net_edge_pct),
        mandatory_pct * (1.0 + max(float(minimum_edge_to_cost_multiple), 0.0)),
    )
    net_before = float(expected_gross_pct) - mandatory_pct
    net_stress = net_before - cost.slippage_pct_of_notional
    # The published hurdle must be the actual acceptance rule.  Previously the
    # function reported a cost-multiple hurdle but accepted trades using only the
    # smaller minimum-net-edge floor, so UI/reporting and execution disagreed.
    accepted = (float(expected_gross_pct) >= hurdle_pct
                and net_stress >= max(float(stress_net_edge_pct), 0.0))
    reason = "economic_edge_cleared" if accepted else "economic_hurdle_not_cleared"
    return EconomicDecision(
        expected_gross_pct=float(expected_gross_pct),
        expected_gross_rupees=gross_rupees,
        mandatory_cost_rupees=cost.mandatory_cost_total,
        mandatory_cost_pct=mandatory_pct,
        economic_hurdle_pct=hurdle_pct,
        expected_net_before_slippage_pct=net_before,
        expected_net_before_slippage_rupees=gross_rupees - cost.mandatory_cost_total,
        expected_net_under_stress_pct=net_stress,
        accepted=accepted,
        reason=reason,
    )


def candidate_record(*, candidate_id: str, timestamp: Any, session_date: Any,
                     symbol: str, direction: str, family: str,
                     entry_reference: float, regime: Dict[str, Any],
                     features: Dict[str, float]) -> Dict[str, Any]:
    return asdict(Candidate(
        candidate_id=str(candidate_id),
        timestamp=pd.Timestamp(timestamp).isoformat(),
        session_date=str(session_date),
        symbol=canonical_symbol(symbol),
        direction=str(direction).upper(),
        family=str(family),
        entry_reference=float(entry_reference),
        regime=dict(regime or {}),
        features={str(k): float(v) for k, v in (features or {}).items() if _finite(v)},
    ))


def _finite(x: Any) -> bool:
    try:
        return bool(np.isfinite(float(x)))
    except Exception:
        return False


def summarize_returns(returns: Iterable[float]) -> Dict[str, Any]:
    r = np.asarray(list(returns), dtype=float)
    r = r[np.isfinite(r)]
    if len(r) == 0:
        return {"n": 0}
    wins = r[r > 0]
    losses = r[r < 0]
    gross_profit = float(wins.sum()) if len(wins) else 0.0
    gross_loss = float(-losses.sum()) if len(losses) else 0.0
    pf = gross_profit / gross_loss if gross_loss > 0 else (math.inf if gross_profit > 0 else 0.0)
    return {
        "n": int(len(r)),
        "mean": float(r.mean()),
        "median": float(np.median(r)),
        "win_rate": float((r > 0).mean()),
        "profit_factor": float(pf),
        "expectancy": float(r.mean()),
        "best": float(r.max()),
        "worst": float(r.min()),
    }
