"""V2 intraday engine: six-phase, data-first research and live/paper/replay gates.

The module is intentionally dependency-light and side-effect free. It can run beside the
existing Flask application and is shared by live, paper and replay. Historical research
can pass a frozen edge model; live/paper may load the same frozen model from JSON.

Important honesty rule: lack of point-in-time universe/sector history is represented as a
limitation. This module never invents historical membership, bid/ask, tick, pre-open or event
information that is not actually present in the supplied data.
"""
from __future__ import annotations

import datetime as dt
import json
import math
import os
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

ENGINE_VERSION = "v2.3.0-edge-first-economic-gate"
SIGNAL_INTERVAL = "5m"
EXECUTION_INTERVAL = "1m"
COMPAT_EXECUTION_INTERVAL = "5m"  # used only when real 1m history is unavailable
MODEL_MIN_SAMPLES = int(os.environ.get("V2_MODEL_MIN_SAMPLES", "30"))
EDGE_HURDLE_PCT = float(os.environ.get("V2_EDGE_HURDLE_PCT", "0.12"))
MIN_NET_EDGE_PCT = float(os.environ.get("V2_MIN_NET_EDGE_PCT", "0.03"))
SLIPPAGE_STRESS_BPS = float(os.environ.get("V2_SLIPPAGE_STRESS_BPS", "5.0"))
MIN_EDGE_TO_COST_MULTIPLE = float(os.environ.get("V2_MIN_EDGE_TO_COST_MULTIPLE", "1.5"))
ENTRY_CHASE_MAX_PCT = float(os.environ.get("V2_ENTRY_CHASE_MAX_PCT", "0.30"))
MAX_HOLD_BARS = int(os.environ.get("V2_MAX_HOLD_BARS", "12"))
RISK_MIN_PCT = float(os.environ.get("V2_RISK_MIN_PCT", "0.35"))
RISK_MAX_PCT = float(os.environ.get("V2_RISK_MAX_PCT", "0.80"))
TARGET_R = float(os.environ.get("V2_TARGET_R", "1.60"))
SLIPPAGE_BPS = float(os.environ.get("INTRADAY_COST_SLIPPAGE_BPS", os.environ.get("INTRADAY_REPLAY_ENTRY_SLIPPAGE_BPS", "5")))


@dataclass
class DatasetContract:
    source: str
    signal_interval: str
    execution_interval: str
    is_point_in_time_universe: bool
    is_point_in_time_sector: bool
    has_index_data: bool
    has_vix_data: bool
    has_bid_ask: bool
    has_tick_data: bool
    has_preopen: bool
    has_events: bool
    quality_status: str
    note: str = ""

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


def current_contract(source: str, *, one_minute_available: bool, index_available: bool = False,
                     vix_available: bool = False, event_available: bool = False) -> DatasetContract:
    return DatasetContract(
        source=source,
        signal_interval=SIGNAL_INTERVAL,
        execution_interval=EXECUTION_INTERVAL if one_minute_available else COMPAT_EXECUTION_INTERVAL,
        is_point_in_time_universe=False,
        is_point_in_time_sector=False,
        has_index_data=index_available,
        has_vix_data=vix_available,
        has_bid_ask=False,
        has_tick_data=False,
        has_preopen=False,
        has_events=event_available,
        quality_status="COMPATIBLE" if not one_minute_available else "READY",
        note=("1m execution unavailable in supplied historical bundle; research uses 5m compatibility mode. "
              "No synthetic 1m candles are created.") if not one_minute_available else "1m execution + completed 5m signals.",
    )


def canonical_symbol(symbol: str) -> str:
    s = str(symbol or "").upper().strip()
    for suffix in (".NS", ".BO"):
        if s.endswith(suffix):
            s = s[:-3]
    return s


def resample_1m_to_5m(df: pd.DataFrame) -> pd.DataFrame:
    """Resample REAL 1-minute OHLCV into completed 5-minute NSE bars only.

    A 5-minute signal is never allowed to use a still-forming 1-minute bucket.
    If fewer than five source observations exist in the final bucket, that bucket
    is dropped.  This is the canonical path used by Live/Paper/Replay feature code.
    """
    if df is None or df.empty:
        return pd.DataFrame()
    x = df.copy()
    if not isinstance(x.index, pd.DatetimeIndex):
        if "timestamp" in x.columns:
            x["timestamp"] = pd.to_datetime(x["timestamp"], utc=True, errors="coerce")
            x = x.set_index("timestamp")
        else:
            return pd.DataFrame()
    x = x.sort_index()
    cols = {str(c).lower(): c for c in x.columns}
    required = {"open", "high", "low", "close", "volume"}
    if not required.issubset(cols):
        return pd.DataFrame()
    o, h, l, c, v = [cols[k] for k in ("open", "high", "low", "close", "volume")]
    base = pd.DataFrame({"Open": pd.to_numeric(x[o], errors="coerce"),
                         "High": pd.to_numeric(x[h], errors="coerce"),
                         "Low": pd.to_numeric(x[l], errors="coerce"),
                         "Close": pd.to_numeric(x[c], errors="coerce"),
                         "Volume": pd.to_numeric(x[v], errors="coerce").fillna(0.0)})
    count = base["Close"].resample("5min", label="left", closed="left").count()
    out = pd.DataFrame({
        "Open": base["Open"].resample("5min", label="left", closed="left").first(),
        "High": base["High"].resample("5min", label="left", closed="left").max(),
        "Low": base["Low"].resample("5min", label="left", closed="left").min(),
        "Close": base["Close"].resample("5min", label="left", closed="left").last(),
        "Volume": base["Volume"].resample("5min", label="left", closed="left").sum(),
    })
    out = out[(count >= 5)].dropna(subset=["Open", "High", "Low", "Close"]).copy()
    return out


def normalize_signal_history(hist: Any, source_interval: Optional[str] = None) -> pd.DataFrame:
    """Return canonical completed 5-minute signal bars from either 1m or 5m input.

    This fixes a critical runtime mismatch: Live/Paper can fetch 1m bars for execution,
    while the V2 signal layer must measure 5m/15m/30m features in actual 5-minute units.
    """
    x = hist.copy() if isinstance(hist, pd.DataFrame) else pd.DataFrame(hist or {})
    x = _v2_hist_frame(x)
    if x.empty:
        return x
    interval = str(source_interval or "").strip().lower()
    if interval in {"1m", "1min", "1minute", "minute"}:
        return resample_1m_to_5m(x)
    # If interval is unspecified, infer from median timestamp spacing conservatively.
    if interval == "":
        try:
            idx = x.index if isinstance(x.index, pd.DatetimeIndex) else pd.to_datetime(x.index, errors="coerce")
            diffs = pd.Series(idx[1:] - idx[:-1]).dropna().dt.total_seconds() / 60.0
            interval = "1m" if (not diffs.empty and float(diffs.median()) <= 1.5) else "5m"
        except Exception:
            interval = "5m"
    return x.sort_index().copy()


def _safe_pct(a: float, b: float) -> float:
    try:
        if not b:
            return 0.0
        return (float(a) / float(b) - 1.0) * 100.0
    except Exception:
        return 0.0


def true_range(df: pd.DataFrame) -> pd.Series:
    prev_close = df["Close"].shift(1)
    return pd.concat([
        df["High"] - df["Low"],
        (df["High"] - prev_close).abs(),
        (df["Low"] - prev_close).abs(),
    ], axis=1).max(axis=1)


def atr_pct(df: pd.DataFrame, n: int = 14) -> float:
    if df is None or len(df) < 2:
        return 0.5
    tr = true_range(df)
    atr = float(tr.rolling(n, min_periods=2).mean().iloc[-1])
    px = float(df["Close"].iloc[-1])
    return (atr / px * 100.0) if px > 0 else 0.5


def session_vwap(df: pd.DataFrame) -> pd.Series:
    price = (df["High"] + df["Low"] + df["Close"]) / 3.0
    vol = df["Volume"].fillna(0).astype(float)
    cum = (price * vol).cumsum()
    cv = vol.cumsum().replace(0, np.nan)
    return cum / cv


def _rvol(df: pd.DataFrame, n: int = 12) -> float:
    if len(df) < 4:
        return 1.0
    base = float(df["Volume"].iloc[:-1].tail(n).median())
    if base <= 0:
        return 1.0
    return float(df["Volume"].iloc[-1]) / base


def derive_market_state(day_frames: Dict[str, pd.DataFrame], upto: int,
                        sector_by_symbol: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    closes: List[float] = []
    returns_5: List[float] = []
    above_vwap = 0
    valid = 0
    sectors: Dict[str, List[float]] = {}
    for sym, df in day_frames.items():
        x = df.iloc[:upto + 1]
        if len(x) < 3:
            continue
        try:
            last = float(x["Close"].iloc[-1]); prev = float(x["Close"].iloc[-2])
            closes.append(last)
            returns_5.append(_safe_pct(last, prev))
            vw = session_vwap(x).iloc[-1]
            if np.isfinite(vw) and last > float(vw):
                above_vwap += 1
            valid += 1
            sec = (sector_by_symbol or {}).get(canonical_symbol(sym)) or "Unknown"
            sectors.setdefault(sec, []).append(_safe_pct(last, prev))
        except Exception:
            continue
    breadth = (above_vwap / valid * 100.0) if valid else 50.0
    med5 = float(np.median(returns_5)) if returns_5 else 0.0
    dispersion = float(np.std(returns_5)) if returns_5 else 0.0
    sector_strength = {
        sec: round(float(np.median(vals)), 4) for sec, vals in sectors.items() if vals
    }
    if breadth >= 65 and med5 > 0.03:
        market_regime = "strong_bull"
    elif breadth >= 55 and med5 >= 0:
        market_regime = "weak_bull"
    elif breadth <= 35 and med5 < -0.03:
        market_regime = "strong_bear"
    elif breadth <= 45 and med5 <= 0:
        market_regime = "weak_bear"
    else:
        market_regime = "range"
    if dispersion >= 0.45:
        vol_regime = "high_dispersion"
    elif dispersion <= 0.18:
        vol_regime = "low_dispersion"
    else:
        vol_regime = "normal_dispersion"
    return {
        "market_regime": market_regime,
        "volatility_regime": vol_regime,
        "breadth_pct_above_vwap": round(breadth, 2),
        "median_5m_return_pct": round(med5, 4),
        "cross_sectional_dispersion_pct": round(dispersion, 4),
        "sector_strength": sector_strength,
        "confidence": round(min(99.0, max(1.0, abs(breadth - 50.0) * 2 + abs(med5) * 10)), 1),
    }


def derive_stock_features(df: pd.DataFrame, i: int, prev_close: Optional[float], market_state: Dict[str, Any],
                          sector_name: Optional[str] = None) -> Dict[str, Any]:
    x = df.iloc[:i + 1].copy()
    last = float(x["Close"].iloc[-1]); op = float(x["Open"].iloc[0])
    vw_series = session_vwap(x)
    vw = float(vw_series.iloc[-1]) if np.isfinite(vw_series.iloc[-1]) else last
    ret5 = _safe_pct(last, float(x["Close"].iloc[-2])) if len(x) >= 2 else 0.0
    ret15 = _safe_pct(last, float(x["Close"].iloc[-4])) if len(x) >= 4 else 0.0
    ret30 = _safe_pct(last, float(x["Close"].iloc[-7])) if len(x) >= 7 else 0.0
    ret_open = _safe_pct(last, op)
    gap = _safe_pct(op, prev_close) if prev_close else 0.0
    prior = x.iloc[:-1]
    first3 = x.iloc[:3]
    first6 = x.iloc[:6]
    recent6 = x.iloc[-7:-1] if len(x) >= 8 else prior.tail(6)
    recent12 = x.iloc[-13:-1] if len(x) >= 14 else prior.tail(12)
    breakout_up = bool(len(first3) >= 3 and last > float(first3["High"].max()))
    breakout_down = bool(len(first3) >= 3 and last < float(first3["Low"].min()))
    prior6_high = float(recent6["High"].max()) if not recent6.empty else last
    prior6_low = float(recent6["Low"].min()) if not recent6.empty else last
    compression_ratio = 1.0
    if len(recent6) >= 3 and len(recent12) >= 6:
        a = float(recent6["High"].max() - recent6["Low"].min())
        b = float(recent12["High"].max() - recent12["Low"].min())
        compression_ratio = a / b if b > 0 else 1.0
    atr = atr_pct(x)
    vwap_dev = _safe_pct(last, vw)
    sector_mom = None
    if sector_name:
        sector_mom = (market_state.get("sector_strength") or {}).get(sector_name)
    return {
        "last": last, "open": op, "gap_pct": gap, "ret_5m_pct": ret5, "ret_15m_pct": ret15,
        "ret_30m_pct": ret30, "ret_60m_pct": _safe_pct(last, float(x["Close"].iloc[-13])) if len(x) >= 13 else 0.0, "ret_from_open_pct": ret_open, "vwap": vw, "vwap_dev_pct": vwap_dev,
        "rvol": _rvol(x), "atr_pct": atr, "breakout_up": breakout_up, "breakout_down": breakout_down,
        "prior6_high": prior6_high, "prior6_low": prior6_low, "compression_ratio": compression_ratio,
        "sector_momentum_pct": sector_mom, "market_breadth": market_state.get("breadth_pct_above_vwap", 50),
        "market_regime": market_state.get("market_regime", "range"),
        "volatility_regime": market_state.get("volatility_regime", "normal_dispersion"),
        "distance_from_first3_high_pct": _safe_pct(last, float(first3["High"].max())) if len(first3) >= 3 else 0.0,
        "distance_from_first3_low_pct": _safe_pct(last, float(first3["Low"].min())) if len(first3) >= 3 else 0.0,
    }


def signal_candidates(f: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Technical signal families. All decisions use only the current prefix of bars."""
    out: List[Dict[str, Any]] = []
    rvol = f["rvol"]
    bull_market = f["market_breadth"] >= 55 and f["market_regime"] in {"strong_bull", "weak_bull", "range"}
    bear_market = f["market_breadth"] <= 45 and f["market_regime"] in {"strong_bear", "weak_bear", "range"}
    # A: gap continuation
    if f["gap_pct"] >= 0.45 and f["ret_from_open_pct"] >= 0.30 and f["ret_15m_pct"] > 0 and f["vwap_dev_pct"] > 0 and rvol >= 1.15:
        out.append({"family": "gap_continuation", "side": "LONG", "strength": 0.8})
    if f["gap_pct"] <= -0.45 and f["ret_from_open_pct"] <= -0.30 and f["ret_15m_pct"] < 0 and f["vwap_dev_pct"] < 0 and rvol >= 1.15:
        out.append({"family": "gap_continuation", "side": "SHORT", "strength": 0.8})
    # B: opening-range expansion (first 15m = three bars)
    if f["breakout_up"] and f["ret_15m_pct"] > 0.15 and rvol >= 1.20 and bull_market:
        out.append({"family": "opening_range_expansion", "side": "LONG", "strength": 0.9})
    if f["breakout_down"] and f["ret_15m_pct"] < -0.15 and rvol >= 1.20 and bear_market:
        out.append({"family": "opening_range_expansion", "side": "SHORT", "strength": 0.9})
    # C: cross-sectional momentum proxy (ranking is supplied by caller as momentum_rank)
    if f.get("momentum_rank", 0.5) >= 0.90 and f["ret_15m_pct"] > 0.20 and f["vwap_dev_pct"] > 0 and bull_market:
        out.append({"family": "cross_sectional_momentum", "side": "LONG", "strength": f.get("momentum_rank", 0.9)})
    if f.get("momentum_rank", 0.5) <= 0.10 and f["ret_15m_pct"] < -0.20 and f["vwap_dev_pct"] < 0 and bear_market:
        out.append({"family": "cross_sectional_momentum", "side": "SHORT", "strength": 1.0 - f.get("momentum_rank", 0.1)})
    # D: market -> sector -> stock alignment
    sec = f.get("sector_momentum_pct")
    if sec is not None and sec > 0.05 and f["ret_15m_pct"] > 0.20 and f["market_breadth"] >= 58 and f["vwap_dev_pct"] > 0:
        out.append({"family": "market_sector_stock_alignment", "side": "LONG", "strength": 0.85})
    if sec is not None and sec < -0.05 and f["ret_15m_pct"] < -0.20 and f["market_breadth"] <= 42 and f["vwap_dev_pct"] < 0:
        out.append({"family": "market_sector_stock_alignment", "side": "SHORT", "strength": 0.85})
    # E: compression -> expansion
    if f["compression_ratio"] <= 0.65 and f["last"] > f["prior6_high"] and rvol >= 1.25 and f["ret_5m_pct"] > 0.05:
        out.append({"family": "compression_expansion", "side": "LONG", "strength": 0.9})
    if f["compression_ratio"] <= 0.65 and f["last"] < f["prior6_low"] and rvol >= 1.25 and f["ret_5m_pct"] < -0.05:
        out.append({"family": "compression_expansion", "side": "SHORT", "strength": 0.9})
    # F: failed breakout / reclaim
    if len(f.get("_series_close", [])) >= 2:
        prev_close_bar = float(f["_series_close"][-2])
        if prev_close_bar > f["prior6_high"] and f["last"] < prev_close_bar and f["ret_5m_pct"] < 0:
            out.append({"family": "failed_breakout", "side": "SHORT", "strength": 0.8})
        if prev_close_bar < f["prior6_low"] and f["last"] > prev_close_bar and f["ret_5m_pct"] > 0:
            out.append({"family": "failed_breakout", "side": "LONG", "strength": 0.8})
    # G: conditional VWAP mean reversion
    if f["vwap_dev_pct"] <= -0.85 and f["ret_5m_pct"] > 0.08 and f["market_regime"] in {"range", "weak_bull"}:
        out.append({"family": "conditional_mean_reversion", "side": "LONG", "strength": 0.75})
    if f["vwap_dev_pct"] >= 0.85 and f["ret_5m_pct"] < -0.08 and f["market_regime"] in {"range", "weak_bear"}:
        out.append({"family": "conditional_mean_reversion", "side": "SHORT", "strength": 0.75})
    return out


def estimated_round_trip_cost_pct(entry: float, exit_px: Optional[float] = None, qty: Optional[int] = None,
                                  slippage_bps: Optional[float] = None) -> Dict[str, float]:
    """Estimate mandatory fees and execution stress as percentages of entry notional.

    This is deliberately separate from strategy P&L.  The economic gate can therefore
    reject a signal before trading when expected gross opportunity cannot cover friction.
    """
    px = float(entry)
    if px <= 0:
        return {"mandatory_pct": 99.0, "slippage_pct": 99.0, "total_with_slippage_pct": 99.0}
    q = int(qty or max(1, 65000 // px))
    ex = float(exit_px if exit_px is not None and exit_px > 0 else px)
    c = _cost_components(px, ex, q, SLIPPAGE_STRESS_BPS if slippage_bps is None else float(slippage_bps))
    denom = max(px * q, 1e-9)
    mandatory_pct = c["mandatory_fees"] / denom * 100.0
    slippage_pct = c["slippage"] / denom * 100.0
    return {
        "mandatory_pct": mandatory_pct,
        "slippage_pct": slippage_pct,
        "total_with_slippage_pct": mandatory_pct + slippage_pct,
    }


def economic_edge_check(expected_gross_pct: float, entry: float, *,
                        min_net_edge_pct: float = MIN_NET_EDGE_PCT,
                        stress_slippage_bps: float = SLIPPAGE_STRESS_BPS) -> Dict[str, Any]:
    costs = estimated_round_trip_cost_pct(entry, slippage_bps=stress_slippage_bps)
    expected_net = float(expected_gross_pct) - costs["mandatory_pct"]
    expected_net_stress = expected_net - costs["slippage_pct"]
    hurdle = max(float(min_net_edge_pct), float(costs["mandatory_pct"] * MIN_EDGE_TO_COST_MULTIPLE - costs["mandatory_pct"]))
    return {
        **costs,
        "expected_gross_pct": float(expected_gross_pct),
        "expected_net_before_slippage_pct": expected_net,
        "expected_net_stress_pct": expected_net_stress,
        "economic_hurdle_pct": hurdle,
        "passes_before_slippage": expected_net >= hurdle,
        "passes_stress": expected_net_stress >= hurdle,
    }


def _cost_components(entry: float, exit_px: float, qty: int, slippage_bps: float = SLIPPAGE_BPS) -> Dict[str, float]:
    buy = max(0.0, entry * qty)
    sell = max(0.0, exit_px * qty)
    turnover = buy + sell
    percent_brokerage_buy = buy * 0.0003
    percent_brokerage_sell = sell * 0.0003
    brokerage = min(20.0, percent_brokerage_buy) + min(20.0, percent_brokerage_sell)
    stt = sell * 0.00025
    exchange = turnover * 0.0000307
    sebi = turnover * 0.000001
    stamp = buy * 0.00003
    gst = (brokerage + exchange + sebi) * 0.18
    slippage = turnover * float(slippage_bps) / 10000.0
    total = brokerage + stt + exchange + sebi + stamp + gst + slippage
    return {"brokerage": brokerage, "stt": stt, "exchange": exchange, "sebi": sebi, "stamp": stamp,
            "gst": gst, "slippage": slippage, "mandatory_fees": brokerage + stt + exchange + sebi + stamp + gst,
            "total": total}


def net_pnl(entry: float, exit_px: float, qty: int, side: str) -> Tuple[float, Dict[str, float]]:
    gross = (exit_px - entry) * qty if side == "LONG" else (entry - exit_px) * qty
    c = _cost_components(entry, exit_px, qty)
    return float(gross - c["total"]), c


def risk_profile(entry: float, atr_pct_value: float) -> Tuple[float, float]:
    stop_pct = min(RISK_MAX_PCT, max(RISK_MIN_PCT, atr_pct_value * 1.20))
    target_pct = min(2.0, stop_pct * TARGET_R)
    return stop_pct, target_pct


def simulate_outcome(day_df: pd.DataFrame, entry_idx: int, side: str, entry: float,
                     stop_pct: float, target_pct: float, max_hold_bars: int = MAX_HOLD_BARS) -> Dict[str, Any]:
    stop = entry * (1 - stop_pct / 100.0) if side == "LONG" else entry * (1 + stop_pct / 100.0)
    target = entry * (1 + target_pct / 100.0) if side == "LONG" else entry * (1 - target_pct / 100.0)
    future = day_df.iloc[entry_idx:min(len(day_df), entry_idx + 1 + max_hold_bars)]
    if future.empty:
        return {"exit_price": entry, "exit_idx": entry_idx, "reason": "no_future_bar", "mfe_pct": 0.0, "mae_pct": 0.0}
    mfe = 0.0; mae = 0.0
    exit_px = float(future["Close"].iloc[-1]); exit_i = future.index[-1]; reason = "time_stop"
    for j, (_, bar) in enumerate(future.iterrows()):
        hi = float(bar["High"]); lo = float(bar["Low"])
        if side == "LONG":
            mfe = max(mfe, (hi - entry) / entry * 100.0)
            mae = max(mae, (entry - lo) / entry * 100.0)
            hit_stop = lo <= stop
            hit_target = hi >= target
        else:
            mfe = max(mfe, (entry - lo) / entry * 100.0)
            mae = max(mae, (hi - entry) / entry * 100.0)
            hit_stop = hi >= stop
            hit_target = lo <= target
        if hit_stop and hit_target:
            # conservative: assume the adverse level was hit first on the same OHLC bar.
            exit_px = stop; reason = "stop_loss_same_bar_conflict"; exit_i = future.index[j]; break
        if hit_stop:
            exit_px = stop; reason = "stop_loss_hit"; exit_i = future.index[j]; break
        if hit_target:
            exit_px = target; reason = "target_hit"; exit_i = future.index[j]; break
        exit_px = float(bar["Close"]); exit_i = future.index[j]
    qty = max(1, int(50000 // entry))
    net, costs = net_pnl(entry, float(exit_px), qty, side)
    notional = entry * qty
    return {
        "exit_price": float(exit_px), "exit_idx": exit_i, "reason": reason, "mfe_pct": float(mfe), "mae_pct": float(mae),
        "qty": qty, "notional": notional, "gross_pnl": (float(exit_px) - entry) * qty if side == "LONG" else (entry - float(exit_px)) * qty,
        "net_pnl": net, "net_return_pct": net / notional * 100.0 if notional else 0.0, "costs": costs,
        "stop_pct": stop_pct, "target_pct": target_pct,
    }


def discover_examples(day_frames: Dict[str, pd.DataFrame], prev_closes: Dict[str, float], sector_map: Dict[str, str],
                      *, first_bar: int = 6) -> List[Dict[str, Any]]:
    examples: List[Dict[str, Any]] = []
    max_len = max((len(df) for df in day_frames.values()), default=0)
    for i in range(first_bar, max_len - 2):
        market_state = derive_market_state(day_frames, i, sector_map)
        raw: List[Tuple[str, Dict[str, Any], float]] = []
        for sym, df in day_frames.items():
            if len(df) <= i + 1:
                continue
            f = derive_stock_features(df, i, prev_closes.get(canonical_symbol(sym)), market_state, sector_map.get(canonical_symbol(sym)))
            f["_series_close"] = df["Close"].iloc[:i + 1].tolist()
            raw.append((sym, f, f["ret_15m_pct"]))
        vals = np.array([r[2] for r in raw], dtype=float)
        for sym, f, _ in raw:
            if len(vals):
                f["momentum_rank"] = float((vals <= f["ret_15m_pct"]).mean())
            sigs = signal_candidates(f)
            if not sigs:
                continue
            # only the strongest family per stock/timestamp; overlapping versions of the same bet are not independent.
            sigs.sort(key=lambda s: (float(s.get("strength", 0)), s["family"]), reverse=True)
            sig = sigs[0]
            entry_idx = i + 1
            entry_bar = day_frames[sym].iloc[entry_idx]
            entry = float(entry_bar["Open"])
            signal_price = float(f["last"])
            fill_gap = abs(entry - signal_price) / signal_price * 100.0 if signal_price else 999.0
            if fill_gap > ENTRY_CHASE_MAX_PCT:
                continue
            sp, tp = risk_profile(entry, f["atr_pct"])
            out = simulate_outcome(day_frames[sym], entry_idx, sig["side"], entry, sp, tp)
            regime_key = f"{f['market_regime']}|{f['volatility_regime']}"
            examples.append({
                "timestamp": str(day_frames[sym].index[i]), "symbol": canonical_symbol(sym), "family": sig["family"],
                "side": sig["side"], "regime_key": regime_key, "market_regime": f["market_regime"],
                "sector": sector_map.get(canonical_symbol(sym)), "signal_strength": float(sig["strength"]),
                "entry": entry, "entry_idx": entry_idx, "fill_gap_pct": fill_gap, "mfe_pct": out["mfe_pct"],
                "mae_pct": out["mae_pct"], "net_return_pct": out["net_return_pct"], "net_pnl": out["net_pnl"],
                "gross_pnl": out["gross_pnl"], "exit_reason": out["reason"], "stop_pct": out["stop_pct"],
                "target_pct": out["target_pct"], "cost_amount": out["costs"]["total"],
            })
    return examples


def fit_edge_model(examples: Sequence[Dict[str, Any]], min_samples: int = MODEL_MIN_SAMPLES) -> Dict[str, Any]:
    groups: Dict[str, List[Dict[str, Any]]] = {}
    families: Dict[str, List[Dict[str, Any]]] = {}
    for e in examples:
        k = f"{e['family']}|{e['regime_key']}|{e['side']}"
        groups.setdefault(k, []).append(e)
        families.setdefault(str(e['family']), []).append(e)
    model: Dict[str, Any] = {"engine_version": ENGINE_VERSION, "groups": {}, "families": {}, "created_from_examples": len(examples)}
    for key, rows in groups.items():
        n = len(rows)
        wins = sum(1 for r in rows if float(r["net_return_pct"]) > 0)
        avg = float(np.mean([float(r["net_return_pct"]) for r in rows])) if rows else 0.0
        pf = _profit_factor(rows)
        model["groups"][key] = {
            "n": n, "win_rate_pct": wins / n * 100.0 if n else 0.0,
            "expectancy_pct": avg, "profit_factor": pf,
            "proven": bool(n >= min_samples and avg > EDGE_HURDLE_PCT and pf > 1.05),
        }
    for fam, rows in families.items():
        n = len(rows); wins = sum(1 for r in rows if float(r["net_return_pct"]) > 0)
        avg = float(np.mean([float(r["net_return_pct"]) for r in rows])) if rows else 0.0
        model["families"][fam] = {"n": n, "win_rate_pct": wins / n * 100.0 if n else 0.0,
                                   "expectancy_pct": avg, "profit_factor": _profit_factor(rows),
                                   "proven": bool(n >= min_samples and avg > EDGE_HURDLE_PCT and _profit_factor(rows) > 1.05)}
    return model


def _profit_factor(rows: Sequence[Dict[str, Any]]) -> float:
    pos = sum(max(0.0, float(r["net_pnl"])) for r in rows)
    neg = sum(-min(0.0, float(r["net_pnl"])) for r in rows)
    if neg <= 0:
        return 99.0 if pos > 0 else 0.0
    return pos / neg


def score_signal_with_model(sig: Dict[str, Any], f: Dict[str, Any], model: Dict[str, Any]) -> Dict[str, Any]:
    regime_key = f"{f['market_regime']}|{f['volatility_regime']}"
    key = f"{sig['family']}|{regime_key}|{sig['side']}"
    g = (model.get("groups") or {}).get(key)
    if g is None:
        g = (model.get("families") or {}).get(sig["family"])
    if not g:
        return {"eligible": False, "reason": "unseen_signal_bucket", "expected_net_edge_pct": None}
    if not bool(g.get("proven")):
        return {"eligible": False, "reason": "bucket_not_proven", "expected_net_edge_pct": float(g.get("expectancy_pct") or 0)}
    return {"eligible": True, "reason": "proven_positive_edge_bucket", "expected_net_edge_pct": float(g.get("expectancy_pct") or 0),
            "win_rate_pct": float(g.get("win_rate_pct") or 0), "profit_factor": float(g.get("profit_factor") or 0),
            "evidence_n": int(g.get("n") or 0), "bucket": key}


def walkforward_splits(days: Sequence[dt.date], train_n: int = 36, valid_n: int = 12) -> Tuple[List[dt.date], List[dt.date], List[dt.date]]:
    ordered = sorted(days)
    train = ordered[:train_n]
    valid = ordered[train_n:train_n + valid_n]
    test = ordered[train_n + valid_n:]
    return train, valid, test


def save_model(model: Dict[str, Any], path: str | Path) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(model, fh, indent=2, sort_keys=True)


def load_model(path: str | Path) -> Optional[Dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return None
    try:
        with open(p, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return None


def evaluate_candidate(f: Dict[str, Any], model: Dict[str, Any]) -> Dict[str, Any]:
    f2 = dict(f)
    sigs = signal_candidates(f2)
    scored = []
    for sig in sigs:
        s = score_signal_with_model(sig, f2, model)
        if s.get("eligible"):
            scored.append({**sig, **s})
    if not scored:
        return {"eligible": False, "signals": sigs, "reason": "no_proven_positive_edge"}
    scored.sort(key=lambda s: float(s.get("expected_net_edge_pct") or 0), reverse=True)
    return {"eligible": True, "selected": scored[0], "signals": sigs}


def robustness_summary(trades: Sequence[Dict[str, Any]], profit_remove_top_n: int = 3) -> Dict[str, Any]:
    if not trades:
        return {"n": 0, "net_pnl": 0.0, "expectancy_pct": 0.0, "profit_factor": 0.0, "max_drawdown": 0.0}
    pnl = np.array([float(t["net_pnl"]) for t in trades], dtype=float)
    ret = np.array([float(t["net_return_pct"]) for t in trades], dtype=float)
    wins = pnl[pnl > 0].sum(); losses = -pnl[pnl < 0].sum()
    eq = np.cumsum(pnl)
    peak = np.maximum.accumulate(np.r_[0.0, eq])
    dd = peak[1:] - eq
    sorted_pnl = np.sort(pnl)[::-1]
    stripped = float(pnl.sum() - sorted_pnl[:min(profit_remove_top_n, len(sorted_pnl))].sum())
    return {
        "n": int(len(pnl)), "net_pnl": float(pnl.sum()), "expectancy_pct": float(ret.mean()),
        "win_rate_pct": float((pnl > 0).mean() * 100), "profit_factor": float(wins / losses) if losses > 0 else 99.0,
        "max_drawdown": float(dd.max()) if len(dd) else 0.0,
        "net_pnl_remove_top_winners": stripped,
        "median_trade_return_pct": float(np.median(ret)),
        "avg_win_inr": float(pnl[pnl > 0].mean()) if np.any(pnl > 0) else 0.0,
        "avg_loss_inr": float(pnl[pnl < 0].mean()) if np.any(pnl < 0) else 0.0,
    }
