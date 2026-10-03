"""Rapid Strategy Forensics for MarketPredictor.

This module is intentionally independent of Flask/DB/network code.  It performs a
single vectorised 1-minute feature pass per ticker/day, evaluates the 20 frozen
strategy definitions in both directions, and then classifies the future price path
relative to the requested protection thresholds.

The production app supplies the historical frames and market/sector context.  No
AI calls, portfolio simulation, or per-signal DB writes happen here.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
import math
import time
import hashlib

import numpy as np
import pandas as pd

from strategies import STRATEGIES, _mirror_features_for_short, evaluate_all_strategies


DEFAULT_STOP_PCT = 0.125
DEFAULT_PROFIT_ACTIVATION_PCT = 0.40
DEFAULT_TRAIL_GIVEBACK_PCT = 30.0
DEFAULT_COST_PCT = 0.15
DEFAULT_ENTRY_SLIPPAGE_BPS = 5.0
SYNTHETIC_BARS_PER_SESSION = 375
SYNTHETIC_MAX_SESSIONS = 500
SYNTHETIC_SECTORS = 12


def _stable_seed(*parts: Any) -> int:
    raw="|".join(str(x) for x in parts).encode("utf-8")
    return int.from_bytes(hashlib.sha256(raw).digest()[:8], "little", signed=False) & 0x7FFFFFFF


def synthetic_trading_dates(start_date: Any, end_date: Any, max_sessions: int = SYNTHETIC_MAX_SESSIONS) -> List[Any]:
    start=pd.Timestamp(start_date).date() if start_date else (pd.Timestamp.today().date()-pd.Timedelta(days=180))
    end=pd.Timestamp(end_date).date() if end_date else pd.Timestamp.today().date()
    days=[x.date() for x in pd.date_range(start,end,freq="B")]
    return days[-max(1,min(int(max_sessions),SYNTHETIC_MAX_SESSIONS)):]


def _synthetic_regime_params(regime_mix: str, rng: np.random.Generator) -> Tuple[float,float]:
    mode=str(regime_mix or "balanced").lower()
    if mode=="trend":
        regimes=[("up",0.42),("down",0.33),("chop",0.15),("volatile",0.10)]
    elif mode=="choppy":
        regimes=[("up",0.15),("down",0.15),("chop",0.55),("volatile",0.15)]
    elif mode=="volatile":
        regimes=[("up",0.15),("down",0.15),("chop",0.20),("volatile",0.50)]
    else:
        regimes=[("up",0.25),("down",0.20),("chop",0.40),("volatile",0.15)]
    labels=[x[0] for x in regimes]; probs=[x[1] for x in regimes]
    label=labels[int(rng.choice(len(labels),p=probs))]
    if label=="up": return 0.000020,0.00052
    if label=="down": return -0.000020,0.00052
    if label=="volatile": return float(rng.normal(0,0.00001)),0.00105
    return float(rng.normal(0,0.000008)),0.00038


def _synthetic_intraday_context(day: Any, seed: int, regime_mix: str) -> Dict[str,Any]:
    rng=np.random.default_rng(_stable_seed(seed,"market",day))
    drift,vol=_synthetic_regime_params(regime_mix,rng)
    n=SYNTHETIC_BARS_PER_SESSION
    minutes=np.arange(n,dtype=float)
    shape=np.ones(n,dtype=float)
    shape[:45]=1.35
    shape[-45:]=1.20
    shape[45:-45]=0.78
    latent=rng.normal(0,vol,n)
    latent += drift
    # Mild return autocorrelation + occasional news-like jumps.
    for i in range(1,n): latent[i]+=0.10*latent[i-1]
    jumps=(rng.random(n)<0.003) * rng.normal(0,vol*8,n)
    latent+=jumps
    market_log_path=np.cumsum(np.clip(latent*shape, -0.020, 0.020))
    market_log_path=np.clip(market_log_path, -0.22, 0.22)
    mclose=100.0*np.exp(market_log_path)
    mopen=np.r_[100.0,mclose[:-1]]
    wick=np.abs(rng.normal(0,vol*0.65,n))+0.00012
    mhigh=np.maximum(mopen,mclose)*(1+wick)
    mlow=np.minimum(mopen,mclose)*(1-wick)
    volume=np.exp(rng.normal(12.0,0.75,n))*shape
    idx=pd.date_range(f"{day} 09:15",periods=n,freq="min")
    market_df=pd.DataFrame({"Open":mopen,"High":mhigh,"Low":mlow,"Close":mclose,"Volume":volume},index=idx)
    sectors={}
    for sec in range(SYNTHETIC_SECTORS):
        srng=np.random.default_rng(_stable_seed(seed,"sector",sec,day))
        sret=srng.normal(drift*0.65,vol*0.72,n)
        for i in range(1,n): sret[i]+=0.12*sret[i-1]+0.18*latent[i]
        sector_log_path=np.cumsum(np.clip(sret, -0.015, 0.015))
        sector_log_path=np.clip(sector_log_path, -0.18, 0.18)
        sc=np.exp(sector_log_path)
        so=np.r_[1.0,sc[:-1]]
        sw=np.abs(srng.normal(0,vol*0.45,n))+0.0001
        sectors[sec]=pd.DataFrame({"Open":so,"High":np.maximum(so,sc)*(1+sw),"Low":np.minimum(so,sc)*(1-sw),"Close":sc,"Volume":np.exp(srng.normal(10.5,0.65,n))*shape},index=idx)
    return {"market":market_df,"sectors":sectors,"drift":drift,"vol":vol}


def _synthetic_daily_histories(tickers: List[str], start_date: Any, end_date: Any, seed: int, regime_mix: str) -> Dict[str,pd.DataFrame]:
    start=pd.Timestamp(start_date).date()-pd.Timedelta(days=420)
    end=pd.Timestamp(end_date).date()+pd.Timedelta(days=1)
    dates=pd.date_range(start,end,freq="B")
    out={}
    for t in tickers:
        rng=np.random.default_rng(_stable_seed(seed,"daily",t))
        level=80.0+(_stable_seed(t)%5000)/100.0
        rows=[]
        for d in dates:
            drift,vol=_synthetic_regime_params(regime_mix,rng)
            overnight=float(rng.normal(0,0.0045))
            op=max(1e-3,level*(1+overnight))
            ret=float(rng.normal(drift,vol*3.0))
            cl=max(1e-3,op*(1+ret))
            span=abs(ret)+float(abs(rng.normal(0,vol*2.0)))+0.003
            hi=max(op,cl)*(1+span*0.55); lo=min(op,cl)*(1-span*0.45)
            rows.append((d,op,hi,lo,cl,np.exp(rng.normal(13.0,0.8))))
            level=cl
        out[t]=pd.DataFrame(rows,columns=["Date","Open","High","Low","Close","Volume"]).set_index("Date")
    return out


def synthetic_ticker_day(ticker: str, day: Any, market_context: Dict[str,Any], seed: int, regime_mix: str, stock_rank: int=0) -> pd.DataFrame:
    rng=np.random.default_rng(_stable_seed(seed,"ticker",ticker,day))
    mdf=market_context["market"]
    idx=mdf.index
    n=len(idx)
    mret=mdf["Close"].pct_change().fillna(0).to_numpy(dtype=float)
    sec_id=stock_rank % SYNTHETIC_SECTORS
    sdf=market_context["sectors"][sec_id]
    sret=sdf["Close"].pct_change().fillna(0).to_numpy(dtype=float)
    beta=0.75+0.75*((_stable_seed(ticker)%1000)/1000.0)
    sec_beta=0.20+0.45*(rng.random())
    idio_vol=0.00035+0.00055*rng.random()
    ar=rng.normal(0,idio_vol,n)
    for i in range(1,n): ar[i]+=0.10*ar[i-1]
    overnight=float(rng.normal(0,0.007))
    base=80.0+(_stable_seed(ticker)%7000)/100.0
    op=base*(1+overnight)
    ret=beta*mret + sec_beta*sret + ar
    if regime_mix=="volatile": ret*=1.15
    # Keep one synthetic intraday path inside a realistic stress envelope so that
    # a low-price collapse cannot create 1,000%–10,000% MFE/MAE artefacts.
    stock_log_path=np.cumsum(np.clip(ret, -0.018, 0.018))
    stock_log_path=np.clip(stock_log_path, -0.20, 0.20)
    close=op*np.exp(stock_log_path)
    opens=np.r_[op,close[:-1]]
    micro=np.abs(rng.normal(0,0.00035,n))+np.abs(ret)*0.35
    high=np.maximum(opens,close)*(1+micro)
    low=np.minimum(opens,close)*(1-micro)
    shape=np.ones(n); shape[:45]=1.35; shape[-45:]=1.20; shape[45:-45]=0.80
    volume=np.exp(rng.normal(11.5,0.72,n))*shape
    # Occasional participation shock, useful for volume-confirmation strategies.
    spike=rng.random(n)<0.008
    volume[spike]*=(2.0+rng.random(np.sum(spike))*3.0)
    return pd.DataFrame({"Open":opens,"High":high,"Low":low,"Close":close,"Volume":volume},index=idx)


@dataclass
class SignalEvent:
    strategy_id: str
    base_strategy_id: str
    side: str
    confidence: float
    target_pct: float
    stop_pct: float
    index: int
    entry_index: int
    entry_price: float
    ticker: str
    timestamp: Any
    raw_features: Dict[str, Any]


def _finite(a: np.ndarray, fill=np.nan) -> np.ndarray:
    x = np.asarray(a, dtype=float)
    return np.where(np.isfinite(x), x, fill)


def _rolling_mean(values: np.ndarray, window: int) -> np.ndarray:
    s = pd.Series(values, dtype="float64")
    return s.rolling(window, min_periods=1).mean().to_numpy(dtype=float)


def _compute_rsi_series(close: np.ndarray, period: int = 14) -> np.ndarray:
    d = np.diff(close, prepend=np.nan)
    gains = np.where(np.isfinite(d) & (d > 0), d, 0.0)
    losses = np.where(np.isfinite(d) & (d < 0), -d, 0.0)
    gain_mean = pd.Series(gains).rolling(period, min_periods=period).mean().to_numpy(dtype=float)
    loss_mean = pd.Series(losses).rolling(period, min_periods=period).mean().to_numpy(dtype=float)
    out = np.full(len(close), np.nan, dtype=float)
    valid = np.isfinite(gain_mean) & np.isfinite(loss_mean)
    nz = valid & (loss_mean != 0)
    out[nz] = 100.0 - (100.0 / (1.0 + (gain_mean[nz] / loss_mean[nz])))
    out[valid & (loss_mean == 0)] = 100.0
    return np.round(out, 1)


def _compute_ema_recursive(close: np.ndarray, span: int) -> np.ndarray:
    out = np.full(len(close), np.nan, dtype=float)
    if len(close) == 0:
        return out
    k = 2.0 / (span + 1.0)
    ema = float(close[0])
    out[0] = ema
    for i in range(1, len(close)):
        x = close[i]
        if not np.isfinite(x):
            out[i] = ema
            continue
        ema = float(x) * k + ema * (1.0 - k)
        out[i] = ema
    return out


def _compute_ema_last40(close: np.ndarray, span: int = 9) -> np.ndarray:
    """Exact equivalent of compute_ema(closes[-40:], span) at every timestamp.

    For the first 40 bars the window grows from the session start. Thereafter the
    seed is always the oldest member of the rolling 40-bar window. The fixed-width
    portion is evaluated as a convolution for speed; the short warm-up is tiny.
    """
    n = len(close)
    out = np.full(n, np.nan, dtype=float)
    if n == 0:
        return out
    k = 2.0 / (span + 1.0)
    q = 1.0 - k
    warm = min(n, 40)
    for i in range(warm):
        ema = float(close[0])
        for j in range(1, i + 1):
            ema = float(close[j]) * k + ema * q
        out[i] = ema
    if n > 40:
        # For a 40-sample window: x0*q^39 + x1*k*q^38 + ... + x39*k.
        w = np.empty(40, dtype=float)
        w[0] = q ** 39
        for j in range(1, 40):
            w[j] = k * (q ** (39 - j))
        out[39:] = np.convolve(close[:], w[::-1], mode="valid")
    return out


def _compute_atr_pct(close: np.ndarray, high: np.ndarray, low: np.ndarray, period: int = 14) -> np.ndarray:
    prev = np.roll(close, 1)
    prev[0] = np.nan
    tr = np.maximum(high - low, np.maximum(np.abs(high - prev), np.abs(low - prev)))
    atr = pd.Series(tr).rolling(period, min_periods=period).mean().to_numpy(dtype=float)
    out = np.divide(atr, close, out=np.full(len(close), np.nan), where=close != 0) * 100.0
    return np.round(out, 2)


def _feature_arrays(df: pd.DataFrame,
                    prev_close: Optional[float],
                    prev_day_high: Optional[float], prev_day_low: Optional[float],
                    swing_high_20d: Optional[float], swing_low_20d: Optional[float],
                    swing_high_252d: Optional[float], swing_low_252d: Optional[float],
                    market_change: Optional[np.ndarray] = None,
                    sector_change: Optional[np.ndarray] = None,
                    interval_minutes: int = 1) -> Dict[str, np.ndarray]:
    d = df.copy()
    close = pd.to_numeric(d["Close"], errors="coerce").to_numpy(dtype=float)
    opn = pd.to_numeric(d["Open"], errors="coerce").to_numpy(dtype=float)
    high = pd.to_numeric(d["High"], errors="coerce").to_numpy(dtype=float)
    low = pd.to_numeric(d["Low"], errors="coerce").to_numpy(dtype=float)
    vol = pd.to_numeric(d.get("Volume", pd.Series(np.zeros(len(d)))), errors="coerce").fillna(0).to_numpy(dtype=float)
    n = len(d)

    typical = (high + low + close) / 3.0
    cum_vol = np.cumsum(vol)
    cum_tpv = np.cumsum(typical * vol)
    vwap = np.divide(cum_tpv, cum_vol, out=np.full(n, np.nan), where=cum_vol != 0)
    vwap_dist = np.divide(close - vwap, vwap, out=np.full(n, np.nan), where=vwap != 0) * 100.0

    minutes = np.arange(n, dtype=float) * float(interval_minutes)
    session_high = np.maximum.accumulate(high)
    session_low = np.minimum.accumulate(low)
    change = np.divide(close - opn[0], opn[0], out=np.zeros(n), where=opn[0] != 0) * 100.0
    gap = np.full(n, np.nan)
    if prev_close:
        gap[:] = (opn[0] - float(prev_close)) / float(prev_close) * 100.0

    or15n = max(1, int(math.ceil(15 / max(1, interval_minutes))))
    or30n = max(1, int(math.ceil(30 / max(1, interval_minutes))))
    oh15 = np.full(n, np.nan); ol15 = np.full(n, np.nan)
    oh30 = np.full(n, np.nan); ol30 = np.full(n, np.nan)
    if n:
        oh15[:]=np.nanmax(high[:min(n, or15n)])
        ol15[:]=np.nanmin(low[:min(n, or15n)])
        oh30[:]=np.nanmax(high[:min(n, or30n)])
        ol30[:]=np.nanmin(low[:min(n, or30n)])
    breakout = (minutes >= 15) & np.isfinite(oh15) & (close > oh15)
    breakout_down = (minutes >= 15) & np.isfinite(ol15) & (close < ol15)
    failed_breakout = (session_high > oh15) & (close < oh15)
    failed_breakdown = (session_low < ol15) & (close > ol15)

    recent_up = (close > np.roll(close,1)) & (np.roll(close,1) > np.roll(close,2)) & (np.roll(close,2) > np.roll(close,3))
    recent_down = (close < np.roll(close,1)) & (np.roll(close,1) < np.roll(close,2)) & (np.roll(close,2) < np.roll(close,3))
    d1 = close - np.roll(close,1)
    d3 = np.roll(close,1) - np.roll(close,2)
    accelerating = recent_up & (np.abs(d1) > np.abs(d3) * 1.1)
    decelerating_up = recent_up & (np.abs(d1) < np.abs(d3) * 0.6)
    momentum = np.full(n, "steady", dtype=object)
    momentum[recent_up] = "rising"
    momentum[recent_up & accelerating] = "accelerating"
    momentum[decelerating_up] = "decelerating"
    momentum[recent_down] = "decelerating"
    prior20 = pd.Series(vol).shift(1).rolling(20, min_periods=1).mean().to_numpy(dtype=float)
    last3 = pd.Series(vol).rolling(3, min_periods=1).mean().to_numpy(dtype=float)
    vol_mult = np.divide(last3, prior20, out=np.full(n, np.nan), where=prior20 > 0)
    exhausted = (vwap_dist > 1.5) & (last3 < prior20 * 0.7)
    momentum[exhausted] = "exhausted"

    ema9 = _compute_ema_last40(close, 9)
    ema21 = _compute_ema_recursive(close, 21)
    rsi = _compute_rsi_series(close, 14)
    atr = _compute_atr_pct(close, high, low, 14)

    pull_low = np.divide(close - session_low, session_low, out=np.full(n, np.nan), where=session_low != 0) * 100.0
    pull_high = np.divide(session_high - close, session_high, out=np.full(n, np.nan), where=session_high != 0) * 100.0
    coil_hi = pd.Series(close).shift(1).rolling(8, min_periods=8).max().to_numpy(dtype=float)
    coil_lo = pd.Series(close).shift(1).rolling(8, min_periods=8).min().to_numpy(dtype=float)
    coil = np.divide(coil_hi - coil_lo, coil_lo, out=np.full(n, np.nan), where=coil_lo != 0) * 100.0

    rs = np.full(n, np.nan)
    if market_change is not None:
        rs = change - np.asarray(market_change, dtype=float)
    sector = np.full(n, np.nan) if sector_change is None else np.asarray(sector_change, dtype=float)

    return {
        "last": close, "open": opn, "session_high": session_high, "session_low": session_low,
        "opening_high": oh15, "opening_low": ol15, "opening_high_30": oh30, "opening_low_30": ol30,
        "vwap": vwap, "vwap_distance_pct": vwap_dist, "above_vwap": close >= vwap,
        "change_pct": change, "gap_pct": gap, "volume_multiplier": vol_mult,
        "minutes_since_open": minutes, "breakout": breakout, "breakout_down": breakout_down,
        "failed_breakout": failed_breakout, "failed_breakdown": failed_breakdown,
        "recent_trend": np.where(recent_up, "rising", np.where(recent_down, "falling", "flat")),
        "momentum_state": momentum, "ema9": ema9, "ema21": ema21, "rsi": rsi, "atr_pct": atr,
        "pullback_from_low_pct": pull_low, "pullback_from_high_pct": pull_high,
        "consolidation_range_pct": coil, "relative_strength_pct": rs,
        "market_change_pct": np.full(n, np.nan) if market_change is None else np.asarray(market_change, dtype=float),
        "sector_change_pct": sector,
        "prev_day_high": np.full(n, np.nan) if prev_day_high is None else np.full(n, float(prev_day_high)),
        "prev_day_low": np.full(n, np.nan) if prev_day_low is None else np.full(n, float(prev_day_low)),
        "swing_high_20d": np.full(n, np.nan) if swing_high_20d is None else np.full(n, float(swing_high_20d)),
        "swing_low_20d": np.full(n, np.nan) if swing_low_20d is None else np.full(n, float(swing_low_20d)),
        "swing_high_252d": np.full(n, np.nan) if swing_high_252d is None else np.full(n, float(swing_high_252d)),
        "swing_low_252d": np.full(n, np.nan) if swing_low_252d is None else np.full(n, float(swing_low_252d)),
        "_close": close, "_high": high, "_low": low, "_timestamps": np.asarray(d.index),
    }


def _mirror_arrays(f: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
    keys = ("last", "open", "vwap", "ema9", "ema21", "resistance_level", "support_level")
    vals = [f[k] for k in keys if k in f]
    anchor = np.nanmax(np.vstack(vals), axis=0) if vals else np.ones(len(f["last"]))
    anchor = anchor + np.maximum(np.abs(anchor) * 1e-6, 1e-6)
    out = {k: np.array(v, copy=True) if isinstance(v, np.ndarray) else v for k,v in f.items()}
    for k in ("last", "open", "vwap", "ema9", "ema21"):
        out[k] = 2.0 * anchor - f[k]
    out["session_high"] = 2.0 * anchor - f["session_low"]
    out["session_low"] = 2.0 * anchor - f["session_high"]
    out["opening_high"] = 2.0 * anchor - f["opening_low"]
    out["opening_low"] = 2.0 * anchor - f["opening_high"]
    out["opening_high_30"] = 2.0 * anchor - f["opening_low_30"]
    out["opening_low_30"] = 2.0 * anchor - f["opening_high_30"]
    out["vwap_distance_pct"] = np.divide(out["last"] - out["vwap"], out["vwap"], out=np.full(len(anchor), np.nan), where=out["vwap"]!=0) * 100.0
    out["pullback_from_high_pct"] = np.divide(out["last"] - out["session_high"], out["session_high"], out=np.full(len(anchor), np.nan), where=out["session_high"]!=0) * 100.0
    out["pullback_from_low_pct"] = np.divide(out["last"] - out["session_low"], out["session_low"], out=np.full(len(anchor), np.nan), where=out["session_low"]!=0) * 100.0
    for k in ("change_pct", "relative_strength_pct", "gap_pct", "sector_change_pct", "market_change_pct"):
        out[k] = -f[k]
    out["above_vwap"] = ~f["above_vwap"]
    out["breakout"] = f["breakout_down"]
    out["failed_breakout"] = f["failed_breakdown"]
    out["recent_trend"] = np.where(f["recent_trend"]=="rising","falling",np.where(f["recent_trend"]=="falling","rising",f["recent_trend"]))
    out["rsi"] = 100.0 - f["rsi"]
    out["_entry_anchor"] = anchor
    return out


def _value_at(f: Dict[str, np.ndarray], i: int) -> Dict[str, Any]:
    d: Dict[str, Any] = {}
    for k,v in f.items():
        if isinstance(v, np.ndarray) and len(v) > i and not k.startswith("_"):
            x = v[i]
            if isinstance(x, (np.floating, float)):
                d[k] = None if not np.isfinite(float(x)) else float(x)
            elif isinstance(x, np.bool_):
                d[k] = bool(x)
            elif isinstance(x, np.generic):
                d[k] = x.item()
            else:
                d[k] = x
    return d


def _hard_strategy_masks(f: Dict[str, np.ndarray]) -> List[np.ndarray]:
    """Necessary-condition masks, one per frozen strategy, excluding confidence.

    These masks mirror every *blocking* condition in strategies.py. The actual
    strategy function is still called on every masked candidate, so the fast path
    cannot change the strategy definitions or confidence math.
    """
    x=f; n=len(x['last']); finite=lambda a: np.isfinite(a)
    def vol_ok(th):
        return (~finite(x['volume_multiplier'])) | (x['volume_multiplier']>=th)
    # Gap family
    gap_base=(x['gap_pct']<=-0.8)&(x['minutes_since_open']<=120)&(x['change_pct']<0.35)
    # These three derived strategies call finalize() in their parent helper and then
    # append extra blocks without re-finalizing. The current frozen engine therefore
    # has the parent's eligibility contract; the rapid scanner must mirror that exact
    # behavior rather than silently 'fixing' the strategy while researching it.
    gap_market=gap_base
    gap_volume=gap_base
    # ORB family
    orb15=(x['minutes_since_open']>=15)&x['breakout']&(~x['failed_breakout'])&(x['minutes_since_open']<=150)&vol_ok(1.15)
    orb30=orb15
    change=x['change_pct']; market=x['market_change_pct']
    market_align=finite(market) & ~(((change>0)&(market<-0.5))|((change<0)&(market>0.5)))
    orb_market=orb15
    # VWAP / EMA family
    dist=x['vwap_distance_pct']; ema9=x['ema9']; ema21=x['ema21']; rsi=x['rsi']; atr=x['atr_pct']
    vwap_rev=(finite(dist)&(dist<=-0.35)&(dist>=-3.0)&(x['minutes_since_open']>=30)&(change<=-0.2))
    vwap_trend=x['above_vwap']&( (finite(dist)&((dist>=0.05)&(dist<=1.8))) | (~finite(dist)) )&(finite(ema9)&finite(ema21)&(ema9>ema21))&(x['recent_trend']=='rising')
    ema_rsi=finite(ema9)&finite(ema21)&finite(rsi)&(ema9>ema21)&(rsi>=52)&(rsi<=68)
    pull=x['pullback_from_high_pct']
    ema_pull=finite(pull)&(pull>=0.25)&(pull<=1.8)&finite(ema21)&finite(x['last'])&(x['last']>=ema21)&x['above_vwap']&((x['recent_trend']!='falling')|(x['momentum_state']=='accelerating'))
    supertrend=(x['recent_trend']=='rising')&finite(ema9)&finite(ema21)&(ema9>ema21)&finite(atr)&(atr>=0.15)&(atr<=4.0)&(change<=4.0)
    boll_rev=finite(dist)&(dist<=-0.8)&((~finite(rsi))|(rsi<=42))&(change<=-0.5)
    coil=x['consolidation_range_pct']; last=x['last']; high=x['session_high']; vol=x['volume_multiplier']
    squeeze=finite(coil)&(coil<=1.0)&finite(last)&finite(high)&(last>=high*0.998)&finite(vol)&(vol>=1.25)
    prev= x['prev_day_high']
    prev_break=finite(last)&finite(prev)&(last>=prev*1.001)&vol_ok(1.15)
    swing20=x['swing_high_20d']
    donchian=finite(last)&finite(swing20)&(last>=swing20*0.998)&vol_ok(1.1)
    atr_expand=finite(atr)&(atr>=0.8)&(change>=0.8)&vol_ok(1.2)
    adx=finite(ema9)&finite(ema21)&(ema9>ema21)&(change>=0.6)&((~finite(atr))|(atr>=0.25))
    high52=x['swing_high_252d']
    breakout52=finite(last)&finite(high52)&(last>=high52*0.998)&finite(vol)&(vol>=1.3)
    rs=x['relative_strength_pct']; sector=x['sector_change_pct']
    rs_break=finite(rs)&(rs>=0.8)&x['breakout']&x['above_vwap']
    rs_hyst=finite(rs)&(rs>=1.0)&finite(sector)&(sector>=0.15)&(change>0)&(x['minutes_since_open']>=30)
    return [gap_base,gap_market,gap_volume,orb15,orb30,orb_market,vwap_rev,vwap_trend,ema_rsi,ema_pull,supertrend,boll_rev,squeeze,prev_break,donchian,atr_expand,adx,breakout52,rs_break,rs_hyst]


def _potential_mask(f: Dict[str, np.ndarray]) -> np.ndarray:
    masks=_hard_strategy_masks(f)
    return np.logical_or.reduce(masks) if masks else np.zeros(len(f['last']),dtype=bool)


def _evaluate_prepared(features: Dict[str, Any], mirrored: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Evaluate the frozen 20 strategies without recomputing the short-view mirror 20 times.

    The strategy functions themselves are unchanged. This helper only reuses the already
    computed LONG and SHORT feature dictionaries inside the rapid scanner.
    """
    out: List[Dict[str, Any]] = []
    for spec in STRATEGIES:
        for side, view in (('LONG', features), ('SHORT', mirrored)):
            try:
                sig = spec['fn'](view, {}).to_dict()
                sig['base_strategy_id'] = spec['id']
                sig['side'] = side
                sig['strategy_id'] = spec['id'] if side == 'LONG' else f"{spec['id']}_short"
                sig['name'] = spec['name'] if side == 'LONG' else f"{spec['name']} Short"
                sig['source'] = spec.get('source')
                out.append(sig)
            except Exception as e:
                sid = spec['id'] if side == 'LONG' else f"{spec['id']}_short"
                out.append({'strategy_id':sid,'base_strategy_id':spec['id'],'side':side,'name':spec['name'] if side == 'LONG' else f"{spec['name']} Short",'eligible':False,'confidence':0.0,'reasons':[],'blocks':[f'evaluation_error: {e}'],'target_pct':None,'stop_pct':None,'min_confidence':55.0})
    return out


def _evaluate_exact_at_index(f: Dict[str, np.ndarray], index: int) -> List[Dict[str, Any]]:
    """Exact production engine check used only on a sampled subset for parity validation."""
    feat = _value_at(f, index)
    return evaluate_all_strategies(feat, {})


def parity_check(f: Dict[str, np.ndarray], indices: Iterable[int]) -> Dict[str, Any]:
    """Compare the fast prepared evaluator and vector gate with the exact production engine."""
    gate = _potential_mask(f) | _potential_mask(_mirror_arrays(f))
    checked = misses = fast_mismatches = exact_eligible_count = fast_eligible_count = 0
    rows=[]
    for i in indices:
        if i < 3 or i >= len(gate):
            continue
        feat = _value_at(f, i)
        mir = _value_at(_mirror_arrays(f), i)
        exact = _evaluate_exact_at_index(f, i)
        fast = _evaluate_prepared(feat, mir)
        exact_ids={e['strategy_id'] for e in exact if e.get('eligible')}
        fast_ids={e['strategy_id'] for e in fast if e.get('eligible')}
        exact_eligible_count += len(exact_ids)
        fast_eligible_count += len(fast_ids)
        if exact_ids and not gate[i]:
            misses += len(exact_ids)
        if exact_ids != fast_ids:
            fast_mismatches += len(exact_ids.symmetric_difference(fast_ids))
        checked += len(exact)
        if (exact_ids and not gate[i]) or exact_ids != fast_ids:
            rows.append({'index':i,'exact_eligible':sorted(exact_ids),'fast_eligible':sorted(fast_ids),'gate':bool(gate[i])})
    return {'checked_evaluations':checked,'missed_eligible':misses,'fast_evaluator_mismatches':fast_mismatches,
            'exact_eligible_count':exact_eligible_count,'fast_eligible_count':fast_eligible_count,'samples':rows[:10],
            'status':'EXACT_GATE' if misses==0 and fast_mismatches==0 else 'SAFE_FALLBACK_REQUIRED'}


def _segment_tree_next_ge(arr: np.ndarray, start: int, threshold: float, find_max: bool=True) -> Optional[int]:
    """First index >= start whose value is >= threshold (max tree) or <= threshold (min tree)."""
    n=len(arr)
    if start>=n or not np.isfinite(threshold): return None
    size=1
    while size<n: size*=2
    tree=np.full(2*size, -np.inf if find_max else np.inf, dtype=float)
    vals=np.where(np.isfinite(arr),arr,(-np.inf if find_max else np.inf))
    tree[size:size+n]=vals
    if n: tree[size:size+n]=vals
    for i in range(size-1,0,-1):
        tree[i]=max(tree[2*i],tree[2*i+1]) if find_max else min(tree[2*i],tree[2*i+1])
    # recursive-free leftmost search on [start,n)
    l=start+size; r=n+size
    nodes=[]
    while l<r:
        if l&1: nodes.append(l); l+=1
        if r&1: r-=1; nodes.append(r)
        l//=2; r//=2
    # Need earliest by original order; scan candidate nodes from low index.
    nodes.sort()
    for node in nodes:
        ok = tree[node] >= threshold if find_max else tree[node] <= threshold
        if not ok: continue
        while node < size:
            left=node*2; right=left+1
            if (tree[left] >= threshold if find_max else tree[left] <= threshold):
                node=left
            else:
                node=right
        idx=node-size
        return idx if idx<n else None
    return None


def _first_event(high: np.ndarray, low: np.ndarray, start: int, entry: float,
                side: str, stop_pct: float, profit_pct: float,
                suffix_hi: np.ndarray, suffix_lo: np.ndarray,
                next_hi=None, next_lo=None) -> Tuple[Optional[int], Optional[str]]:
    """Find the chronological first stop/profit event.

    In production Rapid Scan the caller supplies segment-tree first-cross functions so
    repeated signals do not rescan the same future bars. The fallback loop is retained
    for compatibility/tests that call this helper directly.
    """
    n=len(high)
    if start>=n or not np.isfinite(entry) or entry<=0: return None,None
    if side=='LONG':
        pstop=entry*(1.0-stop_pct/100.0); pprof=entry*(1.0+profit_pct/100.0)
        prof_possible=np.isfinite(suffix_hi[start]) and suffix_hi[start]>=pprof
        stop_possible=np.isfinite(suffix_lo[start]) and suffix_lo[start]<=pstop
        first_prof=(next_hi(pprof,start) if next_hi and prof_possible else None)
        first_stop=(next_lo(pstop,start) if next_lo and stop_possible else None)
    else:
        pstop=entry*(1.0+stop_pct/100.0); pprof=entry*(1.0-profit_pct/100.0)
        prof_possible=np.isfinite(suffix_lo[start]) and suffix_lo[start]<=pprof
        stop_possible=np.isfinite(suffix_hi[start]) and suffix_hi[start]>=pstop
        first_prof=(next_lo(pprof,start) if next_lo and prof_possible else None)
        first_stop=(next_hi(pstop,start) if next_hi and stop_possible else None)
    if next_hi is not None and next_lo is not None:
        if first_prof is None and first_stop is None: return None,None
        if first_prof is not None and first_stop is not None:
            if first_prof==first_stop: return first_prof,'BOTH_SAME_BAR'
            return (first_prof,'PROFIT_FIRST') if first_prof<first_stop else (first_stop,'STOP_FIRST')
        if first_prof is not None: return first_prof,'PROFIT_FIRST'
        return first_stop,'STOP_FIRST'
    if not prof_possible and not stop_possible: return None,None
    for j in range(start,n):
        h=high[j]; l=low[j]
        if side=='LONG': hit_prof=np.isfinite(h) and h>=pprof; hit_stop=np.isfinite(l) and l<=pstop
        else: hit_prof=np.isfinite(l) and l<=pprof; hit_stop=np.isfinite(h) and h>=pstop
        if hit_prof and hit_stop: return j,'BOTH_SAME_BAR'
        if hit_prof: return j,'PROFIT_FIRST'
        if hit_stop: return j,'STOP_FIRST'
    return None,None


def _first_cross_factory(arr: np.ndarray, find_max: bool):
    """Build a cheap first-cross function using a segment tree closure."""
    n=len(arr); size=1
    while size<n: size*=2
    sentinel=-np.inf if find_max else np.inf
    tree=np.full(2*size,sentinel,dtype=float)
    vals=np.where(np.isfinite(arr),arr,sentinel)
    tree[size:size+n]=vals
    for i in range(size-1,0,-1):
        tree[i]=max(tree[2*i],tree[2*i+1]) if find_max else min(tree[2*i],tree[2*i+1])
    def first(threshold: float, start: int, _find_max=find_max):
        if start>=n: return None
        # Decompose suffix into O(log n) segment nodes, ordered left-to-right.
        L=start+size; R=n+size; left_nodes=[]; right_nodes=[]
        while L<R:
            if L&1: left_nodes.append(L); L+=1
            if R&1: R-=1; right_nodes.append(R)
            L//=2; R//=2
        for node in left_nodes + right_nodes[::-1]:
            ok=tree[node]>=threshold if _find_max else tree[node]<=threshold
            if not ok: continue
            while node<size:
                lc=node*2; rc=lc+1
                if (tree[lc]>=threshold if _find_max else tree[lc]<=threshold): node=lc
                else: node=rc
            idx=node-size
            return idx if idx<n else None
        return None
    return first


def _daily_levels(daily_history: Optional[pd.DataFrame], day: Any) -> Dict[str, Optional[float]]:
    if daily_history is None or daily_history.empty:
        return {k:None for k in ('prev_close','prev_day_high','prev_day_low','swing_high_20d','swing_low_20d','swing_high_252d','swing_low_252d')}
    h=daily_history.copy()
    try:
        idx=pd.DatetimeIndex(h.index)
        d=pd.Timestamp(day).date()
        mask=np.array([x.date()<d for x in idx],dtype=bool)
        prior=h.loc[mask]
    except Exception:
        prior=h[h.index < pd.Timestamp(day)]
    if prior.empty:
        return {k:None for k in ('prev_close','prev_day_high','prev_day_low','swing_high_20d','swing_low_20d','swing_high_252d','swing_low_252d')}
    return {
        'prev_close':float(prior['Close'].iloc[-1]),
        'prev_day_high':float(prior['High'].iloc[-1]),
        'prev_day_low':float(prior['Low'].iloc[-1]),
        'swing_high_20d':(float(prior['High'].tail(20).max()) if len(prior) >= 20 else None),
        'swing_low_20d':(float(prior['Low'].tail(20).min()) if len(prior) >= 20 else None),
        'swing_high_252d':(float(prior['High'].tail(252).max()) if len(prior) >= 252 else None),
        'swing_low_252d':(float(prior['Low'].tail(252).min()) if len(prior) >= 252 else None),
    }


def _net_pct(gross_pct: float, cost_pct: float) -> float:
    return float(gross_pct - cost_pct)



def _rapid_path_points(bar: Any, prior_close: Optional[float] = None) -> Tuple[List[float], str]:
    """Deterministic intrabar path matching Replay's historical resolver convention."""
    op=float(bar['Open']); hi=float(bar['High']); lo=float(bar['Low']); cl=float(bar['Close'])
    if cl > op:
        return [op, lo, hi, cl], 'O→L→H→C'
    if cl < op:
        return [op, hi, lo, cl], 'O→H→L→C'
    if prior_close is not None and cl > prior_close:
        return [op, lo, hi, cl], 'O→L→H→C_DOJI_PRIOR_UP'
    return [op, hi, lo, cl], 'O→H→L→C_DOJI_PRIOR_DOWN'


def rapid_path_aware_exit_shadow(ev: SignalEvent, day_df: pd.DataFrame,
                                 cost_pct: float, activation_pct: float,
                                 giveback_pct: float = DEFAULT_TRAIL_GIVEBACK_PCT,
                                 force_exit_before_close_minutes: int = 15) -> Dict[str, Any]:
    """Fast, dependency-free execution shadow for Rapid Scan.

    It is intentionally independent of portfolio/capital logic, but it uses the same
    historical path contract as Replay: next-open entry, strategy target/stop, +activation
    profit protection, dynamic peak-based giveback trail, net break-even floor, and the
    final intraday square-off window. This makes Rapid Scan capable of validating whether
    the exit layer is actually reacting to favorable/reversing 1-minute paths instead of
    reporting only the eventual EOD outcome.
    """
    n=len(day_df)
    if ev.entry_index >= n or ev.entry_index < 0:
        return {'ok':False,'reason':'INVALID_ENTRY'}
    entry=float(ev.entry_price)
    if not math.isfinite(entry) or entry <= 0:
        return {'ok':False,'reason':'INVALID_ENTRY_PRICE'}
    side=str(ev.side).upper()
    target_pct=max(0.0,float(ev.target_pct or 0.0))
    stop_pct=max(0.0,float(ev.stop_pct or 0.0))
    target = entry*(1.0 + target_pct/100.0) if side=='LONG' and target_pct>0 else (entry*(1.0 - target_pct/100.0) if side=='SHORT' and target_pct>0 else 0.0)
    stop = entry*(1.0 - stop_pct/100.0) if side=='LONG' and stop_pct>0 else (entry*(1.0 + stop_pct/100.0) if side=='SHORT' and stop_pct>0 else 0.0)
    # Round-trip friction is represented as a net floor after activation. The Rapid
    # evidence layer already models cost_pct on every event, so this keeps the shadow
    # consistent with its own net-performance contract.
    net_floor = entry*(1.0 + float(cost_pct)/100.0) if side=='LONG' else entry*(1.0 - float(cost_pct)/100.0)
    peak=entry; trough=entry; armed=False; trail=stop
    activation_seen=False; exit_reason=None; exit_price=None; exit_idx=None; exit_step=None; exit_point=None
    force_cutoff=None
    try:
        last_ts=day_df.index[-1]
        force_cutoff=last_ts - pd.Timedelta(minutes=int(force_exit_before_close_minutes))
    except Exception:
        force_cutoff=None
    for j in range(int(ev.entry_index), n):
        row=day_df.iloc[j]
        prior_close=float(day_df.iloc[j-1]['Close']) if j>0 else None
        try:
            points,path_model=_rapid_path_points(row,prior_close)
        except Exception:
            continue
        for step,point in enumerate(points):
            point=float(point)
            if side=='LONG':
                peak=max(peak,point); trough=min(trough,point)
                fav=((peak-entry)/entry*100.0) if entry else 0.0
                # Hard target/stop are checked before protection activation on each path point.
                if target>0 and point >= target:
                    exit_reason='TARGET_HIT'; exit_price=target if step else point
                elif (not armed) and stop>0 and point <= stop:
                    exit_reason='STOP_LOSS_HIT'; exit_price=stop if step else point
                else:
                    if (not armed) and fav >= float(activation_pct):
                        armed=True; activation_seen=True
                        trail=max(stop,net_floor,peak - ((peak-entry)*float(giveback_pct)/100.0))
                    elif armed:
                        # Once armed, trail can only rise for LONGs.
                        trail=max(trail,stop,net_floor,peak - ((peak-entry)*float(giveback_pct)/100.0))
                    if armed and trail>0 and point <= trail:
                        exit_reason='TRAILING_PROFIT_PROTECT' if trail >= net_floor else 'TRAILING_STOP_GAVE_BACK_PROFIT'
                        exit_price=trail
            else:
                peak=max(peak,point); trough=min(trough,point)
                fav=((entry-trough)/entry*100.0) if entry else 0.0
                if target>0 and point <= target:
                    exit_reason='TARGET_HIT'; exit_price=target if step else point
                elif (not armed) and stop>0 and point >= stop:
                    exit_reason='STOP_LOSS_HIT'; exit_price=stop if step else point
                else:
                    if (not armed) and fav >= float(activation_pct):
                        armed=True; activation_seen=True
                        new_trail=trough + ((entry-trough)*float(giveback_pct)/100.0)
                        trail=min([x for x in (stop, net_floor, new_trail) if x>0]) if any(x>0 for x in (stop, net_floor, new_trail)) else new_trail
                    elif armed:
                        new_trail=trough + ((entry-trough)*float(giveback_pct)/100.0)
                        trail=min(trail if trail else net_floor,new_trail)
                        if stop: trail=min(trail,stop)
                        trail=min(trail,net_floor)
                    if armed and trail>0 and point >= trail:
                        exit_reason='TRAILING_PROFIT_PROTECT' if trail <= net_floor else 'TRAILING_STOP_GAVE_BACK_PROFIT'
                        exit_price=trail
            if exit_reason:
                exit_idx=j; exit_step=step; exit_point=point; break
        if exit_reason:
            break
        if force_cutoff is not None and day_df.index[j] >= force_cutoff:
            exit_idx=j; exit_price=float(row['Close']); exit_reason='INTRADAY_FORCE_EXIT_BEFORE_CLOSE'; exit_step=3; exit_point=float(row['Close']); break
    if exit_reason is None:
        exit_idx=n-1; exit_price=float(day_df.iloc[-1]['Close']); exit_reason='END_OF_DAY_EXIT'; exit_step=3; exit_point=exit_price
    gross=((exit_price-entry)/entry*100.0) if side=='LONG' else ((entry-exit_price)/entry*100.0)
    net=gross-float(cost_pct)
    max_fav=((peak-entry)/entry*100.0) if side=='LONG' else ((entry-trough)/entry*100.0)
    max_adv=((trough-entry)/entry*100.0) if side=='LONG' else ((entry-peak)/entry*100.0)
    return {
        'ok':True,'exit_reason':exit_reason,'exit_price':round(float(exit_price),6),'exit_idx':int(exit_idx),'exit_step':int(exit_step or 0),
        'exit_point':round(float(exit_point),6),'path_model':path_model,'path_aware':True,'activation_seen':bool(activation_seen),
        'peak_price':round(float(peak),6),'trough_price':round(float(trough),6),'mfe_pct':round(float(max_fav),5),'mae_pct':round(float(max_adv),5),
        'gross_return_pct':round(float(gross),5),'net_return_pct':round(float(net),5),'protected':bool(activation_seen and exit_reason.startswith('TRAILING')),
    }


def analyze_ticker_day(ticker: str, day_df: pd.DataFrame,
                       daily_history: Optional[pd.DataFrame], day: Any,
                       market_change: Optional[np.ndarray], sector_change: Optional[np.ndarray],
                       stop_pct: float, profit_activation_pct: float,
                       cost_pct: float, entry_slippage_bps: float,
                       parity: bool = True, parity_stride: int = 173,
                       stop_check: Optional[Callable[[], bool]] = None,
                       exit_layer: bool = True) -> Dict[str, Any]:
    if stop_check and stop_check():
        return {'ticker':ticker,'day':str(day),'strategies':{},'sample_events':[],'parity':{'status':'STOPPED'},
                'safe_fallback_used':False,'eligible_signals':0,'gate_rows':0,'bars':0,'day_net_sum_pct':0.0,'day_gross_sum_pct':0.0,'aborted':True}
    lev=_daily_levels(daily_history,day)
    f=_feature_arrays(day_df, **lev, market_change=market_change, sector_change=sector_change)
    mf=_mirror_arrays(f)
    gate=_potential_mask(f) | _potential_mask(mf)
    n=len(f['last'])
    parity_info = parity_check(f, np.arange(3, n, max(1, parity_stride))) if parity and n > 3 else {'status':'NOT_RUN'}
    # Build exact strategy candidates only where that specific strategy's hard blocks
    # can possibly pass. This is the major speed win: a broad union gate is retained for
    # parity, but the production scan never calls all 20 strategies on the same bar.
    strat_events: List[SignalEvent] = []
    timestamps=f['_timestamps']; n=len(timestamps)
    masks_long=_hard_strategy_masks(f); masks_short=_hard_strategy_masks(mf)
    union=np.logical_or.reduce(masks_long+masks_short) if masks_long else np.zeros(n,dtype=bool)
    indices=np.flatnonzero(union)
    open_arr=f['open']; high_arr=f['_high']; low_arr=f['_low']; close_arr=f['_close']
    suffix_hi=np.maximum.accumulate(np.where(np.isfinite(high_arr[::-1]),high_arr[::-1],-np.inf))[::-1]
    suffix_lo=np.minimum.accumulate(np.where(np.isfinite(low_arr[::-1]),low_arr[::-1],np.inf))[::-1]
    next_hi=_first_cross_factory(high_arr,True); next_lo=_first_cross_factory(low_arr,False)
    scalar_long: Dict[int,Dict[str,Any]]={}; scalar_short: Dict[int,Dict[str,Any]]={}
    strategy_error_counts: Dict[str,int]={}
    fallback_all = bool((parity_info.get('missed_eligible') or 0) or (parity_info.get('fast_evaluator_mismatches') or 0))
    if fallback_all:
        indices = np.arange(3, n, dtype=int)
    for side, view, masks in (('LONG', f, masks_long), ('SHORT', mf, masks_short)):
        for i in indices:
            if stop_check and (int(i) % 64 == 0) and stop_check():
                return {'ticker':ticker,'day':str(day),'strategies':{},'sample_events':[],'parity':{'status':'STOPPED'},
                        'safe_fallback_used':False,'eligible_signals':0,'gate_rows':0,'bars':n,'day_net_sum_pct':0.0,'day_gross_sum_pct':0.0,'aborted':True}
            ii=int(i)
            relevant=list(range(len(STRATEGIES))) if fallback_all else [j for j,m in enumerate(masks) if m[ii]]
            if not relevant: continue
            feat_cache=scalar_long if side=='LONG' else scalar_short
            feat=feat_cache.get(ii)
            if feat is None:
                feat=_value_at(view,ii); feat_cache[ii]=feat
            for j in relevant:
                spec=STRATEGIES[j]
                try:
                    ev=spec['fn'](feat,{}).to_dict()
                except Exception as exc:
                    # Keep the scanner truthful: evaluator failures are evidence, not silent skips.
                    err_sid=str(spec.get('id') or '')
                    strategy_error_counts[err_sid]=strategy_error_counts.get(err_sid,0)+1
                    continue
                if not ev.get('eligible'): continue
                entry_i=ii+1
                if entry_i>=n: continue
                raw_open=float(open_arr[entry_i])
                entry=raw_open*(1.0+entry_slippage_bps/10000.0 if side=='LONG' else 1.0-entry_slippage_bps/10000.0)
                if not np.isfinite(entry) or entry<=0: continue
                strat_events.append(SignalEvent(
                    strategy_id=str(spec['id']) if side=='LONG' else f"{spec['id']}_short",
                    base_strategy_id=str(spec['id']), side=side,
                    confidence=float(ev.get('confidence') or 0), target_pct=float(ev.get('target_pct') or 0),
                    stop_pct=float(ev.get('stop_pct') or 0), index=ii, entry_index=entry_i,
                    entry_price=entry, ticker=ticker, timestamp=timestamps[ii], raw_features=feat))

    # Aggregation without retaining all event rows in RAM.
    by_strategy: Dict[str, Dict[str, Any]]={}
    for err_sid, err_count in strategy_error_counts.items():
        spec_err=next((x for x in STRATEGIES if str(x.get('id') or '')==err_sid), {'name':err_sid})
        by_strategy[err_sid]={'strategy_id':err_sid,'name':spec_err.get('name') or err_sid,'signals':0,'wins':0,'losses':0,'gross_sum':0.0,'net_sum':0.0,'gross_profit':0.0,'gross_loss':0.0,
                              'profit_first':0,'stop_first':0,'both_same_bar':0,'neither':0,'profit_reached':0,'stop_reached':0,'recovered_after_stop':0,'profit_then_reversed':0,'clean_profit_first':0,'profit_reached_but_lost':0,'pure_loss':0,
                              'stop_first_then_1pct':0,'stop_first_then_2pct':0,'mfe_sum':0.0,'mae_sum':0.0,'mfe_max':-np.inf,'mae_worst':0.0,'capture_sum':0.0,
                              'path_exit_count':0,'path_exit_wins':0,'path_exit_losses':0,'path_exit_net_sum':0.0,'path_exit_gross_profit':0.0,'path_exit_gross_loss':0.0,
                              'path_target_count':0,'path_stop_count':0,'path_trailing_count':0,'path_force_count':0,'path_eod_count':0,'path_activation_count':0,'path_protected_losses_avoided':0,
                              'long_signals':0,'short_signals':0,'entry_cost_sum':0.0,'evaluation_errors':err_count}
    samples: List[Dict[str,Any]]=[]
    exit_cache: Dict[Tuple[int,str,float,float], Dict[str,Any]] = {}
    for ev_idx,ev in enumerate(strat_events):
        if stop_check and (ev_idx % 128 == 0) and stop_check():
            return {'ticker':ticker,'day':str(day),'strategies':{},'sample_events':[],'parity':{'status':'STOPPED'},
                    'safe_fallback_used':False,'eligible_signals':0,'gate_rows':int(gate.sum()),'bars':n,'day_net_sum_pct':0.0,'day_gross_sum_pct':0.0,'aborted':True}
        g=by_strategy.setdefault(ev.base_strategy_id, {
            'strategy_id':ev.base_strategy_id,'name':next((x.get('name') for x in STRATEGIES if x.get('id')==ev.base_strategy_id),ev.base_strategy_id),
            'signals':0,'wins':0,'losses':0,'gross_sum':0.0,'net_sum':0.0,'gross_profit':0.0,'gross_loss':0.0,
            'profit_first':0,'stop_first':0,'both_same_bar':0,'neither':0,'profit_reached':0,'stop_reached':0,
            'recovered_after_stop':0,'profit_then_reversed':0,'clean_profit_first':0,'profit_reached_but_lost':0,'pure_loss':0,'stop_first_then_1pct':0,'stop_first_then_2pct':0,
            'mfe_sum':0.0,'mae_sum':0.0,'mfe_max':-np.inf,'mae_worst':0.0,'capture_sum':0.0,
            'path_exit_count':0,'path_exit_wins':0,'path_exit_losses':0,'path_exit_net_sum':0.0,'path_exit_gross_profit':0.0,'path_exit_gross_loss':0.0,
            'path_target_count':0,'path_stop_count':0,'path_trailing_count':0,'path_force_count':0,'path_eod_count':0,'path_activation_count':0,'path_protected_losses_avoided':0,
            'long_signals':0,'short_signals':0,'entry_cost_sum':0.0,'evaluation_errors':0,
        })
        g['signals']+=1; g['long_signals'] += ev.side=='LONG'; g['short_signals'] += ev.side=='SHORT'
        # Raw full-day path from next-open entry onward.
        entry_idx=ev.entry_index
        future_hi=float(suffix_hi[entry_idx]); future_lo=float(suffix_lo[entry_idx]); eod_close=float(close_arr[-1])
        if ev.side=='LONG':
            mfe=(future_hi-ev.entry_price)/ev.entry_price*100.0
            mae=(future_lo-ev.entry_price)/ev.entry_price*100.0
            eod=(eod_close-ev.entry_price)/ev.entry_price*100.0
        else:
            mfe=(ev.entry_price-future_lo)/ev.entry_price*100.0
            mae=(ev.entry_price-future_hi)/ev.entry_price*100.0
            eod=(ev.entry_price-eod_close)/ev.entry_price*100.0
        g['mfe_sum']+=mfe; g['mae_sum']+=mae; g['mfe_max']=max(g['mfe_max'],mfe); g['mae_worst']=min(g['mae_worst'],mae)
        if mfe>=profit_activation_pct: g['profit_reached']+=1
        if mae<=-stop_pct: g['stop_reached']+=1
        net_eod=_net_pct(eod,cost_pct); g['gross_sum']+=eod; g['net_sum']+=net_eod; g['entry_cost_sum']+=cost_pct
        if net_eod>0:
            g['wins']+=1; g['gross_profit']+=net_eod
        else:
            g['losses']+=1; g['gross_loss']+=-net_eod
        exit_result=None
        if exit_layer:
            cache_key=(int(ev.entry_index), str(ev.side), round(float(ev.target_pct or 0.0),5), round(float(ev.stop_pct or 0.0),5))
            exit_result=exit_cache.get(cache_key)
            if exit_result is None:
                try:
                    exit_result=rapid_path_aware_exit_shadow(ev, day_df, cost_pct, profit_activation_pct, giveback_pct=DEFAULT_TRAIL_GIVEBACK_PCT)
                except Exception:
                    exit_result={'ok':False,'reason':'EXIT_SHADOW_ERROR'}
                exit_cache[cache_key]=exit_result
            if exit_result.get('ok'):
                pe=float(exit_result.get('net_return_pct') or 0.0); pg=float(exit_result.get('gross_return_pct') or 0.0)
                g['path_exit_count']+=1; g['path_exit_net_sum']+=pe; g['path_exit_wins'] += int(pe>0); g['path_exit_losses'] += int(pe<=0)
                if pe>0: g['path_exit_gross_profit']+=pe
                else: g['path_exit_gross_loss']+=-pe
                reason=str(exit_result.get('exit_reason') or '')
                g['path_target_count'] += int(reason=='TARGET_HIT'); g['path_stop_count'] += int(reason=='STOP_LOSS_HIT')
                g['path_trailing_count'] += int(reason.startswith('TRAILING')); g['path_force_count'] += int(reason=='INTRADAY_FORCE_EXIT_BEFORE_CLOSE'); g['path_eod_count'] += int(reason=='END_OF_DAY_EXIT')
                g['path_activation_count'] += int(exit_result.get('activation_seen'))
                if float(exit_result.get('mfe_pct') or 0.0)>=profit_activation_pct and net_eod<=0 and pe>0:
                    g['path_protected_losses_avoided'] += 1
        j,kind=_first_event(high_arr,low_arr,ev.entry_index,ev.entry_price,ev.side,stop_pct,profit_activation_pct,suffix_hi,suffix_lo,next_hi,next_lo)
        if kind=='PROFIT_FIRST': g['profit_first']+=1
        elif kind=='STOP_FIRST': g['stop_first']+=1
        elif kind=='BOTH_SAME_BAR': g['both_same_bar']+=1
        else: g['neither']+=1
        if mfe>=profit_activation_pct:
            # Capture proxy = first 0.40 activation as the minimum executable protected
            # gain; the full trailing exit is deliberately measured separately from raw EOD.
            protected_floor=profit_activation_pct
            g['capture_sum']+=min(max(protected_floor, eod), mfe)
        else:
            g['capture_sum']+=eod
        if kind=='STOP_FIRST' and mfe>=profit_activation_pct: g['recovered_after_stop']+=1
        if kind=='PROFIT_FIRST' and eod<0: g['profit_then_reversed']+=1
        if kind=='PROFIT_FIRST' and net_eod>0: g['clean_profit_first']+=1
        if mfe>=profit_activation_pct and net_eod<=0: g['profit_reached_but_lost']+=1
        if mfe<profit_activation_pct and net_eod<=0: g['pure_loss']+=1
        if kind=='STOP_FIRST' and mfe>=1.0: g['stop_first_then_1pct']+=1
        if kind=='STOP_FIRST' and mfe>=2.0: g['stop_first_then_2pct']+=1
        if len(samples)<250 and (kind in ('STOP_FIRST','PROFIT_FIRST','BOTH_SAME_BAR') or mfe>=1.5):
            samples.append({
                'ticker':ticker,'timestamp':str(ev.timestamp),'strategy_id':ev.base_strategy_id,'side':ev.side,
                'entry_price':round(ev.entry_price,4),'raw_eod_pct':round(eod,4),'net_eod_pct':round(net_eod,4),
                'mfe_pct':round(mfe,4),'mae_pct':round(mae,4),'first_event':kind or 'NEITHER',
                'reached_profit_040':bool(mfe>=profit_activation_pct),'hit_stop_0125':bool(mae<=-stop_pct),
                'path_exit_reason':(exit_result.get('exit_reason') if isinstance(exit_result,dict) and exit_result.get('ok') else None),
                'path_exit_net_pct':(exit_result.get('net_return_pct') if isinstance(exit_result,dict) and exit_result.get('ok') else None),
                'path_exit_fill_price':(exit_result.get('exit_price') if isinstance(exit_result,dict) and exit_result.get('ok') else None),
                'path_activation_seen':bool(exit_result.get('activation_seen')) if isinstance(exit_result,dict) else False,
            })
    day_net_sum = sum(float(v.get('net_sum') or 0.0) for v in by_strategy.values())
    day_gross_sum = sum(float(v.get('gross_sum') or 0.0) for v in by_strategy.values())
    return {'ticker':ticker,'day':str(day),'strategies':by_strategy,'sample_events':samples,'parity':parity_info,
            'safe_fallback_used':fallback_all,'eligible_signals':len(strat_events),'gate_rows':int(gate.sum()),'bars':n,
            'day_net_sum_pct':day_net_sum,'day_gross_sum_pct':day_gross_sum,'exit_layer':{'enabled':bool(exit_layer),'mode':'PATH_AWARE_SHADOW_V1','simulated_signals':int(sum(int(v.get('path_exit_count') or 0) for v in by_strategy.values())),'status':'ACTIVE' if exit_layer else 'DISABLED'}}


def merge_strategy_aggregate(dst: Dict[str,Any], src: Dict[str,Any]) -> None:
    for sid,row in src.get('strategies',{}).items():
        g=dst.setdefault(sid,{k:v for k,v in row.items() if k not in ('name','strategy_id')})
        if 'name' not in g: g['name']=row.get('name')
        for k,v in row.items():
            if k in ('name','strategy_id'): continue
            if isinstance(v,(int,float,np.integer,np.floating)) and not isinstance(v,bool):
                g[k]=g.get(k,0)+float(v)
            elif isinstance(v,bool):
                g[k]=g.get(k,0)+int(v)


def finalize_strategy_rows(aggregates: Dict[str,Any]) -> List[Dict[str,Any]]:
    out=[]
    for sid,g in aggregates.items():
        n=int(g.get('signals',0)); wins=int(g.get('wins',0)); losses=int(g.get('losses',0));
        gp=float(g.get('gross_profit',0)); gl=float(g.get('gross_loss',0));
        pf=gp/gl if gl>0 else (999.0 if gp>0 else None)
        first_total=int(g.get('profit_first',0))+int(g.get('stop_first',0))+int(g.get('both_same_bar',0))+int(g.get('neither',0))
        avg_net=float(g.get('net_sum',0))/n if n else None
        profit_first_pct=float(g.get('profit_first',0))/first_total*100 if first_total else None
        stop_first_pct=float(g.get('stop_first',0))/first_total*100 if first_total else None
        profit_reached_pct=float(g.get('profit_reached',0))/n*100 if n else None
        stop_reached_pct=float(g.get('stop_reached',0))/n*100 if n else None
        stop_first=int(g.get('stop_first',0)); recovered=int(g.get('recovered_after_stop',0))
        recovered_pct=recovered/max(1,stop_first)*100
        never_recovered_pct=max(0,stop_first-recovered)/max(1,stop_first)*100
        capture_avg=float(g.get('capture_sum',0))/n if n else None
        eval_errors=int(g.get('evaluation_errors',0) or 0)
        status = ('EVALUATOR_ERROR' if eval_errors>0 and n==0 else
                  'STRONG_CANDIDATE' if n>=30 and pf is not None and pf>=1.25 and avg_net is not None and avg_net>0 else
                  'NEGATIVE_EDGE' if n>=30 and (pf is not None and pf<0.9 and (avg_net is not None and avg_net<0)) else
                  'UNPROVEN' if n<30 else 'MIXED')
        recovered_share = recovered_pct
        pure_share = float(g.get('pure_loss',0))/n*100 if n else None
        profit_lost_share = float(g.get('profit_reached_but_lost',0))/n*100 if n else None
        reversed_share = float(g.get('profit_then_reversed',0))/max(1,int(g.get('profit_first',0)))*100
        path_n=int(g.get('path_exit_count') or 0)
        path_gp=float(g.get('path_exit_gross_profit') or 0.0)
        path_gl=float(g.get('path_exit_gross_loss') or 0.0)
        path_pf=path_gp/path_gl if path_gl>0 else (999.0 if path_gp>0 else None)
        path_avg=float(g.get('path_exit_net_sum') or 0.0)/path_n if path_n else None
        path_win=float(g.get('path_exit_wins') or 0.0)/path_n*100 if path_n else None
        if eval_errors>0 and n==0:
            diagnosis = 'Evaluator error — not a zero-edge result'
            pattern_reason = f'{eval_errors:,} strategy-function evaluations failed; fix the evaluator before interpreting this row.'
        elif n < 30:
            diagnosis = 'Insufficient sample'
            pattern_reason = 'Too few signals to distinguish a real pattern from noise.'
        elif status == 'STRONG_CANDIDATE' and recovered_share >= 15:
            diagnosis = 'Natural edge + recovery sensitivity'
            pattern_reason = 'Positive raw expectancy, with a meaningful share of trades experiencing an adverse move before recovery.'
        elif pure_share >= 60:
            diagnosis = 'Mostly pure failures'
            pattern_reason = 'Most signals never reached the profit marker and finished negative.'
        elif recovered_share >= 25 and profit_lost_share >= 15:
            diagnosis = 'Entry has recoverable paths'
            pattern_reason = 'A sizable group first moved against the entry or reached profit and still ended poorly; exit timing deserves testing.'
        elif reversed_share >= 25 or profit_lost_share >= 25:
            diagnosis = 'Profit capture / reversal issue'
            pattern_reason = 'Many signals reached useful profit before reversing or finishing negative.'
        elif status == 'NEGATIVE_EDGE':
            diagnosis = 'Negative natural edge'
            pattern_reason = 'Large sample with persistently negative net expectancy.'
        else:
            diagnosis = 'Mixed path behaviour'
            pattern_reason = 'The price-path evidence is mixed; use regime and side breakdown before changing the strategy.'
        out.append({
            'strategy_id':sid,'name':g.get('name') or sid,'signals':n,'wins':wins,'losses':losses,
            'win_rate_pct':wins/n*100 if n else None,'profit_factor':pf,'avg_gross_pct':float(g.get('gross_sum',0))/n if n else None,
            'avg_net_pct':avg_net,'mfe_avg_pct':float(g.get('mfe_sum',0))/n if n else None,
            'mfe_max_pct':float(g.get('mfe_max',0)) if n else None,'mae_avg_pct':float(g.get('mae_sum',0))/n if n else None,
            'mae_worst_pct':float(g.get('mae_worst',0)) if n else None,
            'profit_reached_pct':profit_reached_pct,'stop_reached_pct':stop_reached_pct,
            'profit_first_pct':profit_first_pct,'stop_first_pct':stop_first_pct,
            'both_same_bar_pct':float(g.get('both_same_bar',0))/first_total*100 if first_total else None,
            'recovered_after_stop_pct':recovered_pct,'stop_first_never_recovered_pct':never_recovered_pct,
            'profit_then_reversed_pct':reversed_share,
            'clean_profit_first_pct':float(g.get('clean_profit_first',0))/max(1,int(g.get('profit_first',0)))*100,
            'profit_reached_but_lost_pct':profit_lost_share,
            'pure_loss_pct':pure_share,
            'stop_first_then_1pct_pct':float(g.get('stop_first_then_1pct',0))/max(1,stop_first)*100,
            'stop_first_then_2pct_pct':float(g.get('stop_first_then_2pct',0))/max(1,stop_first)*100,
            'capture_proxy_avg_pct':capture_avg,
            'exit_layer_enabled':bool(path_n>0),'path_exit_pf':path_pf,'path_exit_avg_net_pct':path_avg,'path_exit_win_rate_pct':path_win,
            'path_exit_count':path_n,'path_activation_pct':float(g.get('path_activation_count',0))/path_n*100 if path_n else None,
            'path_target_pct':float(g.get('path_target_count',0))/path_n*100 if path_n else None,
            'path_stop_pct':float(g.get('path_stop_count',0))/path_n*100 if path_n else None,
            'path_trailing_pct':float(g.get('path_trailing_count',0))/path_n*100 if path_n else None,
            'path_force_exit_pct':float(g.get('path_force_count',0))/path_n*100 if path_n else None,
            'path_eod_pct':float(g.get('path_eod_count',0))/path_n*100 if path_n else None,
            'protected_losses_avoided':int(g.get('path_protected_losses_avoided',0)),
            'long_signals':int(g.get('long_signals',0)),'short_signals':int(g.get('short_signals',0)),
            'evaluation_errors':eval_errors,'status':status,'diagnosis':diagnosis,'pattern_reason':pattern_reason,
        })
    # Keep all 20 in registry order for UI; add zero rows where necessary.
    by={r['strategy_id']:r for r in out}
    final=[]
    for s in STRATEGIES:
        final.append(by.get(s['id'], {'strategy_id':s['id'],'name':s['name'],'signals':0,'status':'UNPROVEN','exit_layer_enabled':False,'path_exit_count':0}))
    return final
