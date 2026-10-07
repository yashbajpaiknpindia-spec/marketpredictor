"""Leakage-safe empirical calibration for UltraScalp V12.

Calibration is deliberately separate from the trading score.  The legacy V12
confidence is a model score, not a probability.  This module learns:
1) P(net-positive at the evaluation horizon)
2) P(adverse excursion reaches the configured protection threshold)

The profile must be fit on a chronological training segment and then frozen
before it is applied to a blind test segment.
"""
from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional, Sequence
import numpy as np
import pandas as pd

@dataclass(frozen=True)
class V12CalibrationProfile:
    version: str
    horizon_seconds: int
    round_trip_cost_pct: float
    protection_pct: float
    score_edges: list
    positive_rates: list
    adverse_rates: list
    sample_counts: list
    train_rows: int

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

def _future_paths(s: pd.DataFrame, horizon_seconds: int, protection_pct: float):
    """Return causal signed terminal return, MFE and MAE for each signal row.

    Direction is applied to the path itself. This is important for shorts: a raw
    price rise is adverse for a short even when the raw minimum price move was
    negative somewhere later in the window.
    """
    s=s.sort_values(["ticker","captured_at"]).reset_index(drop=True).copy()
    s["captured_at"]=pd.to_datetime(s["captured_at"])
    net_label=np.full(len(s), np.nan)
    mfe=np.full(len(s), np.nan)
    mae=np.full(len(s), np.nan)
    direction=pd.to_numeric(s.get("signal_direction",0),errors="coerce").fillna(0).to_numpy(dtype=float)
    for _, g in s.groupby("ticker", sort=False):
        idxs=g.index.to_numpy()
        t=g["captured_at"].astype("int64").to_numpy()
        px=g["ltp"].astype(float).to_numpy()
        dirs=direction[idxs]
        for j, ix in enumerate(idxs):
            if j+1 >= len(g):
                continue
            end_t=t[j]+int(horizon_seconds*1e9)
            k=np.searchsorted(t,end_t,side="left")
            if k >= len(g):
                continue
            base=px[j]
            if not np.isfinite(base) or base<=0 or dirs[j] == 0:
                continue
            path=(px[j:k+1]/base-1.0)*100.0
            signed=path*dirs[j]
            net_label[ix]=signed[-1]
            mfe[ix]=np.max(signed)
            mae[ix]=np.min(signed)
    return net_label, mfe, mae

def _score_series(s: pd.DataFrame) -> np.ndarray:
    # Raw V12 confidence is intentionally treated as a score. L5 features are
    # included only as a bounded directional-strength input, never as a label.
    raw=pd.to_numeric(s.get("raw_confidence", s.get("signal_confidence", 0.0)),errors="coerce").fillna(0.0).to_numpy()
    micro=(0.45*pd.to_numeric(s.get("ofi_proxy",0.0),errors="coerce").fillna(0.0)
           +0.35*pd.to_numeric(s.get("imbalance_l5",0.0),errors="coerce").fillna(0.0)
           +0.20*pd.to_numeric(s.get("microprice_edge_pct",0.0),errors="coerce").fillna(0.0)).to_numpy()
    return raw + 8.0*np.tanh(micro)

def fit_v12_calibration(snapshots: pd.DataFrame, *, train_fraction: float=1.0,
                        horizon_seconds:int=300, round_trip_cost_pct:float=0.1363,
                        protection_pct:float=0.18, target_net_pct:float=0.4487,
                        entry_slippage_pct:float=0.015, bins:int=12) -> V12CalibrationProfile:
    s=snapshots.copy()
    if s.empty: raise ValueError("No snapshots supplied.")
    s["captured_at"]=pd.to_datetime(s["captured_at"])
    s=s.sort_values(["captured_at","ticker"]).reset_index(drop=True)
    ntrain=max(1,min(len(s),int(len(s)*train_fraction)))
    train=s.iloc[:ntrain].copy()
    # Calibration must describe the outcome of the *signal*, not the raw price
    # direction.  A short signal followed by a price rise is a negative outcome.
    # The previous implementation calibrated unsigned returns, which could make
    # short signals look positive and contributed to the 0-candidate failure.
    train = train[pd.to_numeric(train.get("signal_direction", 0), errors="coerce").fillna(0).astype(int) != 0].copy()
    future_signed, mfe_signed, mae_signed=_future_paths(train,horizon_seconds,protection_pct)
    score=_score_series(train)
    gross_terminal=np.asarray(future_signed,dtype=float)
    target_gross=float(round_trip_cost_pct)+float(entry_slippage_pct)+float(target_net_pct)
    # Calibration now answers the economically relevant question: did the signal
    # reach the full target, rather than merely finish one tick above friction?
    positive=(np.asarray(mfe_signed) >= target_gross).astype(float)
    danger=(np.asarray(mae_signed) <= -abs(float(protection_pct))).astype(float)
    gross=np.asarray(gross_terminal,dtype=float)
    valid=np.isfinite(score)&np.isfinite(gross)&np.isfinite(mfe_signed)&np.isfinite(mae_signed)
    score=score[valid]; positive=positive[valid]; danger=danger[valid]
    if len(score)<100: raise ValueError(f"Not enough labeled calibration rows: {len(score)}")
    # Fixed quantile bins from TRAIN only. Laplace smoothing avoids 0/1 probabilities.
    qs=np.linspace(0,1,bins+1)
    edges=np.unique(np.quantile(score,qs))
    if len(edges)<3:
        edges=np.array([score.min()-1e-9, np.median(score), score.max()+1e-9])
    bi=np.clip(np.searchsorted(edges,score,side="right")-1,0,len(edges)-2)
    pos=[]; adv=[]; counts=[]
    for b in range(len(edges)-1):
        m=bi==b; n=int(m.sum()); counts.append(n)
        # Jeffreys/Laplace-style smoothing, intentionally conservative.
        pos.append(float((positive[m].sum()+1.0)/(n+2.0)) if n else 0.5)
        adv.append(float((danger[m].sum()+1.0)/(n+2.0)) if n else 0.5)
    return V12CalibrationProfile(
        version="v12-calibration-v1",
        horizon_seconds=int(horizon_seconds),
        round_trip_cost_pct=float(round_trip_cost_pct),
        protection_pct=float(protection_pct),
        score_edges=[float(x) for x in edges.tolist()],
        positive_rates=pos, adverse_rates=adv, sample_counts=counts,
        train_rows=int(len(train)),
    )

def apply_v12_calibration(scores: Sequence[float], profile: Dict[str,Any]) -> tuple[np.ndarray,np.ndarray]:
    edges=np.asarray(profile["score_edges"],dtype=float)
    pos=np.asarray(profile["positive_rates"],dtype=float)
    adv=np.asarray(profile["adverse_rates"],dtype=float)
    x=np.asarray(scores,dtype=float)
    bi=np.clip(np.searchsorted(edges,x,side="right")-1,0,len(pos)-1)
    return pos[bi],adv[bi]

def calibrate_snapshot_frame(snapshots: pd.DataFrame, profile: Dict[str,Any]) -> pd.DataFrame:
    out=snapshots.copy()
    scores=_score_series(out)
    p,a=apply_v12_calibration(scores,profile)
    out["v12_calibration_score"]=scores
    out["v12_p_net_positive"]=p
    out["v12_p_adverse_stop"]=a
    return out
