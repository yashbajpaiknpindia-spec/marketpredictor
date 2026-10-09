"""Leakage-safe UltraScalp V12 directional + economic-edge models.

The estimator is outcome-based rather than a heuristic return score:
- For each side, classify TARGET / STOP / TIMEOUT under the real V12 policy.
- Predict P(TARGET), P(STOP), P(TIMEOUT) from causal L5/price features.
- Predict the timeout return separately.
- Compute expected net edge from those probabilities and the actual economic
  target/stop/cost assumptions.

Everything is fitted only on the chronological training window.
"""
from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Any, Dict
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor

FEATURES = [
    "ret_1","ret_3","ret_5","range_pct","body_pct","close_location",
    "vol_ratio_20","range_ratio_20","rs_1","rs_3","market_ret_1","market_ret_3",
    "spread_pct","imbalance_l1","imbalance_l5","microprice_edge_pct","ofi_proxy",
    "book_pressure","depth_log","l5_imbalance_delta","ofi_delta","microprice_delta",
    "spread_delta","book_pressure_delta","minute_sin","minute_cos",
]
TARGET, STOP, TIMEOUT = 1, -1, 0

@dataclass(frozen=True)
class V12ModelProfile:
    version: str
    horizon_seconds: int
    target_gross_pct: float
    economic_lock_net_pct: float
    economic_target_gross_pct: float
    protection_pct: float
    round_trip_cost_pct: float
    entry_slippage_pct: float
    target_net_pct: float
    stop_net_pct: float
    min_expected_net_edge_pct: float
    feature_names: list
    train_rows: int
    candidate_train_rows: int
    long_outcome_model: Any
    short_outcome_model: Any
    long_timeout_model: Any
    short_timeout_model: Any
    direction_probability_threshold: float = 0.50
    direction_margin: float = 0.05

    def to_dict(self):
        return asdict(self)


def _num(x, c, default=0.0):
    return pd.to_numeric(x.get(c, pd.Series(default,index=x.index)), errors="coerce").fillna(default).astype(float)


def build_model_features(s: pd.DataFrame) -> pd.DataFrame:
    x=s.copy().reset_index(drop=True)
    x["_v12_row_id"]=np.arange(len(x),dtype=int)
    x["captured_at"]=pd.to_datetime(x["captured_at"])
    x=x.sort_values(["ticker","captured_at"]).reset_index(drop=True)
    x["_v12_date"] = x["captured_at"].dt.date
    group_cols = ["ticker"]
    if "session_id" in x.columns:
        group_cols.append("session_id")
    group_cols.append("_v12_date")
    px=_num(x,"ltp")
    for n in (1,3,5): x[f"ret_{n}"]=x.groupby(group_cols, sort=False)["ltp"].pct_change(n).fillna(0)*100
    for c,d in (("range_pct",0), ("body_pct",0), ("close_location",.5), ("market_ret_1",0), ("market_ret_3",0), ("rs_1",0), ("rs_3",0),
                ("spread_pct",0),("imbalance_l1",0),("imbalance_l5",0),("microprice_edge_pct",0),("ofi_proxy",0),("book_pressure",0),("depth_total_qty",0)):
        x[c]=_num(x,c,d)
    vol=_num(x,"volume"); vm=x.groupby(group_cols, sort=False)["volume"].transform(lambda z:pd.to_numeric(z,errors="coerce").fillna(0).rolling(20,min_periods=5).median())
    x["vol_ratio_20"]=(vol/vm.replace(0,np.nan)).replace([np.inf,-np.inf],np.nan).fillna(1)
    rm=x.groupby(group_cols, sort=False)["range_pct"].transform(lambda z:pd.to_numeric(z,errors="coerce").fillna(0).rolling(20,min_periods=5).median())
    x["range_ratio_20"]=(x.range_pct/rm.replace(0,np.nan)).replace([np.inf,-np.inf],np.nan).fillna(1)
    x["depth_log"]=np.log1p(x.depth_total_qty.clip(lower=0))
    for src,dst in (("imbalance_l5","l5_imbalance_delta"),("ofi_proxy","ofi_delta"),("microprice_edge_pct","microprice_delta"),("spread_pct","spread_delta"),("book_pressure","book_pressure_delta")):
        x[dst]=x[src]-x.groupby(group_cols, sort=False)[src].shift(1).fillna(x[src])
    m=x.captured_at.dt.hour*60+x.captured_at.dt.minute; a=2*np.pi*m/(24*60)
    x["minute_sin"]=np.sin(a); x["minute_cos"]=np.cos(a)
    return x


def _side_outcomes(s: pd.DataFrame, horizon_seconds:int, target:float, stop:float):
    """Outcome labels and timeout gross returns for LONG and SHORT."""
    x=s.sort_values(["ticker","captured_at"]).reset_index(drop=True)
    x["_v12_date"] = x["captured_at"].dt.date
    group_cols = ["ticker"]
    if "session_id" in x.columns:
        group_cols.append("session_id")
    group_cols.append("_v12_date")
    lo=np.full(len(x),np.nan); so=np.full(len(x),np.nan); lt=np.full(len(x),np.nan); st=np.full(len(x),np.nan)
    for _,g in x.groupby(group_cols,sort=False,dropna=False):
        idx=g.index.to_numpy(); t=g["captured_at"].astype("datetime64[ns]").astype("int64").to_numpy(); p=_num(g,"ltp").to_numpy()
        for j,i in enumerate(idx):
            if j+1>=len(g) or not np.isfinite(p[j]) or p[j]<=0: continue
            k=np.searchsorted(t,t[j]+int(horizon_seconds*1e9),side="right")
            if k<=j: continue
            long_o=short_o=None; long_ret=short_ret=None
            for q in range(j+1,k):
                r=(p[q]/p[j]-1)*100
                if long_o is None:
                    if r>=target: long_o=TARGET; long_ret=r
                    elif r<=-stop: long_o=STOP; long_ret=r
                if short_o is None:
                    sr=-r
                    if sr>=target: short_o=TARGET; short_ret=sr
                    elif sr<=-stop: short_o=STOP; short_ret=sr
                if long_o is not None and short_o is not None: break
            if long_o is None:
                r=(p[k-1]/p[j]-1)*100; long_o=TIMEOUT; long_ret=r
            if short_o is None:
                r=(p[k-1]/p[j]-1)*-100; short_o=TIMEOUT; short_ret=r
            lo[i]=long_o; so[i]=short_o; lt[i]=long_ret; st[i]=short_ret
    return lo,so,lt,st


def _clf():
    return HistGradientBoostingClassifier(learning_rate=.045,max_iter=180,max_leaf_nodes=9,min_samples_leaf=35,l2_regularization=1.5,early_stopping=False,random_state=42)

def _reg():
    return HistGradientBoostingRegressor(loss="absolute_error",learning_rate=.04,max_iter=160,max_leaf_nodes=7,min_samples_leaf=35,l2_regularization=2.0,early_stopping=False,random_state=42)


def _fit_side(X, y, timeout_ret):
    valid=np.isfinite(timeout_ret) | np.isfinite(y)
    X=X.loc[valid]; y=y[valid]; tr=np.isfinite(timeout_ret)&(y==TIMEOUT)
    clf=_clf(); counts=pd.Series(y).value_counts(); w=np.array([1/max(1,counts.get(v,1)) for v in y],float); w*=len(w)/w.sum(); clf.fit(X,y,sample_weight=w)
    reg=_reg()
    if tr.sum()>=50: reg.fit(X.loc[tr],timeout_ret[tr])
    else:
        reg.fit(X.iloc[:max(1,min(len(X),100))],np.zeros(max(1,min(len(X),100))))
    return clf,reg


def fit_v12_models(
    snapshots: pd.DataFrame, *,
    train_fraction: float = .60,
    horizon_seconds: int = 1800,
    target_gross_pct: float = .60,
    protection_pct: float = .18,
    round_trip_cost_pct: float = .1363,
    entry_slippage_pct: float = .015,
    economic_lock_net_pct: float = .20,
    min_expected_net_edge_pct: float = .0,
    direction_probability_threshold: float = .70,
    direction_margin: float = .02,
) -> V12ModelProfile:
    """Fit V12 on the chronological training window.

    The model predicts the *economic lock opportunity* (+0.20% net by default),
    not only the distant +0.60% terminal target. This prevents a good +0.20%
    opportunity from being labelled a failure simply because it did not reach
    the full target. Thresholds are selected only inside the training window.
    """
    x = build_model_features(snapshots)
    x = x.sort_values(["captured_at", "ticker"]).reset_index(drop=True)
    unique_times = np.sort(x["captured_at"].dropna().unique())
    if len(unique_times) < 2:
        raise ValueError("Need at least two distinct timestamps to fit V12 models.")
    if not 0 < float(train_fraction) <= 1:
        raise ValueError("train_fraction must be in (0, 1].")
    if train_fraction >= 1:
        tr = x.copy()
    else:
        cutoff_index = max(1, min(len(unique_times) - 1, int(len(unique_times) * train_fraction)))
        cutoff = unique_times[cutoff_index]
        tr = x[x["captured_at"] < cutoff].copy()
    economic_target_gross = float(round_trip_cost_pct + entry_slippage_pct + economic_lock_net_pct)
    economic_target_net = float(economic_lock_net_pct)
    economic_stop_net = float(-protection_pct - round_trip_cost_pct - entry_slippage_pct)

    # First fit a strictly earlier sub-window for threshold selection.
    # The final 30% of TRAIN is validation; the blind segment is never touched.
    train_times = np.sort(tr["captured_at"].dropna().unique())
    if len(train_times) >= 2:
        vcut = max(1, min(len(train_times) - 1, int(len(train_times) * 0.70)))
        validation_cutoff = train_times[vcut]
    else:
        vcut = len(train_times)
        validation_cutoff = None
    direction_probability_threshold = float(direction_probability_threshold)
    direction_margin = float(direction_margin)
    min_expected_net_edge_pct = float(min_expected_net_edge_pct)

    if validation_cutoff is not None and vcut < len(train_times):
        fit_part = tr[tr["captured_at"] < validation_cutoff].copy()
        val = tr[tr["captured_at"] >= validation_cutoff].copy()
        flo, fso, flg, fsg = _side_outcomes(fit_part, horizon_seconds, economic_target_gross, protection_pct)
        fvalid = np.isfinite(flo) & np.isfinite(fso) & (
            pd.to_numeric(fit_part.get("signal_direction", 0), errors="coerce").fillna(0).to_numpy() != 0
        )
        FX = fit_part.loc[fvalid, FEATURES].replace([np.inf, -np.inf], np.nan).fillna(0)
        flm, fltm = _fit_side(FX, flo[fvalid], flg[fvalid])
        fsm, fstm = _fit_side(FX, fso[fvalid], fsg[fvalid])

        Xv = val[FEATURES].replace([np.inf, -np.inf], np.nan).fillna(0)
        plt, pls, plto = _probs(flm, Xv)
        pst, pss, psto = _probs(fsm, Xv)
        lto = fltm.predict(Xv); sto = fstm.predict(Xv)
        led = plt * economic_target_net + pls * economic_stop_net + plto * lto
        sed = pst * economic_target_net + pss * economic_stop_net + psto * sto

        cand = (
            val.get("signal_direction", pd.Series(0, index=val.index)).fillna(0).astype(int).ne(0)
            & val.get("edge_pass", pd.Series(False, index=val.index)).astype(bool)
            & val.get("score_pass", pd.Series(False, index=val.index)).astype(bool)
            & val.get("l2_pass", pd.Series(False, index=val.index)).astype(bool)
        ).to_numpy()

        vlo, vso, _, _ = _side_outcomes(val, horizon_seconds, target_gross_pct, protection_pct)
        _, _, vlt, vst = _side_outcomes(val, horizon_seconds, target_gross_pct, protection_pct)
        vnet_l = np.where(vlo == TARGET, target_gross_pct - round_trip_cost_pct - entry_slippage_pct,
                 np.where(vlo == STOP, -protection_pct - round_trip_cost_pct - entry_slippage_pct,
                          vlt - round_trip_cost_pct - entry_slippage_pct))
        vnet_s = np.where(vso == TARGET, target_gross_pct - round_trip_cost_pct - entry_slippage_pct,
                 np.where(vso == STOP, -protection_pct - round_trip_cost_pct - entry_slippage_pct,
                          vst - round_trip_cost_pct - entry_slippage_pct))

        best = None
        for th in np.arange(max(.35, direction_probability_threshold-.15),
                            min(.80, direction_probability_threshold+.30)+1e-9, .05):
            for margin in (.02, .03, .05, .08):
                for edge in (0.0, .02, .05, .08, .10):
                    choose = np.where(
                        cand & (plt >= th) & (plt >= pst + margin) & (led >= edge), 1,
                        np.where(cand & (pst >= th) & (pst >= plt + margin) & (sed >= edge), -1, 0)
                    )
                    mask = choose != 0
                    if int(mask.sum()) < 40:
                        continue
                    a = np.where(choose > 0, vnet_l, np.where(choose < 0, vnet_s, np.nan))[mask]
                    pf = float(a[a > 0].sum() / abs(a[a < 0].sum())) if np.any(a < 0) else 99.0
                    mean_net = float(np.mean(a))
                    if mean_net <= 0 or pf < 1.10:
                        continue
                    score = (mean_net, pf, int(mask.sum()))
                    if best is None or score > best[0]:
                        best = (score, float(th), float(margin), float(edge))
        if best is not None:
            direction_probability_threshold, direction_margin, min_expected_net_edge_pct = best[1:]

    # Refit the final models on ALL training rows after thresholds are frozen.
    lo, so, lg, sg = _side_outcomes(tr, horizon_seconds, economic_target_gross, protection_pct)
    valid = np.isfinite(lo) & np.isfinite(so) & (
        pd.to_numeric(tr.get("signal_direction", 0), errors="coerce").fillna(0).to_numpy() != 0
    )
    X = tr.loc[valid, FEATURES].replace([np.inf, -np.inf], np.nan).fillna(0)
    lm, ltm = _fit_side(X, lo[valid], lg[valid])
    sm, stm = _fit_side(X, so[valid], sg[valid])
    return V12ModelProfile(
        "v12-direction-edge-v3-economic-lock",
        horizon_seconds, target_gross_pct, economic_lock_net_pct,
        economic_target_gross, protection_pct, round_trip_cost_pct,
        entry_slippage_pct, economic_target_net, economic_stop_net,
        float(min_expected_net_edge_pct), FEATURES, len(tr), int(valid.sum()),
        lm, sm, ltm, stm, float(direction_probability_threshold),
        float(direction_margin),
    )

def _probs(model,X):
    p=model.predict_proba(X); classes=np.asarray(model.classes_); out={int(c):p[:,i] for i,c in enumerate(classes)}
    return out.get(TARGET,np.zeros(len(X))),out.get(STOP,np.zeros(len(X))),out.get(TIMEOUT,np.zeros(len(X)))


def apply_v12_models(snapshots:pd.DataFrame,profile:V12ModelProfile)->pd.DataFrame:
    original=snapshots.copy().reset_index(drop=True)
    x=build_model_features(original)
    X=x[FEATURES].replace([np.inf,-np.inf],np.nan).fillna(0)
    plt,pls,plto=_probs(profile.long_outcome_model,X)
    pst,pss,psto=_probs(profile.short_outcome_model,X)
    tnet=profile.target_net_pct; snet=profile.stop_net_pct
    lto=profile.long_timeout_model.predict(X); sto=profile.short_timeout_model.predict(X)
    ledge=plt*tnet+pls*snet+plto*lto
    sedge=pst*tnet+pss*snet+psto*sto

    margin=float(profile.direction_margin)
    direction=np.where((ledge>=sedge+margin)&(ledge>=profile.min_expected_net_edge_pct),1,
                np.where((sedge>=ledge+margin)&(sedge>=profile.min_expected_net_edge_pct),-1,0))
    expected=np.where(direction>0,ledge,np.where(direction<0,sedge,np.maximum(ledge,sedge)))
    pdir=np.where(direction>0,plt,np.where(direction<0,pst,np.maximum(plt,pst)))

    # Restore every prediction to the exact input-row position. This is critical:
    # feature construction is ticker-sorted for causal rolling features, while
    # replay/execution is timestamp-sorted.
    rid=x["_v12_row_id"].to_numpy(dtype=int)
    def restore(a):
        out=np.empty(len(original),dtype=float)
        out[rid]=np.asarray(a,dtype=float)
        return out

    out=original.copy()
    out["v12_model_p_long_target"]=restore(plt)
    out["v12_model_p_long_stop"]=restore(pls)
    out["v12_model_p_long_timeout"]=restore(plto)
    out["v12_model_p_short_target"]=restore(pst)
    out["v12_model_p_short_stop"]=restore(pss)
    out["v12_model_p_short_timeout"]=restore(psto)
    out["v12_model_expected_long_net_edge_pct"]=restore(ledge)
    out["v12_model_expected_short_net_edge_pct"]=restore(sedge)
    out["v12_model_expected_net_edge_pct"]=restore(expected)
    out["v12_model_direction"]=restore(direction).astype(int)
    out["v12_model_p_direction_target"]=restore(pdir)
    return out
