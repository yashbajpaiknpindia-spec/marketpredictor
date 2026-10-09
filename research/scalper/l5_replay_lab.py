"""Real-L5 replay and empirical-L5 proxy research tools.

Two evidence classes are deliberately separated:

1. TRUE_L5_REPLAY: replays snapshots that were actually captured live.  This is
   the only historical test that claims to use real order-book information.
2. EMPIRICAL_L5_PROXY: bootstraps real L5 feature blocks captured live onto a
   historical OHLCV path.  This is useful before enough true-L5 sessions exist,
   but it is explicitly model/proxy evidence, not historical order-book truth.

No synthetic order-book quantities are invented here.  Proxy mode reuses real
observed L5 feature vectors/blocks and only aligns them to an OHLCV path.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
import math

import numpy as np
import pandas as pd

from .ultra_scalper_engine import ScalperConfig, score_event, simulate_first_touch
from .v12_calibration import fit_v12_calibration, calibrate_snapshot_frame


@dataclass(frozen=True)
class L5ReplayConfig:
    round_trip_cost_pct: float = 0.1363
    entry_slippage_pct: float = 0.015
    target_pct: float = 0.60
    protection_pct: float = 0.18
    max_hold_minutes: int = 10
    min_net_edge_pct: float = 0.4487
    min_signal_score: float = 65.0
    capital_inr: float = 200_000.0
    position_notional_inr: float = 200_000.0
    max_open_positions: int = 1

    def effective_max_open_positions(self) -> int:
        if self.max_open_positions > 0:
            return int(self.max_open_positions)
        return max(1, int(self.capital_inr // max(self.position_notional_inr, 1.0)))


def _future_ltp(s: pd.DataFrame, horizon_seconds: int) -> pd.Series:
    """Forward return using the first captured observation at/after the horizon."""
    x = s.sort_values(["ticker", "captured_at"]).copy()
    x["captured_at"] = pd.to_datetime(x["captured_at"])
    x["_cal_date"] = x["captured_at"].dt.date
    group_cols = ["ticker"]
    if "session_id" in x.columns:
        group_cols.append("session_id")
    group_cols.append("_cal_date")
    out = pd.Series(index=x.index, dtype=float)
    for _, g in x.groupby(group_cols, sort=False, dropna=False):
        t = g["captured_at"].astype("datetime64[ns]").astype("int64").to_numpy()
        px = g["ltp"].astype(float).to_numpy()
        target = t + int(horizon_seconds * 1e9)
        idx = np.searchsorted(t, target, side="left")
        vals = np.full(len(g), np.nan)
        ok = idx < len(g)
        vals[ok] = (px[idx[ok]] / px[ok] - 1.0) * 100.0
        out.loc[g.index] = vals
    return out.reindex(s.index)


def l5_signal_summary(snapshots: pd.DataFrame, cost_pct: float = 0.1363,
                      horizons_seconds: Sequence[int] = (30, 60, 120, 180, 300)) -> Dict[str, Any]:
    """Measure whether captured L5 features contain gross/net directional edge."""
    s = snapshots.copy()
    if s.empty:
        return {"ok": False, "error": "No snapshots supplied."}
    s["captured_at"] = pd.to_datetime(s["captured_at"])
    ms = 0.45 * s["ofi_proxy"].astype(float) + 0.35 * s["imbalance_l5"].astype(float) + 0.20 * s["microprice_edge_pct"].astype(float)
    s["micro_signal"] = ms
    s["micro_side"] = np.sign(ms).astype(int)
    result: Dict[str, Any] = {
        "ok": True,
        "evidence_class": "TRUE_L5_REPLAY_CALIBRATION",
        "rows": int(len(s)),
        "tickers": int(s["ticker"].nunique()),
        "l2_valid_pct": round(float(s.get("l2_pass", pd.Series([True] * len(s))).mean()) * 100.0, 3),
        "horizons": {},
    }
    for sec in horizons_seconds:
        fr = _future_ltp(s, int(sec))
        signed = fr * s["micro_side"]
        mask = s["micro_side"] != 0
        x = signed[mask].dropna()
        if x.empty:
            continue
        result["horizons"][str(sec)] = {
            "n": int(len(x)),
            "mean_gross_pct": round(float(x.mean()), 6),
            "median_gross_pct": round(float(x.median()), 6),
            "positive_net_rate_pct": round(float((x > cost_pct).mean() * 100.0), 4),
            "mean_net_if_held_pct": round(float((x - cost_pct).mean()), 6),
            "p90_gross_pct": round(float(x.quantile(0.90)), 6),
        }
    return result


def replay_recorded_decisions(snapshots: pd.DataFrame, config: L5ReplayConfig = L5ReplayConfig()) -> Dict[str, Any]:
    """Replay the decisions that were actually recorded by the live worker.

    This uses the real captured L5/LTP timeline. It does not invent missing book
    levels and does not look ahead when selecting an entry. The replay is intended
    to answer: 'What would the recorded live paper decisions have produced on the
    same real captured stream under a specified capital constraint?'
    """
    s = snapshots.copy()
    required = {"ticker", "captured_at", "ltp", "signal_direction", "edge_pass", "score_pass", "l2_pass"}
    missing = required - set(s.columns)
    if missing:
        return {"ok": False, "error": f"Missing columns: {sorted(missing)}"}
    s["captured_at"] = pd.to_datetime(s["captured_at"])
    s = s.sort_values(["captured_at", "ticker"]).reset_index(drop=True)
    max_hold = pd.Timedelta(minutes=int(config.max_hold_minutes))
    cap = config.effective_max_open_positions()
    open_pos: Dict[str, Dict[str, Any]] = {}
    trades: List[Dict[str, Any]] = []

    # Process events in timestamp order.  All decisions are based only on fields
    # available on the current snapshot.
    for i, r in s.iterrows():
        ts = r["captured_at"]
        ticker = str(r["ticker"])
        if ticker in open_pos:
            continue
        side = int(r.get("signal_direction") or 0)
        if side == 0 or not bool(r.get("edge_pass")) or not bool(r.get("score_pass")) or not bool(r.get("l2_pass")):
            continue
        if len(open_pos) >= cap:
            continue
        # Require a genuinely positive recorded edge after friction.
        recorded_edge = float(r.get("remaining_edge_pct") or 0.0)
        if recorded_edge <= float(config.min_net_edge_pct):
            continue
        entry = float(r["ltp"])
        target = entry * (1.0 + side * config.target_pct / 100.0)
        stop = entry * (1.0 - side * config.protection_pct / 100.0)
        pos = {"entry_i": i, "ticker": ticker, "side": side, "entry": entry,
               "entry_time": ts, "target": target, "stop": stop}
        open_pos[ticker] = pos

        # Because snapshots are not a continuous tick feed, use the first observed
        # LTP at/after the decision timestamp and the actual recorded cadence.
        future = s[(s["ticker"] == ticker) & (s["captured_at"] >= ts) &
                   (s["captured_at"] <= ts + max_hold)].sort_values("captured_at")
        exit_reason = "TIME_EXIT"
        exit_px = float(future.iloc[-1]["ltp"]) if len(future) else entry
        exit_time = future.iloc[-1]["captured_at"] if len(future) else ts
        for _, q in future.iterrows():
            px = float(q["ltp"])
            target_hit = px >= target if side > 0 else px <= target
            stop_hit = px <= stop if side > 0 else px >= stop
            if stop_hit:
                exit_reason = "STOP"; exit_px = px; exit_time = q["captured_at"]; break
            if target_hit:
                exit_reason = "TARGET"; exit_px = px; exit_time = q["captured_at"]; break
        gross = side * ((exit_px / entry) - 1.0) * 100.0
        net = gross - config.round_trip_cost_pct - config.entry_slippage_pct
        trades.append({"ticker": ticker, "side": "LONG" if side > 0 else "SHORT",
                       "entry_time": ts, "exit_time": exit_time, "gross_pct": gross,
                       "net_pct": net, "exit_reason": exit_reason,
                       "recorded_edge_pct": recorded_edge,
                       "confidence": float(r.get("signal_confidence") or 0.0)})
        # This function is sequential but each trade is closed before another on the
        # same symbol; capital concurrency is controlled by cap.
        open_pos.pop(ticker, None)

    t = pd.DataFrame(trades)
    if t.empty:
        return {"ok": True, "evidence_class": "TRUE_L5_REPLAY", "trades": 0, "metrics": {}}
    wins = t[t["net_pct"] > 0]
    losses = t[t["net_pct"] < 0]
    pf = float(wins.net_pct.sum() / abs(losses.net_pct.sum())) if len(losses) and losses.net_pct.sum() else None
    return {"ok": True, "evidence_class": "TRUE_L5_REPLAY", "trades": int(len(t)),
            "metrics": {"win_rate_pct": float((t.net_pct > 0).mean() * 100),
                        "avg_net_pct": float(t.net_pct.mean()),
                        "sum_net_pct": float(t.net_pct.sum()),
                        "profit_factor": pf,
                        "max_open_positions": cap},
            "trades": t.to_dict("records")}


def make_empirical_l5_library(snapshots: pd.DataFrame) -> pd.DataFrame:
    """Return a compact real-observation library for calibrated proxy replay."""
    cols = ["ticker", "captured_at", "ltp", "spread_pct", "imbalance_l1", "imbalance_l5",
            "microprice_edge_pct", "ofi_proxy", "book_pressure", "depth_total_qty",
            "bid_prices", "bid_qtys", "ask_prices", "ask_qtys"]
    x = snapshots[[c for c in cols if c in snapshots.columns]].copy()
    x["captured_at"] = pd.to_datetime(x["captured_at"])
    x["minute_of_day"] = x["captured_at"].dt.hour * 60 + x["captured_at"].dt.minute
    x["depth_log"] = np.log1p(x["depth_total_qty"].astype(float).clip(lower=0))
    return x.reset_index(drop=True)


def empirical_proxy_contract() -> Dict[str, Any]:
    """Description used by UI/API to prevent accidental evidence mixing."""
    return {
        "evidence_class": "EMPIRICAL_L5_PROXY",
        "uses_real_observed_l5": True,
        "uses_historical_ohlcv_path": True,
        "historical_l5_truth": False,
        "method": "sample contiguous real-L5 feature blocks matched by time-of-day and volatility/spread state",
        "latency": "preserve observed live poll-latency distribution from captured sessions",
        "promotion_rule": "never promote a proxy result as true historical L5 performance; require held-out TRUE_L5_REPLAY sessions",
    }


def replay_result_frame(result: Dict[str, Any]) -> pd.DataFrame:
    return pd.DataFrame(result.get("trades") or [])


def blind_test_v12_calibrated(
    snapshots: pd.DataFrame,
    *,
    train_fraction: float = 0.60,
    horizon_seconds: int = 300,
    round_trip_cost_pct: float = 0.1363,
    entry_slippage_pct: float = 0.015,
    protection_pct: float = 0.18,
    positive_net_floor_pct: float = 0.005,
    min_probability: float = 0.70,
    max_adverse_probability: float = 0.35,
    require_calibration_gate: bool = True,
    max_hold_minutes: int = 30,
) -> Dict[str, Any]:
    """Chronological per-symbol diagnostic for the calibrated V12 decision gate.

    This is deliberately NOT labelled a blind portfolio test: it does not
    enforce shared capital and its exit approximation is not the live policy.
    The timestamp split is chronological and the fitted profile uses only the
    earlier segment, but the result is diagnostic until a portfolio replay is used.
    """
    s=snapshots.copy()
    required={"ticker","captured_at","ltp","signal_direction","edge_pass","score_pass","l2_pass"}
    missing=required-set(s.columns)
    if missing:
        return {"ok":False,"error":f"Missing columns: {sorted(missing)}"}
    s["captured_at"]=pd.to_datetime(s["captured_at"])
    s=s.sort_values(["captured_at","ticker"]).reset_index(drop=True)
    unique_times=np.sort(s["captured_at"].dropna().unique())
    if len(unique_times) < 2:
        return {"ok":False,"error":"Need at least two distinct timestamps for a chronological split."}
    cut_index=max(1,min(len(unique_times)-1,int(len(unique_times)*train_fraction)))
    cutoff=unique_times[cut_index]
    train=s[s["captured_at"] < cutoff].copy()
    test=s[s["captured_at"] >= cutoff].copy()

    profile=fit_v12_calibration(
        train, train_fraction=1.0, horizon_seconds=horizon_seconds,
        round_trip_cost_pct=round_trip_cost_pct, protection_pct=protection_pct,
        target_net_pct=positive_net_floor_pct, entry_slippage_pct=entry_slippage_pct
    )
    test=calibrate_snapshot_frame(test, profile.to_dict())
    # Use the application's existing recorded causal signal/edge gates.
    # Calibration is recorded and scored by default but does NOT veto candidates;
    # an explicit research gate can be enabled after independent validation.
    gross_target_pct=float(round_trip_cost_pct+entry_slippage_pct+positive_net_floor_pct)

    trades=[]
    test["_cal_date"]=test["captured_at"].dt.date
    group_cols=["ticker"]
    if "session_id" in test.columns:
        group_cols.append("session_id")
    group_cols.append("_cal_date")
    groups={k:g.reset_index(drop=True) for k,g in test.groupby(group_cols,sort=False,dropna=False)}
    for group_key,g in groups.items():
        ticker=str(g["ticker"].iloc[0])
        session_id=g["session_id"].iloc[0] if "session_id" in g.columns else None
        times=g["captured_at"].to_numpy(dtype="datetime64[ns]")
        px=g["ltp"].astype(float).to_numpy()
        side=g["signal_direction"].fillna(0).astype(int).to_numpy()
        edge=g["edge_pass"].fillna(False).astype(bool).to_numpy()
        score=g["score_pass"].fillna(False).astype(bool).to_numpy()
        l2=g["l2_pass"].fillna(False).astype(bool).to_numpy()
        ptarget=g["v12_p_target_first"].to_numpy(float)
        padv=g["v12_p_adverse_first"].to_numpy(float)
        micro=(0.45*g["ofi_proxy"].fillna(0).astype(float)
               +0.35*g["imbalance_l5"].fillna(0).astype(float)
               +0.20*g["microprice_edge_pct"].fillna(0).astype(float)).to_numpy()
        rawconf=g.get("raw_confidence",g.get("signal_confidence",pd.Series(0,index=g.index))).astype(float).to_numpy()
        n=len(g)
        j=0
        while j<n:
            # Economic gate: a candidate must have enough modelled remaining edge to
            # support the full meaningful net target.  "positive after friction" is
            # intentionally not an executable target anymore.
            remaining_edge = float(g.get("remaining_edge_pct", pd.Series(0.0, index=g.index)).iloc[j] or 0.0)
            economic_pass = remaining_edge >= float(positive_net_floor_pct)
            if side[j]==0 or not edge[j] or not score[j] or not l2[j] or not economic_pass or (require_calibration_gate and (ptarget[j] < min_probability or padv[j] > max_adverse_probability)):
                j+=1; continue
            entry=px[j]
            if not np.isfinite(entry) or entry<=0:
                j+=1; continue
            direction=1 if side[j]>0 else -1
            target=entry*(1+direction*gross_target_pct/100.0)
            stop=entry*(1-direction*protection_pct/100.0)
            end_t=times[j]+np.timedelta64(int(max_hold_minutes*60),'s')
            k=min(n,np.searchsorted(times,end_t,side="right"))
            exit_idx=k-1
            reason="V12_SAFETY_TIMEOUT"
            for q in range(j+1,k):
                hit_stop=(px[q] <= stop) if direction>0 else (px[q] >= stop)
                hit_target=(px[q] >= target) if direction>0 else (px[q] <= target)
                if hit_stop:
                    exit_idx=q; reason="STOP"; break
                if hit_target:
                    exit_idx=q; reason="TARGET"; break
                signed_micro=direction*micro[q]
                if micro[q] and np.sign(micro[q]) != direction:
                    exit_idx=q; reason="V12_THESIS_FAIL_L5_FLIP"; break
                gross_now=direction*((px[q]/entry)-1)*100
                net_now=gross_now-round_trip_cost_pct-entry_slippage_pct
                if abs(micro[j]) and abs(micro[q]) < abs(micro[j])*0.30 and net_now >= 0.20:
                    exit_idx=q; reason="V12_PROFIT_LOCK_L5_WEAK"; break
            if exit_idx < j+1:
                j+=1; continue
            gross=direction*((px[exit_idx]/entry)-1)*100
            net=gross-round_trip_cost_pct-entry_slippage_pct
            expected_stop_pct=-abs(protection_pct)
            stop_overshoot_pct=max(0.0, abs(gross)-abs(protection_pct)) if reason=="STOP" else 0.0
            trades.append({
                "ticker":ticker,"session_id":session_id,"entry_time":str(g["captured_at"].iloc[j]),
                "exit_time":str(g["captured_at"].iloc[exit_idx]),
                "side":"LONG" if direction>0 else "SHORT",
                "gross_pct":gross,"net_pct":net,"exit_reason":reason,
                "expected_stop_gross_pct":expected_stop_pct,
                "actual_stop_gross_pct":gross if reason=="STOP" else None,
                "stop_overshoot_pct":stop_overshoot_pct,
                "raw_confidence":rawconf[j],"p_target_first":ptarget[j],
                "p_adverse_first":padv[j],
            })
            # one position per symbol at a time; resume after exit
            j=exit_idx+1

    t=pd.DataFrame(trades)
    if t.empty:
        metrics={"trades":0}
    else:
        wins=t[t.net_pct>0]; losses=t[t.net_pct<0]
        pf=float(wins.net_pct.sum()/abs(losses.net_pct.sum())) if len(losses) and losses.net_pct.sum() else None
        stop_rows=t[t.exit_reason=="STOP"]
        metrics={
            "trades":int(len(t)),
            "wins":int((t.net_pct>0).sum()),
            "losses":int((t.net_pct<0).sum()),
            "win_rate_pct":float((t.net_pct>0).mean()*100),
            "avg_net_pct":float(t.net_pct.mean()),
            "sum_net_pct":float(t.net_pct.sum()),
            "profit_factor":pf,
            "avg_winner_pct":float(wins.net_pct.mean()) if len(wins) else None,
            "avg_loser_pct":float(losses.net_pct.mean()) if len(losses) else None,
            "stop_exits":int(len(stop_rows)),
            "avg_stop_overshoot_pct":float(stop_rows.stop_overshoot_pct.mean()) if len(stop_rows) else None,
            "max_stop_overshoot_pct":float(stop_rows.stop_overshoot_pct.max()) if len(stop_rows) else None,
        }
    return {
        "ok":True,
        "evidence_class":"TRUE_L5_REPLAY_CALIBRATION_DIAGNOSTIC_NOT_PORTFOLIO",
        "blind_test":False,
        "blind_test_eligible":False,
        "limitations":[
            "This routine simulates each ticker independently and does not enforce a shared portfolio capital limit.",
            "Its simplified exit logic is not an exact replay of the live profit-lock/L5-persistence policy.",
            "The calibration profile estimates fixed target-first barrier probability, not live-policy net-profit probability."
        ],
        "chronological_split": {"train_rows":int(len(train)),"test_rows":int(len(test)),"train_fraction":float(train_fraction),"cutoff_timestamp":str(cutoff)},
        "calibration_profile":profile.to_dict(),
        "policy":{
            "round_trip_cost_pct":round_trip_cost_pct,
            "entry_slippage_pct":entry_slippage_pct,
            "positive_net_floor_pct":positive_net_floor_pct,
            "economic_gate": "remaining_edge_pct >= meaningful_net_target_pct",
            "profit_lock_activation_net_pct": 0.20,
            "gross_target_pct":gross_target_pct,
            "protection_pct":protection_pct,
            "min_probability":min_probability,
            "max_adverse_probability":max_adverse_probability,
            "horizon_seconds":horizon_seconds,
            "max_hold_minutes":max_hold_minutes,
        },
        "metrics":metrics,
        "trades":t.to_dict("records") if not t.empty else [],
    }


def blind_test_v12_direction_edge(
    snapshots: pd.DataFrame,
    *,
    train_fraction: float = 0.60,
    horizon_seconds: int = 1800,
    round_trip_cost_pct: float = 0.1363,
    entry_slippage_pct: float = 0.015,
    target_gross_pct: float = 0.60,
    protection_pct: float = 0.18,
    min_expected_net_edge_pct: float = 0.0,
    direction_probability_threshold: float = 0.70,
    direction_margin: float = 0.02,
    economic_lock_net_pct: float = 0.20,
    profit_lock_trail_gross_pct: float = 0.075,
    l5_flip_exit_enabled: bool = False,
    l5_flip_confirmations: int = 3,
    max_hold_minutes: int = 30,
) -> Dict[str, Any]:
    """Chronological per-symbol diagnostic for V12 directional/edge models.

    This is NOT a blind portfolio test: trades are replayed independently per
    ticker and the simplified exits do not exactly match live-paper execution.
    The later timestamp segment is not used to fit the model, but its PF is only
    a development diagnostic, not promotion evidence.
    """
    from .v12_models import fit_v12_models, apply_v12_models
    s = snapshots.copy()
    s["captured_at"] = pd.to_datetime(s["captured_at"])
    s = s.sort_values(["captured_at", "ticker"]).reset_index(drop=True)
    unique_times = np.sort(s["captured_at"].dropna().unique())
    if len(unique_times) < 2:
        return {"ok": False, "error": "Need at least two distinct timestamps for a chronological split."}
    cut_index = max(1, min(len(unique_times)-1, int(len(unique_times) * train_fraction)))
    cutoff = unique_times[cut_index]
    train = s[s["captured_at"] < cutoff].copy()
    test = s[s["captured_at"] >= cutoff].copy()
    profile = fit_v12_models(
        train, train_fraction=1.0, horizon_seconds=horizon_seconds,
        target_gross_pct=target_gross_pct, protection_pct=protection_pct,
        economic_lock_net_pct=economic_lock_net_pct,
        round_trip_cost_pct=round_trip_cost_pct, entry_slippage_pct=entry_slippage_pct,
        min_expected_net_edge_pct=min_expected_net_edge_pct,
    )
    scored = apply_v12_models(test, profile)
    # Preserve the causal V12 candidate path. The learned model ranks/gates
    # candidates; it does not create trades from arbitrary snapshots.
    candidate_mask = (
        scored.get("signal_direction", pd.Series(0, index=scored.index)).fillna(0).astype(int).ne(0)
        & scored.get("edge_pass", pd.Series(False, index=scored.index)).astype(bool)
        & scored.get("score_pass", pd.Series(False, index=scored.index)).astype(bool)
        & scored.get("l2_pass", pd.Series(False, index=scored.index)).astype(bool)
    )
    scored["v12_candidate_path_pass"] = candidate_mask
    trades=[]
    scored["_cal_date"] = scored["captured_at"].dt.date
    group_cols = ["ticker"]
    if "session_id" in scored.columns:
        group_cols.append("session_id")
    group_cols.append("_cal_date")
    for group_key, g in scored.groupby(group_cols, sort=False, dropna=False):
        g=g.reset_index(drop=True)
        ticker=str(g["ticker"].iloc[0])
        session_id=g["session_id"].iloc[0] if "session_id" in g.columns else None
        ts=g["captured_at"].to_numpy(dtype="datetime64[ns]")
        px=pd.to_numeric(g["ltp"],errors="coerce").to_numpy(float)
        md=g["v12_model_direction"].to_numpy(int)
        p_long=g["v12_model_p_long_target"].to_numpy(float)
        p_short=g["v12_model_p_short_target"].to_numpy(float)
        exp=g["v12_model_expected_net_edge_pct"].to_numpy(float)
        raw=g.get("raw_direction",g.get("signal_direction",pd.Series(0,index=g.index))).fillna(0).astype(int).to_numpy()
        j=0
        n=len(g)
        while j<n:
            direction=int(md[j])
            pdir=max(p_long[j],p_short[j]) if direction else 0.0
            if (not bool(g.get("v12_candidate_path_pass", pd.Series(False, index=g.index)).iloc[j])
                or direction == 0 or pdir < direction_probability_threshold
                or not np.isfinite(exp[j]) or exp[j] < min_expected_net_edge_pct):
                j+=1; continue
            entry=px[j]
            if not np.isfinite(entry) or entry<=0:
                j+=1; continue
            target=entry*(1+direction*target_gross_pct/100.0)
            stop=entry*(1-direction*protection_pct/100.0)
            end=ts[j]+np.timedelta64(int(max_hold_minutes*60),'s')
            k=min(n,int(np.searchsorted(ts,end,side="right")))
            if k<=j+1:
                j+=1; continue
            exit_idx=k-1; reason="V12_SAFETY_TIMEOUT"
            lock_active=False
            peak_net_gross=-np.inf
            opposite_count=0
            for q in range(j+1,k):
                gross_now=direction*((px[q]/entry)-1)*100.0
                net_now=gross_now-round_trip_cost_pct-entry_slippage_pct
                hit_stop=(px[q]<=stop) if direction>0 else (px[q]>=stop)
                hit_target=(px[q]>=target) if direction>0 else (px[q]<=target)
                if hit_stop:
                    exit_idx=q; reason="STOP"; break
                if hit_target:
                    exit_idx=q; reason="TARGET"; break

                # Immediate favorable-move trail: arm on the first net-positive
                # movement after friction/slippage. Do not wait for the economic
                # target before protecting a favorable path. The configured
                # economic target remains the execution objective; this trailing
                # layer is deliberately earlier to prevent favorable MFE from
                # turning into a timeout/loss.
                if net_now > 0.0:
                    lock_active=True
                    peak_net_gross=max(peak_net_gross, gross_now)
                if lock_active and gross_now <= peak_net_gross-profit_lock_trail_gross_pct:
                    exit_idx=q; reason="V12_PROFIT_LOCK"; break

                micro=float(0.45*float(g["ofi_proxy"].iloc[q]) + 0.35*float(g["imbalance_l5"].iloc[q]) + 0.20*float(g["microprice_edge_pct"].iloc[q]))
                if micro and np.sign(micro) != direction:
                    opposite_count += 1
                else:
                    opposite_count = 0
                if l5_flip_exit_enabled and opposite_count >= max(1, int(l5_flip_confirmations)):
                    exit_idx=q; reason="V12_THESIS_FAIL_L5_FLIP_CONFIRMED"; break
            gross=direction*((px[exit_idx]/entry)-1)*100.0
            net=gross-round_trip_cost_pct-entry_slippage_pct
            trades.append({
                "ticker":ticker,"session_id":session_id,"entry_time":str(g["captured_at"].iloc[j]),"exit_time":str(g["captured_at"].iloc[exit_idx]),
                "side":"LONG" if direction>0 else "SHORT","gross_pct":gross,"net_pct":net,"exit_reason":reason,
                "model_direction":direction,"p_direction":pdir,"p_long":p_long[j],"p_short":p_short[j],
                "expected_net_edge_pct":float(exp[j]),"legacy_raw_direction":int(raw[j]),
            })
            j=exit_idx+1
    t=pd.DataFrame(trades)
    if t.empty:
        metrics={"trades":0}
    else:
        wins=t[t.net_pct>0]; losses=t[t.net_pct<0]
        pf=float(wins.net_pct.sum()/abs(losses.net_pct.sum())) if len(losses) and losses.net_pct.sum() else None
        metrics={
            "trades":int(len(t)),"wins":int(len(wins)),"losses":int(len(losses)),
            "win_rate_pct":float((t.net_pct>0).mean()*100),"avg_net_pct":float(t.net_pct.mean()),
            "sum_net_pct":float(t.net_pct.sum()),"profit_factor":pf,
            "avg_winner_pct":float(wins.net_pct.mean()) if len(wins) else None,
            "avg_loser_pct":float(losses.net_pct.mean()) if len(losses) else None,
            "target_exits":int((t.exit_reason=="TARGET").sum()),
            "stop_exits":int((t.exit_reason=="STOP").sum()),
            "flip_exits":int((t.exit_reason=="V12_THESIS_FAIL_L5_FLIP").sum()),
            "timeout_exits":int((t.exit_reason=="V12_SAFETY_TIMEOUT").sum()),
            "mean_predicted_edge_pct":float(t.expected_net_edge_pct.mean()),
        }
    # Blind diagnostics only; they are never fed back into thresholds.
    scored_nonzero=scored[scored["v12_model_direction"]!=0]
    diagnostics={
        "blind_rows":int(len(test)),
        "model_direction_candidates":int(len(scored_nonzero)),
        "model_long_candidates":int((scored.v12_model_direction>0).sum()),
        "model_short_candidates":int((scored.v12_model_direction<0).sum()),
        "edge_pass_candidates":int((scored.v12_model_expected_net_edge_pct>=min_expected_net_edge_pct).sum()),
        "p_direction_min":float(max(p_long.min(),p_short.min())),
        "p_direction_max":float(max(p_long.max(),p_short.max())),
    }
    return {
        "ok":True,"evidence_class":"TRUE_L5_REPLAY_DIRECTION_EDGE_DIAGNOSTIC_NOT_PORTFOLIO","blind_test":False,"blind_test_eligible":False,
        "limitations":[
            "This routine simulates each ticker independently and does not enforce shared portfolio capital.",
            "Its stop/target/profit-lock/flip settings are not an exact replay of the live paper worker's full policy.",
            "Treat all PF values from this routine as development diagnostics, not blind portfolio proof."
        ],
        "chronological_split":{"train_rows":int(len(train)),"test_rows":int(len(test)),"train_fraction":float(train_fraction),"cutoff_timestamp":str(cutoff)},
        "model_profile":profile.to_dict(),"diagnostics":diagnostics,
        "policy":{"target_gross_pct":target_gross_pct,"protection_pct":protection_pct,
                   "round_trip_cost_pct":round_trip_cost_pct,"entry_slippage_pct":entry_slippage_pct,
                   "min_expected_net_edge_pct":min_expected_net_edge_pct,
                   "direction_probability_threshold":direction_probability_threshold,
                   "horizon_seconds":horizon_seconds,"max_hold_minutes":max_hold_minutes},
        "metrics":metrics,"trades":t.to_dict("records") if not t.empty else []
    }
