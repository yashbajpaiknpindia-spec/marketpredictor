from __future__ import annotations

"""V3.2.1 research engine.

This module is research-only: importing it never runs a simulation.  Execution is
under an explicit __main__ guard.  The important fixes are:
- no artificial 11:20 start-time blackout;
- percentage units are consistently fractional internally;
- broad candidate generation (discovery, not approval);
- LONG/SHORT parity;
- barrier/MFE/MAE/time labels aligned with the actual trading objective;
- family evidence is diagnostic, never a tiny-sample hard veto;
- portfolio ranking is by predicted net edge at each timestamp and concurrency
  follows the simulated exit bar rather than a fixed 30-minute approximation;
- costs are calculated once and slippage is a separate stress assumption.
"""

import json
import os
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
from sklearn.metrics import roc_auc_score

from v3_2_engine import ENGINE_VERSION, barrier_outcome, economic_decision

DATA = Path(os.environ.get("V32_RESEARCH_DATA", "/mnt/data/combined_5m.csv"))
OUT = Path(os.environ.get("V32_RESEARCH_OUT", "/mnt/data/v32_fixed/research/v3_2"))
CAPITAL = 200_000.0
NOTIONAL = 65_000.0
MAX_CONCURRENT = 3
MAX_TRADES_DAY = 4
H = 6
TARGET_PCT = 0.50
STOP_PCT = 0.30
SLIPPAGE_BPS_ROUNDTRIP = 2.0

BROKERAGE_RATE = 0.0003
BROKERAGE_CAP = 20.0
STT_SELL = 0.00025
EXCHANGE = 0.0000307
SEBI = 0.000001
GST = 0.18
STAMP_BUY = 0.00003


def mandatory_cost(notional: float = NOTIONAL) -> float:
    brokerage = min(BROKERAGE_CAP, notional * BROKERAGE_RATE) * 2.0
    stt = notional * STT_SELL
    exch = notional * EXCHANGE * 2.0
    sebi = notional * SEBI * 2.0
    gst = GST * (brokerage + exch + sebi)
    stamp = notional * STAMP_BUY
    return brokerage + stt + exch + sebi + gst + stamp


COST = mandatory_cost()
COST_PCT = COST / NOTIONAL * 100.0


def _safe_div(a, b):
    return a / b.replace(0, np.nan)


def add_features(g: pd.DataFrame) -> pd.DataFrame:
    g = g.copy().sort_values("timestamp")
    c = g["close"].astype(float)
    o = g["open"].astype(float)
    h = g["high"].astype(float)
    l = g["low"].astype(float)
    v = g["volume"].astype(float)
    g["ret1"] = c.pct_change()
    g["ret3"] = c.pct_change(3)
    g["ret6"] = c.pct_change(6)
    g["ret12"] = c.pct_change(12)
    g["range_pct"] = (h - l) / c.replace(0, np.nan) * 100.0
    g["body_pct"] = (c - o) / o.replace(0, np.nan) * 100.0
    prevc = c.shift(1)
    tr = pd.concat([(h - l), (h - prevc).abs(), (l - prevc).abs()], axis=1).max(axis=1)
    g["atr_pct"] = tr.rolling(14, min_periods=8).mean() / c.replace(0, np.nan) * 100.0
    g["ema9"] = c.ewm(span=9, adjust=False).mean()
    g["ema20"] = c.ewm(span=20, adjust=False).mean()
    g["ema50"] = c.ewm(span=50, adjust=False).mean()
    g["trend"] = (g.ema20 / g.ema50 - 1.0) * 100.0
    typical = (h + l + c) / 3.0
    vol_cum = v.groupby(g["day"]).cumsum().replace(0, np.nan)
    g["vwap"] = (typical * v).groupby(g["day"]).cumsum() / vol_cum
    g["vwap_dist"] = (c / g["vwap"] - 1.0) * 100.0
    g["vol_rel"] = v / v.rolling(20, min_periods=10).median().replace(0, np.nan)
    g["vol_z"] = (v - v.rolling(20, min_periods=10).mean()) / v.rolling(20, min_periods=10).std().replace(0, np.nan)
    g["hh12"] = h.shift(1).rolling(12, min_periods=12).max()
    g["ll12"] = l.shift(1).rolling(12, min_periods=12).min()
    g["hh6"] = h.shift(1).rolling(6, min_periods=6).max()
    g["ll6"] = l.shift(1).rolling(6, min_periods=6).min()
    g["compression"] = g["range_pct"].rolling(6, min_periods=6).mean() / g["range_pct"].rolling(24, min_periods=12).mean()
    g["dist_hh12"] = (c / g["hh12"] - 1.0) * 100.0
    g["dist_ll12"] = (c / g["ll12"] - 1.0) * 100.0
    g["bar"] = g.groupby("day").cumcount()
    g["open_day"] = g.groupby("day")["open"].transform("first")
    g["gap_from_open"] = (c / g["open_day"] - 1.0) * 100.0
    g["orh"] = g.groupby("day")["high"].transform(lambda x: x.iloc[:6].max() if len(x) >= 6 else np.nan)
    g["orl"] = g.groupby("day")["low"].transform(lambda x: x.iloc[:6].min() if len(x) >= 6 else np.nan)
    g["session_high"] = g.groupby("day")["high"].cummax()
    g["session_low"] = g.groupby("day")["low"].cummin()
    return g


def add_forward_labels(g: pd.DataFrame) -> pd.DataFrame:
    g = g.copy()
    for k in range(1, H + 1):
        g[f"fhigh{k}"] = g["high"].shift(-k)
        g[f"flow{k}"] = g["low"].shift(-k)
    g["entry"] = g["open"].shift(-1)
    return g


def candidate_masks(df: pd.DataFrame) -> Dict[int, Tuple[pd.Series, pd.Series]]:
    # Discovery rules are deliberately permissive. They should surface setups;
    # model/economics/portfolio layers decide whether they are tradable.
    ret1p = df["ret1"]
    ret3p = df["ret3"]
    ret6p = df["ret6"]
    f = {}
    f[1] = (
        (df.close > df.hh12) & (df.vol_rel >= 1.15) & (df.vwap_dist > 0.0),
        (df.close < df.ll12) & (df.vol_rel >= 1.15) & (df.vwap_dist < 0.0),
    )
    f[2] = (
        (df.ema20 > df.ema50) & (df.close > df.ema20) & (df.close.shift(1) <= df.ema20.shift(1)) & (df.vol_rel >= 1.05),
        (df.ema20 < df.ema50) & (df.close < df.ema20) & (df.close.shift(1) >= df.ema20.shift(1)) & (df.vol_rel >= 1.05),
    )
    # FIX: ret1/ret3 are fractional returns, therefore thresholds are 0.12%/0.30%,
    # not 12%/30%.
    f[3] = (
        (df.compression < 0.90) & (df.vol_rel >= 1.25) & (ret1p > 0.0012) & (df.close > df.hh6),
        (df.compression < 0.90) & (df.vol_rel >= 1.25) & (ret1p < -0.0012) & (df.close < df.ll6),
    )
    f[4] = (
        (df.close.shift(1) < df.vwap.shift(1)) & (df.close > df.vwap) & (df.vol_rel >= 1.10) & (ret1p > 0.0010),
        (df.close.shift(1) > df.vwap.shift(1)) & (df.close < df.vwap) & (df.vol_rel >= 1.10) & (ret1p < -0.0010),
    )
    f[5] = (
        (df.vwap_dist < -0.60) & (ret1p > 0) & (df.low < df.ll6) & (df.body_pct > 0.10) & (df.vol_rel >= 1.05),
        (df.vwap_dist > 0.60) & (ret1p < 0) & (df.high > df.hh6) & (df.body_pct < -0.10) & (df.vol_rel >= 1.05),
    )
    f[6] = (
        (ret3p > 0.003) & (df.vol_rel > 1.15) & (df.body_pct > 0),
        (ret3p < -0.003) & (df.vol_rel > 1.15) & (df.body_pct < 0),
    )
    # Opening range expansion/failure; warmup is family-specific, not universal.
    f[7] = (
        (df.bar >= 6) & (df.bar <= 30) & (df.close > df.orh) & (df.vol_rel > 1.05),
        (df.bar >= 6) & (df.bar <= 30) & (df.close < df.orl) & (df.vol_rel > 1.05),
    )
    # Early ignition: designed to capture a move before all lagging confirmations line up.
    f[8] = (
        (df.bar >= 4) & (df.bar <= 24) & (ret1p > 0.0010) & (ret3p > 0.0015) & (df.vol_rel > 1.0),
        (df.bar >= 4) & (df.bar <= 24) & (ret1p < -0.0010) & (ret3p < -0.0015) & (df.vol_rel > 1.0),
    )
    # Failed breakout/trap: recoveries from a prior extreme. These are intentionally
    # independent of the trend family rather than requiring the same confirmation stack.
    f[9] = (
        (df.high.shift(1) >= df.hh12.shift(1)) & (df.close < df.hh12) & (ret1p < 0) & (df.vol_rel > 1.0),
        (df.low.shift(1) <= df.ll12.shift(1)) & (df.close > df.ll12) & (ret1p > 0) & (df.vol_rel > 1.0),
    )
    f[10] = (
        (ret6p > 0.005) & (df.close > df.ema20) & (df.close >= df.vwap) & (df.ret3 < 0.0),
        (ret6p < -0.005) & (df.close < df.ema20) & (df.close <= df.vwap) & (df.ret3 > 0.0),
    )
    return f


def build_candidate_ledger(df: pd.DataFrame) -> pd.DataFrame:
    base_cols = ["day", "timestamp", "symbol", "bar", "entry", "atr_pct", "trend", "vwap_dist", "vol_rel",
                 "ret1", "ret3", "ret6", "compression", "range_pct", "body_pct", "gap_from_open", "orh", "orl"]
    rows: List[pd.DataFrame] = []
    masks = candidate_masks(df)
    for family, (lm, sm) in masks.items():
        for direction, mask in (("LONG", lm), ("SHORT", sm)):
            q = df.loc[mask.fillna(False), base_cols].copy()
            q = q[(q["entry"] > 0) & (q["bar"] >= 4) & (q["bar"] <= 68)]
            q["direction"] = direction
            q["family"] = int(family)
            rows.append(q)
    if not rows:
        return pd.DataFrame()
    cand = pd.concat(rows, ignore_index=True)
    cand = cand.replace([np.inf, -np.inf], np.nan)
    numcols = ["atr_pct", "trend", "vwap_dist", "vol_rel", "ret1", "ret3", "ret6", "compression", "range_pct", "body_pct", "bar", "gap_from_open"]
    return cand.dropna(subset=numcols + ["entry"]).reset_index(drop=True)


def attach_barrier_labels(cand: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
    # Build a keyed lookup of each stock-day path once, then evaluate the exact same
    # side-aware barrier semantics used by the application's shared engine.
    path_map: Dict[Tuple[str, object], pd.DataFrame] = {
        (str(sym), day): g.sort_values("timestamp").reset_index(drop=True)
        for (sym, day), g in df.groupby(["symbol", "day"], sort=False)
    }
    out = []
    for r in cand.itertuples(index=False):
        g = path_map.get((str(r.symbol), r.day))
        if g is None:
            continue
        idxs = np.flatnonzero(g["timestamp"].to_numpy() == r.timestamp)
        if len(idxs) == 0:
            continue
        i = int(idxs[0])
        path = g.iloc[i + 1:i + H + 1]
        if len(path) < H:
            continue
        entry = float(r.entry)
        bo = barrier_outcome(entry, path["high"].to_numpy(), path["low"].to_numpy(), side=r.direction,
                             target_pct=TARGET_PCT, stop_pct=STOP_PCT)
        d = r._asdict()
        d.update({
            "mfe": bo.mfe_pct,
            "mae": bo.mae_pct,
            "target_before_stop": int(bo.target_before_stop),
            "stop_before_target": int(bo.stop_before_target),
            "target_bars": bo.target_bars,
            "stop_bars": bo.stop_bars,
            "barrier_outcome": bo.outcome,
            "horizon_close_return_pct": (float(path.iloc[-1].close) / entry - 1.0) * 100.0 if r.direction == "LONG" else (1.0 - float(path.iloc[-1].close) / entry) * 100.0,
        })
        out.append(d)
    return pd.DataFrame(out)


def make_x(x: pd.DataFrame, families: Iterable[int]) -> pd.DataFrame:
    numcols = ["atr_pct", "trend", "vwap_dist", "vol_rel", "ret1", "ret3", "ret6", "compression", "range_pct", "body_pct", "bar", "gap_from_open"]
    z = x[numcols].copy()
    z["direction_long"] = (x.direction == "LONG").astype(int)
    for i in families:
        z[f"family_{i}"] = (x.family == i).astype(int)
    return z.astype(float)


def fold_dates(days: np.ndarray) -> List[Tuple[np.ndarray, np.ndarray, np.ndarray]]:
    starts = [30, 35, 40, 45, 50, 55]
    folds = []
    for s in starts:
        test_dates = days[s:s + 5]
        val_dates = days[s - 5:s]
        train_dates = days[:s - 6]
        if len(test_dates) == 5 and len(val_dates) == 5 and len(train_dates) >= 20:
            folds.append((train_dates, val_dates, test_dates))
    return folds


def run_walk_forward(cand: pd.DataFrame):
    families = list(range(1, 11))
    days = np.array(sorted(cand.day.unique()))
    folds = fold_dates(days)
    all_test: List[pd.DataFrame] = []
    fold_reports = []
    family_diagnostics = []
    for fi, (trd, vad, ted) in enumerate(folds, 1):
        tr = cand[cand.day.isin(trd)].copy()
        va = cand[cand.day.isin(vad)].copy()
        te = cand[cand.day.isin(ted)].copy()
        Xtr = make_x(tr, families)
        Xv = make_x(va, families)
        Xt = make_x(te, families)
        y = tr.target_before_stop.astype(int)
        clf = HistGradientBoostingClassifier(max_iter=180, max_leaf_nodes=15, learning_rate=0.05,
                                             l2_regularization=2.0, random_state=200 + fi)
        clf.fit(Xtr, y)
        p_val = clf.predict_proba(Xv)[:, 1]
        p_test = clf.predict_proba(Xt)[:, 1]
        # Supplementary MFE model: useful for expected opportunity size but never a
        # replacement for the barrier outcome.
        reg = HistGradientBoostingRegressor(max_iter=160, max_leaf_nodes=15, learning_rate=0.05,
                                            l2_regularization=2.0, random_state=300 + fi)
        reg.fit(Xtr, tr.mfe.clip(upper=5.0))
        mfe_test = np.maximum(reg.predict(Xt), 0.0)
        gross_if_target = p_val * TARGET_PCT - (1.0 - p_val) * STOP_PCT
        # Threshold selected on validation only, based on expected gross net of known
        # mandatory cost and a 2bp round-trip execution stress.
        best = (0.60, -np.inf)
        for thr in np.arange(0.50, 0.96, 0.05):
            m = p_val >= thr
            if int(m.sum()) < 20:
                continue
            expected_gross = float(gross_if_target[m].mean())
            decision = economic_decision(expected_gross, COST_PCT, slippage_pct=SLIPPAGE_BPS_ROUNDTRIP / 100.0,
                                         minimum_net_edge_pct=0.03, edge_to_cost_multiple=1.5)
            if decision["expected_net_under_stress_pct"] > best[1]:
                best = (float(thr), float(decision["expected_net_under_stress_pct"]))
        threshold = best[0]
        expected_gross_test = p_test * TARGET_PCT - (1.0 - p_test) * STOP_PCT
        expected_net_test = expected_gross_test - COST_PCT - SLIPPAGE_BPS_ROUNDTRIP / 100.0
        accepted = (p_test >= threshold) & (expected_net_test > 0.03) & (mfe_test >= 0.50 * 0.75)
        test = te.copy()
        test["pred_target_prob"] = p_test
        test["pred_expected_gross_pct"] = expected_gross_test
        test["pred_expected_net_stress_pct"] = expected_net_test
        test["pred_mfe_pct"] = mfe_test
        test["accepted"] = accepted
        test["fold"] = fi
        test["threshold"] = threshold
        all_test.append(test[test.accepted].copy())
        # Family evidence is diagnostic only. No tiny-n family veto is applied.
        vdiag = va.assign(p=p_val, expected_gross=gross_if_target)
        for (fam, direction), grp in vdiag.groupby(["family", "direction"]):
            family_diagnostics.append({
                "fold": fi, "family": int(fam), "direction": direction, "n": int(len(grp)),
                "mean_target_prob": float(grp.p.mean()),
                "empirical_target_rate": float(grp.target_before_stop.mean()),
                "empirical_mean_mfe": float(grp.mfe.mean()),
                "diagnostic_net_proxy": float((grp["horizon_close_return_pct"] - COST_PCT - SLIPPAGE_BPS_ROUNDTRIP / 100.0).mean()),
            })
        auc = None
        if va.target_before_stop.nunique() == 2:
            try:
                auc = float(roc_auc_score(va.target_before_stop, p_val))
            except Exception:
                auc = None
        fold_reports.append({
            "fold": fi, "train_rows": int(len(tr)), "val_rows": int(len(va)), "test_candidates": int(len(te)),
            "selected": int(accepted.sum()), "threshold": threshold,
            "validation_auc": auc,
            "selected_mean_pred_net_stress_pct": float(expected_net_test[accepted].mean()) if accepted.any() else None,
            "selected_empirical_target_rate": float(te.loc[accepted, "target_before_stop"].mean()) if accepted.any() else None,
            "candidate_start_bar_min": int(te.bar.min()) if len(te) else None,
            "candidate_start_bar_max": int(te.bar.max()) if len(te) else None,
        })
    sel = pd.concat(all_test, ignore_index=True) if any(len(x) for x in all_test) else pd.DataFrame()
    return sel, pd.DataFrame(fold_reports), pd.DataFrame(family_diagnostics)


def simulate_portfolio(sel: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
    if sel.empty:
        return pd.DataFrame()
    path_map = {(str(sym), day): g.sort_values("timestamp").reset_index(drop=True) for (sym, day), g in df.groupby(["symbol", "day"], sort=False)}
    trades = []
    for day, day_sel in sel.groupby("day"):
        # Rank the strongest opportunity available at EACH decision timestamp,
        # rather than sorting globally by timestamp only.
        day_sel = day_sel.sort_values(["timestamp", "pred_expected_net_stress_pct"], ascending=[True, False])
        used_symbols = set()
        active_exit_bars: List[int] = []
        day_trades = 0
        for ts, bucket in day_sel.groupby("timestamp", sort=True):
            if day_trades >= MAX_TRADES_DAY:
                break
            active_exit_bars = [x for x in active_exit_bars if x > int(bucket.iloc[0].bar)]
            if len(active_exit_bars) >= MAX_CONCURRENT:
                continue
            for _, r in bucket.sort_values("pred_expected_net_stress_pct", ascending=False).iterrows():
                if day_trades >= MAX_TRADES_DAY or len(active_exit_bars) >= MAX_CONCURRENT:
                    break
                sym = str(r.symbol)
                if sym in used_symbols:
                    continue
                g = path_map.get((sym, day))
                if g is None:
                    continue
                idxs = np.flatnonzero(g.timestamp.to_numpy() == r.timestamp)
                if len(idxs) == 0:
                    continue
                i = int(idxs[0])
                path = g.iloc[i + 1:i + H + 1]
                if len(path) < H:
                    continue
                bo = barrier_outcome(float(r.entry), path.high.to_numpy(), path.low.to_numpy(), side=r.direction,
                                     target_pct=TARGET_PCT, stop_pct=STOP_PCT)
                if bo.outcome == "target_before_stop":
                    gross_pct = TARGET_PCT
                    exit_bar = int(r.bar) + int(bo.target_bars or H)
                elif bo.outcome == "stop_before_target":
                    gross_pct = -STOP_PCT
                    exit_bar = int(r.bar) + int(bo.stop_bars or H)
                else:
                    gross_pct = float(r.horizon_close_return_pct)
                    exit_bar = int(r.bar) + H
                gross_inr = NOTIONAL * gross_pct / 100.0
                slip = NOTIONAL * SLIPPAGE_BPS_ROUNDTRIP / 10000.0
                net = gross_inr - COST - slip
                trades.append({
                    "day": day, "timestamp": r.timestamp, "symbol": sym, "direction": r.direction,
                    "family": int(r.family), "entry": float(r.entry), "gross_pct": gross_pct,
                    "gross_pnl": gross_inr, "mandatory_cost": COST, "slippage_stress": slip, "net": net,
                    "pred_target_prob": float(r.pred_target_prob),
                    "pred_expected_net_stress_pct": float(r.pred_expected_net_stress_pct),
                    "mfe_pct": float(r.mfe), "mae_pct": float(r.mae),
                    "barrier_outcome": bo.outcome, "exit_bar": exit_bar,
                })
                used_symbols.add(sym)
                active_exit_bars.append(exit_bar)
                day_trades += 1
    return pd.DataFrame(trades)


def build_report(cand, sel, fold_report, family_diag, trades):
    report = {
        "engine_version": ENGINE_VERSION,
        "data_interval": "5m compatibility research only",
        "sessions": int(cand.day.nunique()) if not cand.empty else 0,
        "symbols": int(cand.symbol.nunique()) if not cand.empty else 0,
        "candidate_rows": int(len(cand)),
        "oos_selected": int(len(sel)),
        "trades": int(len(trades)),
        "mandatory_cost_per_trade_inr": COST,
        "mandatory_cost_pct_roundtrip": COST_PCT,
        "slippage_stress_bps_roundtrip": SLIPPAGE_BPS_ROUNDTRIP,
        "production_authorized": False,
        "note": "This script is not automatically executed on import. No simulation runs merely by starting the app.",
    }
    if not trades.empty:
        wins = trades[trades.net > 0]
        losses = trades[trades.net <= 0]
        report.update({
            "wins": int(len(wins)), "losses": int(len(losses)),
            "win_rate_pct": float(len(wins) / len(trades) * 100.0),
            "gross_pnl": float(trades.gross_pnl.sum()),
            "mandatory_cost": float(trades.mandatory_cost.sum()),
            "slippage_stress": float(trades.slippage_stress.sum()),
            "net_pnl": float(trades.net.sum()),
            "avg_net_per_trade": float(trades.net.mean()),
            "profit_factor": float(wins.net.sum() / abs(losses.net.sum())) if len(losses) and losses.net.sum() != 0 else None,
            "max_drawdown": float((trades.net.cumsum().cummax() - trades.net.cumsum()).max()),
        })
    return report


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    if not DATA.exists():
        raise FileNotFoundError(f"research data not found: {DATA}")
    df = pd.read_csv(DATA, parse_dates=["timestamp"])
    df["day"] = pd.to_datetime(df["day"]).dt.date
    df = df.sort_values(["symbol", "timestamp"]).reset_index(drop=True)
    df = df.groupby("symbol", group_keys=False).apply(add_features).reset_index(drop=True)
    df = df.groupby("symbol", group_keys=False).apply(add_forward_labels).reset_index(drop=True)
    cand = build_candidate_ledger(df)
    cand = attach_barrier_labels(cand, df)
    if cand.empty:
        sel = pd.DataFrame(); fold_report = pd.DataFrame(); family_diag = pd.DataFrame(); trades = pd.DataFrame()
    else:
        sel, fold_report, family_diag = run_walk_forward(cand)
        trades = simulate_portfolio(sel, df)
    summary = build_report(cand, sel, fold_report, family_diag, trades)
    cand.to_csv(OUT / "candidate_ledger.csv", index=False)
    sel.to_csv(OUT / "oos_selected.csv", index=False)
    fold_report.to_csv(OUT / "fold_report.csv", index=False)
    family_diag.to_csv(OUT / "family_validation_diagnostics.csv", index=False)
    trades.to_csv(OUT / "portfolio_trades.csv", index=False)
    (OUT / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
    (OUT / "V3.2.1_RESEARCH_REPORT.md").write_text("# V3.2.1 Research Report\n\n```json\n" + json.dumps(summary, indent=2, default=str) + "\n```\n\nNo simulation is executed on import.\n")
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
