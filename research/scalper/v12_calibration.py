"""Leakage-safe empirical target/stop calibration for UltraScalp V12.

The calibration label is the first barrier reached (gross target before
protection, or protection before target) within a fixed horizon. It is not a
generic probability of net profit. Profiles are versioned and execution-policy
metadata must match before the live/paper scorer may use them.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, Sequence
import numpy as np
import pandas as pd


@dataclass(frozen=True)
class V12CalibrationProfile:
    version: str
    horizon_seconds: int
    round_trip_cost_pct: float
    entry_slippage_pct: float
    target_gross_pct: float
    protection_pct: float
    exit_policy_id: str
    score_edges: list
    target_hit_rates: list
    adverse_rates: list
    sample_counts: list
    train_rows: int

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _future_outcomes(
    snapshots: pd.DataFrame,
    *,
    horizon_seconds: int,
    target_gross_pct: float,
    protection_pct: float,
    max_horizon_overshoot_seconds: int = 30,
):
    """Return signed terminal return and first-barrier outcomes without session leakage.

    A row is labelled only if an observation exists at the horizon (within the
    allowed tolerance). We never carry a ticker's path across sessions/dates.
    """
    required = {"ticker", "captured_at", "ltp", "signal_direction"}
    missing = required.difference(snapshots.columns)
    if missing:
        raise ValueError(f"Missing calibration columns: {sorted(missing)}")

    s = snapshots.copy()
    s["captured_at"] = pd.to_datetime(s["captured_at"])
    s["_cal_date"] = s["captured_at"].dt.date
    group_cols = ["ticker", "_cal_date"]
    if "session_id" in s.columns:
        group_cols.insert(1, "session_id")
    s = s.sort_values(group_cols + ["captured_at"]).reset_index(drop=True)

    terminal = np.full(len(s), np.nan, dtype=float)
    target_first = np.full(len(s), np.nan, dtype=float)
    adverse_first = np.full(len(s), np.nan, dtype=float)
    direction = pd.to_numeric(s["signal_direction"], errors="coerce").fillna(0).to_numpy(dtype=float)

    for _, group in s.groupby(group_cols, sort=False, dropna=False):
        indices = group.index.to_numpy()
        times = group["captured_at"].astype("datetime64[ns]").astype("int64").to_numpy()
        prices = pd.to_numeric(group["ltp"], errors="coerce").to_numpy(dtype=float)
        dirs = direction[indices]
        for j, ix in enumerate(indices):
            if j + 1 >= len(group):
                continue
            base = prices[j]
            if not np.isfinite(base) or base <= 0 or dirs[j] not in (-1, 1):
                continue
            end_time = times[j] + int(horizon_seconds * 1e9)
            k = int(np.searchsorted(times, end_time, side="left"))
            if k >= len(group):
                continue
            overshoot = (times[k] - end_time) / 1e9
            if overshoot > max_horizon_overshoot_seconds:
                continue
            path = (prices[j:k + 1] / base - 1.0) * 100.0 * dirs[j]
            if not np.isfinite(path).all():
                continue
            terminal[ix] = float(path[-1])
            target_hits = np.flatnonzero(path >= float(target_gross_pct))
            stop_hits = np.flatnonzero(path <= -abs(float(protection_pct)))
            first_target = int(target_hits[0]) if len(target_hits) else None
            first_stop = int(stop_hits[0]) if len(stop_hits) else None
            if first_target is not None and (first_stop is None or first_target < first_stop):
                target_first[ix] = 1.0
                adverse_first[ix] = 0.0
            elif first_stop is not None:
                target_first[ix] = 0.0
                adverse_first[ix] = 1.0
            else:
                target_first[ix] = 0.0
                adverse_first[ix] = 0.0
    return terminal, target_first, adverse_first


def _score_series(s: pd.DataFrame) -> np.ndarray:
    raw = pd.to_numeric(s.get("raw_confidence", s.get("signal_confidence", 0.0)), errors="coerce")
    if not isinstance(raw, pd.Series):
        raw = pd.Series(np.full(len(s), float(raw)), index=s.index)
    raw = raw.fillna(0.0).to_numpy(dtype=float)
    ofi = pd.to_numeric(s.get("ofi_proxy", 0.0), errors="coerce")
    imb = pd.to_numeric(s.get("imbalance_l5", 0.0), errors="coerce")
    micro = pd.to_numeric(s.get("microprice_edge_pct", 0.0), errors="coerce")
    if not isinstance(ofi, pd.Series):
        ofi = pd.Series(np.full(len(s), float(ofi)), index=s.index)
    if not isinstance(imb, pd.Series):
        imb = pd.Series(np.full(len(s), float(imb)), index=s.index)
    if not isinstance(micro, pd.Series):
        micro = pd.Series(np.full(len(s), float(micro)), index=s.index)
    combined = 0.45 * ofi.fillna(0.0).to_numpy() + 0.35 * imb.fillna(0.0).to_numpy() + 0.20 * micro.fillna(0.0).to_numpy()
    return raw + 8.0 * np.tanh(combined)


def fit_v12_calibration(
    snapshots: pd.DataFrame,
    *,
    train_fraction: float = 1.0,
    horizon_seconds: int = 300,
    round_trip_cost_pct: float = 0.1363,
    protection_pct: float = 0.18,
    target_net_pct: float = 0.4487,
    entry_slippage_pct: float = 0.015,
    bins: int = 12,
    max_horizon_overshoot_seconds: int = 30,
    exit_policy_id: str = "fixed_target_stop_first_touch_v1",
) -> V12CalibrationProfile:
    if snapshots.empty:
        raise ValueError("No snapshots supplied.")
    if not 0 < float(train_fraction) <= 1:
        raise ValueError("train_fraction must be in (0, 1].")
    if int(horizon_seconds) <= 0 or int(bins) < 2:
        raise ValueError("horizon_seconds must be positive and bins must be at least 2.")
    if exit_policy_id != "fixed_target_stop_first_touch_v1":
        raise ValueError("This fitter only creates fixed-target/stop first-touch profiles; live exit-policy calibration needs a matching simulator.")

    s = snapshots.copy()
    s["captured_at"] = pd.to_datetime(s["captured_at"])
    unique_times = np.sort(s["captured_at"].dropna().unique())
    if len(unique_times) < 2:
        raise ValueError("Not enough distinct timestamps for chronological calibration.")
    if train_fraction >= 1:
        train = s.copy()
    else:
        cutoff_index = max(1, min(len(unique_times) - 1, int(len(unique_times) * train_fraction)))
        cutoff = unique_times[cutoff_index]
        train = s.loc[s["captured_at"] < cutoff].copy()

    train = train[pd.to_numeric(train["signal_direction"], errors="coerce").fillna(0).isin([-1, 1])].copy()
    target_gross_pct = float(round_trip_cost_pct) + float(entry_slippage_pct) + float(target_net_pct)
    terminal, target_first, adverse_first = _future_outcomes(
        train,
        horizon_seconds=int(horizon_seconds),
        target_gross_pct=target_gross_pct,
        protection_pct=float(protection_pct),
        max_horizon_overshoot_seconds=int(max_horizon_overshoot_seconds),
    )
    scores = _score_series(train)
    valid = np.isfinite(scores) & np.isfinite(terminal) & np.isfinite(target_first) & np.isfinite(adverse_first)
    scores = scores[valid]
    target_first = target_first[valid]
    adverse_first = adverse_first[valid]
    if len(scores) < 100:
        raise ValueError(f"Not enough fully labelled training rows: {len(scores)}")

    edges = np.unique(np.quantile(scores, np.linspace(0, 1, int(bins) + 1)))
    if len(edges) < 3:
        edges = np.array([scores.min() - 1e-9, np.median(scores), scores.max() + 1e-9])
    bin_index = np.clip(np.searchsorted(edges, scores, side="right") - 1, 0, len(edges) - 2)
    target_rates, adverse_rates, counts = [], [], []
    for b in range(len(edges) - 1):
        mask = bin_index == b
        n = int(mask.sum())
        counts.append(n)
        target_rates.append(float((target_first[mask].sum() + 1.0) / (n + 2.0)) if n else 0.5)
        adverse_rates.append(float((adverse_first[mask].sum() + 1.0) / (n + 2.0)) if n else 0.5)

    return V12CalibrationProfile(
        version="v12-calibration-v2-first-barrier",
        horizon_seconds=int(horizon_seconds),
        round_trip_cost_pct=float(round_trip_cost_pct),
        entry_slippage_pct=float(entry_slippage_pct),
        target_gross_pct=target_gross_pct,
        protection_pct=float(protection_pct),
        exit_policy_id=str(exit_policy_id),
        score_edges=[float(x) for x in edges.tolist()],
        target_hit_rates=target_rates,
        adverse_rates=adverse_rates,
        sample_counts=counts,
        train_rows=int(len(scores)),
    )


def validate_v12_calibration_profile(
    profile: Dict[str, Any],
    *,
    horizon_seconds: int,
    round_trip_cost_pct: float,
    entry_slippage_pct: float,
    target_gross_pct: float,
    protection_pct: float,
    exit_policy_id: str = "fixed_target_stop_first_touch_v1",
    tolerance: float = 1e-6,
) -> tuple[bool, str]:
    """Reject legacy or policy-mismatched profiles rather than silently misusing them."""
    if profile.get("version") != "v12-calibration-v2-first-barrier":
        return False, "unsupported_profile_version"
    expected = {
        "horizon_seconds": int(horizon_seconds),
        "round_trip_cost_pct": float(round_trip_cost_pct),
        "entry_slippage_pct": float(entry_slippage_pct),
        "target_gross_pct": float(target_gross_pct),
        "protection_pct": float(protection_pct),
    }
    if profile.get("exit_policy_id") != exit_policy_id:
        return False, "mismatched_exit_policy_id"
    for key, wanted in expected.items():
        try:
            actual = float(profile[key])
        except (KeyError, TypeError, ValueError):
            return False, f"missing_{key}"
        if abs(actual - float(wanted)) > tolerance:
            return False, f"mismatched_{key}"
    edges = np.asarray(profile.get("score_edges", []), dtype=float)
    target = np.asarray(profile.get("target_hit_rates", []), dtype=float)
    adverse = np.asarray(profile.get("adverse_rates", []), dtype=float)
    if len(edges) != len(target) + 1 or len(target) != len(adverse) or len(target) < 2:
        return False, "invalid_profile_shape"
    if not (np.isfinite(edges).all() and np.isfinite(target).all() and np.isfinite(adverse).all()):
        return False, "nonfinite_profile_values"
    if ((target < 0) | (target > 1)).any() or ((adverse < 0) | (adverse > 1)).any():
        return False, "probability_out_of_range"
    return True, "compatible"


def apply_v12_calibration(scores: Sequence[float], profile: Dict[str, Any]) -> tuple[np.ndarray, np.ndarray]:
    if profile.get("version") != "v12-calibration-v2-first-barrier":
        raise ValueError("Unsupported calibration profile; expected v12-calibration-v2-first-barrier.")
    edges = np.asarray(profile["score_edges"], dtype=float)
    target = np.asarray(profile["target_hit_rates"], dtype=float)
    adverse = np.asarray(profile["adverse_rates"], dtype=float)
    x = np.asarray(scores, dtype=float)
    bi = np.clip(np.searchsorted(edges, x, side="right") - 1, 0, len(target) - 1)
    return target[bi], adverse[bi]


def calibrate_snapshot_frame(snapshots: pd.DataFrame, profile: Dict[str, Any]) -> pd.DataFrame:
    out = snapshots.copy()
    scores = _score_series(out)
    target, adverse = apply_v12_calibration(scores, profile)
    out["v12_calibration_score"] = scores
    out["v12_p_target_first"] = target
    out["v12_p_adverse_first"] = adverse
    return out
