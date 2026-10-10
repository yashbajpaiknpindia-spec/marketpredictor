"""Real-market UltraScalp paper-capture worker.

The worker deliberately uses only capabilities that the current INDstocks
API documents: full live quotes/market depth snapshots (5 levels) and the
instrument master.  It does *not* claim to have 20/200 levels or historical
order-book reconstruction.

It collects real market depth continuously, derives L1/L5 book features and
book-change proxies, feeds the frozen UltraScalp rules, and records PAPER-only
entries/exits.  No broker order placement occurs here.
"""
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import asdict
from datetime import datetime, timedelta, time as dtime
import math
import os
import hashlib
import threading
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import joblib
import pandas as pd

from .ultra_scalper_engine import ScalperConfig, score_event

IST = ZoneInfo("Asia/Kolkata")
PREOPEN_TIME = dtime(9, 0)
OPEN_TIME = dtime(9, 15)
CLOSE_TIME = dtime(15, 30)
FORCE_EXIT_TIME = dtime(15, 25)


DEFAULT_SETTINGS: Dict[str, Any] = {
    "enabled": False,
    "paper_only": True,
    "auto_start": True,
    "market": "IN",
    "universe_limit": 250,
    "poll_interval_ms": 1000,
    "persist_mode": "full_5level",
    # V12 live-paper policy: accept any positive net opportunity after configured round-trip friction; a small buffer prevents zero-rounding.
    "v12_mode": True,
    "target_net_pct": 0.20,
    "v12_safety_timeout_minutes": 30,
    "v12_entry_latency_bars": 1,
    "v12_profit_lock_enabled": True,
    "v12_economic_lock_net_pct": 0.20,
    "v12_profit_lock_arm_net_pct": 0.075,
    "v12_profit_lock_trail_gross_pct": 0.075,
    "v12_profit_lock_trail_fraction": 0.25,
    "v12_thesis_flip_persistence": 2,
    # Raw V12 confidence is a score, never a probability. Live entry requires
    # an explicitly trained, frozen calibration profile.
    "v12_research_probability_candidate": 0.70,
    "v12_max_adverse_probability": 0.80,
    "v12_require_calibration": False,
    "v12_calibration_profile": None,
    "v12_model_artifact_path": "research/scalper/artifacts/v12_direction_edge_production.joblib",
    "v12_model_enabled": True,
    "v12_min_model_expected_edge_pct": 0.0,
    "v12_min_model_target_probability": 0.70,
    # Frozen positive-edge research gate discovered on real S13/S14.
    # This is an execution gate only; it does not alter the generic research scorer.
    "v12_research_gate_enabled": True,
    "v12_research_max_spread_pct": 0.15,
    "v12_research_price_lookback": 3,
    "target_pct": 0.60,  # executable gross target after cost/slippage reserve.
    "protection_pct": 0.18,
    "max_hold_minutes": 30,
    "round_trip_cost_pct": 0.1363,
    "entry_slippage_pct": 0.015,
    "min_remaining_edge_pct": 0.005,
    "min_signal_score": 65.0,
    "max_entry_lag_bars": 0,
    "freshness_decay_per_bar": 0.15,
    "min_notional_inr": 200000.0,
    "max_open_positions": 0,
    "paper_capital_inr": 200000.0,
    "store_signal_snapshots_only": False,
    "preopen_capture": True,
}


def _v12_gross_target_pct(settings: Dict[str, Any]) -> float:
    """Return the explicit V12 execution target.

    V12 separates the economic lock from the full execution target: +0.20% net
    activates protection, while the normal execution target remains +0.60% gross.
    This prevents the UI from showing a target that the worker does not actually use.
    """
    return max(0.05, float(settings.get("target_pct", 0.60)))


def _num(v: Any, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return default
        return float(str(v).replace(",", ""))
    except (TypeError, ValueError):
        return default


def _ts_ms_to_utc(ms: Any) -> datetime:
    try:
        return datetime.fromtimestamp(float(ms) / 1000.0, tz=IST).astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
    except Exception:
        return datetime.utcnow()


def _local_now() -> datetime:
    return datetime.now(IST)


class SessionHeldElsewhere(Exception):
    """Another process is already capturing today's session; this worker must stand by."""


_PRICE_KEYS = ("price", "rate", "p", "px", "pricevalue", "price_value")
_QTY_KEYS = ("quantity", "qty", "volume", "size", "q", "total_quantity", "totalqty")
_SIDE_ALIASES = {
    "buy": "buy", "bid": "buy", "bids": "buy", "buy_depth": "buy", "bid_levels": "buy",
    "buy_orders": "buy", "buy_side": "buy", "buyorder": "buy", "buyorders": "buy",
    "sell": "sell", "ask": "sell", "asks": "sell", "sell_depth": "sell", "ask_levels": "sell",
    "sell_orders": "sell", "sell_side": "sell", "sellorder": "sell", "sellorders": "sell",
}

def _norm_key(k: Any) -> str:
    return str(k).strip().replace("-", "_").replace(" ", "_").casefold()


def _pick(d: Any, keys) -> float:
    if not isinstance(d, dict):
        return 0.0
    norm = {_norm_key(k): v for k, v in d.items()}
    for k in keys:
        nk = _norm_key(k)
        if nk in norm and norm.get(nk) not in (None, ""):
            return _num(norm.get(nk))
    return 0.0


def _side_rows(v: Any) -> List[Dict[str, float]]:
    """Normalize one side of a book; accepts one row, arrays, or numeric-key maps."""
    if isinstance(v, dict):
        # A single level row: {price: ..., quantity: ...}
        if any(_pick(v, (k,)) for k in _PRICE_KEYS) or any(_pick(v, (k,)) for k in _QTY_KEYS):
            return [{"price": _pick(v, _PRICE_KEYS), "qty": _pick(v, _QTY_KEYS)}]
        values = [v[k] for k in sorted(v.keys(), key=lambda x: str(x)) if isinstance(v[k], dict)]
        v = values
    out: List[Dict[str, float]] = []
    if isinstance(v, list):
        for lvl in v[:5]:
            if isinstance(lvl, dict):
                out.append({"price": _pick(lvl, _PRICE_KEYS), "qty": _pick(lvl, _QTY_KEYS)})
    return out


def _paired_side_lists(node: Any) -> Tuple[List[Dict[str, float]], List[Dict[str, float]], str]:
    if not isinstance(node, dict):
        return [], [], ""
    side_nodes: Dict[str, Any] = {}
    for k, v in node.items():
        alias = _SIDE_ALIASES.get(_norm_key(k))
        if alias:
            side_nodes[alias] = v
    if "buy" in side_nodes and "sell" in side_nodes:
        b = _side_rows(side_nodes["buy"])
        a = _side_rows(side_nodes["sell"])
        if b and a:
            return b[:5], a[:5], "sides"
    # Some payloads expose levels as {"1": {buy: ..., sell: ...}, ...}
    keyed = [v for k, v in sorted(node.items(), key=lambda kv: str(kv[0])) if isinstance(v, dict)]
    if keyed:
        bids: List[Dict[str, float]] = []
        asks: List[Dict[str, float]] = []
        for lvl in keyed[:5]:
            bmap = next((vv for kk, vv in lvl.items() if _SIDE_ALIASES.get(_norm_key(kk)) == "buy" and isinstance(vv, dict)), None)
            amap = next((vv for kk, vv in lvl.items() if _SIDE_ALIASES.get(_norm_key(kk)) == "sell" and isinstance(vv, dict)), None)
            if bmap is not None or amap is not None:
                bids.append({"price": _pick(bmap, _PRICE_KEYS), "qty": _pick(bmap, _QTY_KEYS)})
                asks.append({"price": _pick(amap, _PRICE_KEYS), "qty": _pick(amap, _QTY_KEYS)})
        if bids and asks:
            return bids, asks, "keyed_levels"
    # Flattened level fields, case-insensitive.
    def find_flat(keys):
        for k, v in node.items():
            nk = _norm_key(k)
            if nk in {_norm_key(x) for x in keys} and v not in (None, ""):
                return v
        return None
    bids: List[Dict[str, float]] = []
    asks: List[Dict[str, float]] = []
    for i in range(1, 6):
        bp = find_flat((f"bid_price_{i}", f"bid_price{i}", f"buy_price_{i}", f"buy_price{i}", f"bid_{i}", f"buy_{i}"))
        bq = find_flat((f"bid_qty_{i}", f"bid_qty{i}", f"bid_quantity_{i}", f"bid_quantity{i}", f"buy_qty_{i}", f"buy_quantity_{i}", f"buy_quantity{i}"))
        ap = find_flat((f"ask_price_{i}", f"ask_price{i}", f"sell_price_{i}", f"sell_price{i}", f"ask_{i}", f"sell_{i}"))
        aq = find_flat((f"ask_qty_{i}", f"ask_qty{i}", f"ask_quantity_{i}", f"ask_quantity{i}", f"sell_qty_{i}", f"sell_quantity_{i}", f"sell_quantity{i}"))
        if bp is not None or bq is not None or ap is not None or aq is not None:
            bids.append({"price": _num(bp), "qty": _num(bq)})
            asks.append({"price": _num(ap), "qty": _num(aq)})
    if bids and asks:
        return bids, asks, "flat_levels"
    return [], [], ""


def extract_depth_levels(quote: Any) -> Dict[str, Any]:
    """Normalize real provider depth without depending on undocumented casing/nesting.

    Primary documented shape is market_depth.depth[].buy/sell, but the live probe also
    tolerates equivalent nested/keyed/camel-case variants. No synthetic levels are created.
    """
    best_bids: List[Dict[str, float]] = []
    best_asks: List[Dict[str, float]] = []
    best_shape = "none"
    queue: List[Tuple[str, Any, int]] = [("root", quote, 0)]
    seen = set()
    while queue and not (best_bids and best_asks):
        path, node, depth = queue.pop(0)
        if depth > 12 or id(node) in seen:
            continue
        if isinstance(node, (dict, list)):
            seen.add(id(node))
        if isinstance(node, list):
            bids: List[Dict[str, float]] = []
            asks: List[Dict[str, float]] = []
            for lvl in node[:5]:
                if not isinstance(lvl, dict):
                    continue
                buy = next((v for k, v in lvl.items() if _SIDE_ALIASES.get(_norm_key(k)) == "buy" and isinstance(v, dict)), None)
                sell = next((v for k, v in lvl.items() if _SIDE_ALIASES.get(_norm_key(k)) == "sell" and isinstance(v, dict)), None)
                if buy is not None or sell is not None:
                    bids.append({"price": _pick(buy, _PRICE_KEYS), "qty": _pick(buy, _QTY_KEYS)})
                    asks.append({"price": _pick(sell, _PRICE_KEYS), "qty": _pick(sell, _QTY_KEYS)})
                else:
                    bids.append({"price": _pick(lvl, ("bid_price", "buy_price", "bid", "buy")), "qty": _pick(lvl, ("bid_qty", "bid_quantity", "buy_quantity", "buy_qty"))})
                    asks.append({"price": _pick(lvl, ("ask_price", "sell_price", "ask", "sell")), "qty": _pick(lvl, ("ask_qty", "ask_quantity", "sell_quantity", "sell_qty"))})
            if bids and asks:
                best_bids, best_asks, best_shape = bids, asks, f"{path}[list]"
                break
        elif isinstance(node, dict):
            b, a, shape = _paired_side_lists(node)
            if b and a:
                best_bids, best_asks, best_shape = b[:5], a[:5], f"{path}.{shape}"
                break
            # Search every nested mapping/list rather than assuming only documented subkeys.
            for k, v in node.items():
                if isinstance(v, (dict, list)):
                    queue.append((f"{path}.{k}", v, depth + 1))
    valid = sum(1 for b, a in zip(best_bids, best_asks)
                if b.get("price", 0) > 0 and a.get("price", 0) > 0 and b.get("qty", 0) > 0 and a.get("qty", 0) > 0)
    return {"bids": best_bids[:5], "asks": best_asks[:5], "valid_levels": valid, "shape": best_shape}


def _parse_depth(quote: Dict[str, Any]) -> Dict[str, Any]:
    ex = extract_depth_levels(quote)
    bids: List[Dict[str, float]] = list(ex["bids"])
    asks: List[Dict[str, float]] = list(ex["asks"])
    while len(bids) < 5:
        bids.append({"price": 0.0, "qty": 0.0})
    while len(asks) < 5:
        asks.append({"price": 0.0, "qty": 0.0})
    bid1 = bids[0]["price"]
    ask1 = asks[0]["price"]
    bid1q = bids[0]["qty"]
    ask1q = asks[0]["qty"]
    mid = (bid1 + ask1) / 2.0 if bid1 > 0 and ask1 > 0 else 0.0
    spread_abs = max(0.0, ask1 - bid1) if mid else 0.0
    spread_pct = spread_abs / mid * 100.0 if mid else 0.0
    total_bid = sum(x["qty"] for x in bids)
    total_ask = sum(x["qty"] for x in asks)
    denom = total_bid + total_ask
    imbalance_l5 = (total_bid - total_ask) / denom if denom else 0.0
    l1_denom = bid1q + ask1q
    imbalance_l1 = (bid1q - ask1q) / l1_denom if l1_denom else 0.0
    micro = ((ask1 * bid1q) + (bid1 * ask1q)) / l1_denom if l1_denom and bid1 > 0 and ask1 > 0 else mid
    micro_edge_pct = ((micro / mid) - 1.0) * 100.0 if mid else 0.0
    weighted_imb = 0.0
    wsum = 0.0
    for idx, (b, a) in enumerate(zip(bids, asks), start=1):
        w = 1.0 / idx
        d = b["qty"] + a["qty"]
        if d:
            weighted_imb += w * ((b["qty"] - a["qty"]) / d)
            wsum += w
    book_pressure = weighted_imb / wsum if wsum else 0.0
    return {
        "bids": bids,
        "asks": asks,
        "valid_levels": ex["valid_levels"],
        "depth_shape": ex["shape"],
        "bid1": bid1,
        "ask1": ask1,
        "bid1_qty": bid1q,
        "ask1_qty": ask1q,
        "mid": mid,
        "spread_abs": spread_abs,
        "spread_pct": spread_pct,
        "imbalance_l1": imbalance_l1,
        "imbalance_l5": imbalance_l5,
        "microprice": micro,
        "microprice_edge_pct": micro_edge_pct,
        "book_pressure": book_pressure,
        "total_bid_qty": total_bid,
        "total_ask_qty": total_ask,
        "depth_total_qty": total_bid + total_ask,
    }


def _ofi_proxy(prev: Optional[Dict[str, Any]], cur: Dict[str, Any]) -> float:
    """Top-of-book order-flow proxy, bounded to [-1, 1].

    This is intentionally named a proxy: the provider REST market-depth snapshot
    gives displayed depth but not the individual order add/cancel/execution event
    stream needed for canonical order-flow imbalance.
    """
    if not prev:
        return 0.0
    pb, pa = prev.get("bid1", 0.0), prev.get("ask1", 0.0)
    cb, ca = cur.get("bid1", 0.0), cur.get("ask1", 0.0)
    pbq, paq = prev.get("bid1_qty", 0.0), prev.get("ask1_qty", 0.0)
    cbq, caq = cur.get("bid1_qty", 0.0), cur.get("ask1_qty", 0.0)
    if not (cb > 0 and ca > 0):
        return 0.0
    bid_term = cbq if cb > pb else cbq - pbq if cb == pb else -pbq
    ask_term = paq if ca < pa else paq - caq if ca == pa else -caq
    raw = bid_term - ask_term
    denom = max(cur.get("depth_total_qty", 0.0), 1.0)
    return float(np.clip(raw / denom, -1.0, 1.0))


class LivePaperWorker:
    """In-process one-session live collector + paper simulator."""

    def __init__(
        self,
        *,
        get_settings: Callable[[], Dict[str, Any]],
        is_trading_day: Callable[[Any, Any], bool],
        get_universe: Callable[[int], List[str]],
        resolve_scrip: Callable[[str], str],
        fetch_quotes: Callable[[List[str]], Dict[str, Any]],
        create_session: Callable[[Dict[str, Any]], int],
        update_session: Callable[[int, Dict[str, Any]], None],
        persist_snapshot_batch: Callable[[List[Dict[str, Any]]], None],
        persist_trade: Callable[[Dict[str, Any]], None],
        load_open_trades: Callable[[], List[Dict[str, Any]]],
        fetch_market_ltp: Optional[Callable[[], float]] = None,
    ):
        self.get_settings = get_settings
        self.is_trading_day = is_trading_day
        self.get_universe = get_universe
        self.resolve_scrip = resolve_scrip
        self.fetch_quotes = fetch_quotes
        self.create_session = create_session
        self.update_session = update_session
        self.persist_snapshot_batch = persist_snapshot_batch
        self.persist_trade = persist_trade
        self.load_open_trades = load_open_trades
        self.fetch_market_ltp = fetch_market_ltp

        self.stop_event = threading.Event()
        self.lock = threading.RLock()
        self.thread: Optional[threading.Thread] = None
        self.state: Dict[str, Any] = {
            "running": False,
            "armed": False,
            "phase": "disabled",
            "session_id": None,
            "session_date": None,
            "provider": "INDstocks",
            "depth_levels": 5,
            "universe_count": 0,
            "mapped_count": 0,
            "snapshot_count": 0,
            "stored_snapshot_count": 0,
            "signal_count": 0,
            "raw_direction_count": 0,
            "edge_pass_count": 0,
            "score_pass_count": 0,
            "l2_agreement_count": 0,
            "entry_reject_count": 0,
            "open_positions": 0,
            "paper_trade_count": 0,
            "realized_net_pnl_inr": 0.0,
            "last_poll_at": None,
            "last_data_at": None,
            "last_error": None,
            "last_message": "Not armed",
            "next_start": None,
            "credential_status": False,
            "provider_capability": "5-level market depth via INDstocks REST /market/quotes/mkt; full quote and depth are merged per instrument.",
            "market_context_source": "NIFTY 50 when available; otherwise cross-sectional constituent-return proxy",
            "depth_valid_count": 0,
            "depth_zero_count": 0,
            "depth_coverage_pct": 0.0,
            "last_depth_at": None,
            "effective_poll_interval_ms": 1000,
            "last_poll_duration_ms": 0.0,
            "poll_overrun_count": 0,
            "poll_count": 0,
            "provider_latency_ms": 0.0,
            "persist_latency_ms": 0.0,
            "last_quote_requested": 0,
            "last_quote_returned": 0,
            "last_quote_coverage_pct": 0.0,
            "last_invalid_data_count": 0,
            "unusual_data_count": 0,
            "data_quality_status": "NO_DATA",
            "data_quality_warnings": [],
            "latency_status": "NO_DATA",
            "latency_warnings": [],
            "last_poll_error": None,
            "effective_max_open_positions": 1,
            "capital_budget_inr": 200000.0,
            "capital_utilization_inr": 0.0,
            "economic_lock_activated": 0,
            "economic_lock_exits": 0,
            "v12_model_status": "NOT_CHECKED",
            "v12_model_active": False,
            "v12_model_path": None,
            "v12_model_sha256": None,
            "v12_model_error": None,
            "v12_model_last_checked_at": None,
            "entry_rejection_reasons": {},
            "last_poll_entry_rejections": {},
        }
        self._session = None
        self._symbols: Dict[str, str] = {}
        self._prev_depth: Dict[str, Dict[str, Any]] = {}
        self._last_prices: Dict[str, float] = {}
        self._last_volumes: Dict[str, float] = {}
        # Recent real quote prices used by the frozen 3-observation price-confirmation gate.
        self._price_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=4))
        self._bars: Dict[str, deque] = defaultdict(lambda: deque(maxlen=25))
        self._current_bar: Dict[str, Dict[str, Any]] = {}
        self._session_vwap_num = 0.0
        self._session_vwap_den = 0.0
        self._session_volume_prev: Dict[str, float] = {}
        self._market_return_history: deque = deque(maxlen=10)
        self._positions: Dict[str, Dict[str, Any]] = {}
        self._last_entry_at: Dict[str, datetime] = {}
        self._last_signal_direction: Dict[str, int] = {}
        self._snapshot_buffer: List[Dict[str, Any]] = []
        self._last_flush = time.monotonic()
        self._config_fingerprint = None

    def snapshot_state(self) -> Dict[str, Any]:
        # Copy process-local state while holding the lock, then read persisted
        # settings outside it.  Database I/O must never hold the worker state lock.
        with self.lock:
            out = dict(self.state)
            out["thread_alive"] = bool(self.thread and self.thread.is_alive())
            out["positions"] = list(self._positions.values())
        out["settings"] = self.get_settings()
        return out

    def start(self) -> Dict[str, Any]:
        # Do not call snapshot_state() while holding self.lock: snapshot_state()
        # also acquires self.lock, and the old implementation deadlocked every
        # Save & Arm / status auto-start call here.
        with self.lock:
            already_running = bool(self.thread and self.thread.is_alive())
            if not already_running:
                self.stop_event.clear()
                self.state["running"] = True
                self.state["last_error"] = None
                self.thread = threading.Thread(target=self._run, name="ultrascalp-live-paper", daemon=True)
                self.thread.start()
        return {"ok": True, "already_running": already_running, "started": not already_running, "state": self.snapshot_state()}

    def stop(self) -> Dict[str, Any]:
        self.stop_event.set()
        with self.lock:
            self.state["phase"] = "stopping"
            self.state["last_message"] = "Stopping after the current poll. Open paper positions will be force-closed for the session ledger."
        th = self.thread
        if th and th.is_alive() and th is not threading.current_thread():
            th.join(timeout=6.0)
        elif self._session is not None or self.state.get("session_id"):
            self._finalize_session(force=True)
        return {"ok": True, "state": self.snapshot_state()}

    def latest_depth(self, limit: int = 60) -> List[Dict[str, Any]]:
        """Most recent parsed L5 ladder per stock (this process's memory), for the UI inspector."""
        rows: List[Dict[str, Any]] = []
        with self.lock:
            for sym, d in list(self._prev_depth.items()):
                rows.append({
                    "ticker": sym, "ltp": self._last_prices.get(sym),
                    "valid_levels": int(d.get("valid_levels") or 0), "depth_valid": bool(d.get("depth_valid")),
                    "shape": d.get("depth_shape"), "spread_pct": d.get("spread_pct"),
                    "imbalance_l5": d.get("imbalance_l5"), "depth_total_qty": d.get("depth_total_qty"),
                    "bids": d.get("bids") or [], "asks": d.get("asks") or [],
                })
        rows.sort(key=lambda r: (-r["valid_levels"], r["ticker"]))
        return rows[:max(1, int(limit))]

    def _run(self) -> None:
        try:
            while not self.stop_event.is_set():
                settings = self.get_settings()
                enabled = bool(settings.get("enabled"))
                with self.lock:
                    self.state["armed"] = enabled
                    self.state["credential_status"] = bool(self.state.get("credential_status"))
                if not enabled:
                    # Honour a Stop/disable issued from ANY instance (the setting lives in the DB):
                    # close this process's open session instead of leaving a zombie 'running' row.
                    if self._session is not None or self.state.get("session_id"):
                        self._finalize_session(force=True)
                    with self.lock:
                        self.state["phase"] = "disabled"
                        self.state["last_message"] = "Paper capture is disabled."
                    self.stop_event.wait(5)
                    continue

                now = _local_now()
                if not self.is_trading_day("IN", now.date()):
                    with self.lock:
                        self.state["phase"] = "closed_non_trading_day"
                        self.state["next_start"] = None
                        self.state["last_message"] = "NSE is closed today; worker is armed and waiting for the next session."
                    self.stop_event.wait(60)
                    continue

                if now.time() < PREOPEN_TIME:
                    next_dt = datetime.combine(now.date(), PREOPEN_TIME, tzinfo=IST)
                    with self.lock:
                        self.state["phase"] = "armed_waiting"
                        self.state["next_start"] = next_dt.isoformat()
                        self.state["last_message"] = f"Armed. Live 5-level capture will begin at {PREOPEN_TIME.strftime('%H:%M')} IST; paper entries begin at {OPEN_TIME.strftime('%H:%M')} IST."
                    self.stop_event.wait(min(30, max(1, (next_dt - now).total_seconds())))
                    continue

                if now.time() > CLOSE_TIME:
                    self._finalize_session(force=True)
                    tomorrow = (now + timedelta(days=1)).date()
                    with self.lock:
                        self.state["phase"] = "after_close"
                        self.state["next_start"] = datetime.combine(tomorrow, PREOPEN_TIME, tzinfo=IST).isoformat()
                        self.state["last_message"] = "Session complete. Worker remains armed for the next trading day."
                    self.stop_event.wait(60)
                    continue

                if self._session is None or self.state.get("session_date") != now.date().isoformat():
                    try:
                        self._start_session(settings, now)
                    except SessionHeldElsewhere as held:
                        with self.lock:
                            self.state["phase"] = "standby_other_instance"
                            self.state["last_message"] = str(held)
                        self.stop_event.wait(15)
                        continue

                poll_started = time.monotonic()
                try:
                    self._poll_once(settings, now)
                except Exception as poll_exc:  # one bad poll must not kill the capture thread
                    with self.lock:
                        self.state["last_error"] = f"poll: {str(poll_exc)[:300]}"
                        self.state["last_poll_error"] = str(poll_exc)[:500]
                        self.state["last_message"] = "Poll failed; retrying next cycle."
                interval = max(1.0, min(10.0, _num(settings.get("poll_interval_ms"), 1000.0) / 1000.0))
                work_seconds = max(0.0, time.monotonic() - poll_started)
                effective_ms = int(round((work_seconds + max(0.0, interval - work_seconds)) * 1000.0))
                with self.lock:
                    self.state["last_poll_duration_ms"] = round(work_seconds * 1000.0, 2)
                    self.state["poll_count"] = int(self.state.get("poll_count") or 0) + 1
                    self.state["effective_poll_interval_ms"] = effective_ms
                    if work_seconds > interval * 1.15:
                        self.state["poll_overrun_count"] = int(self.state.get("poll_overrun_count") or 0) + 1
                        self.state["latency_status"] = "DEGRADED"
                        warns=list(self.state.get("latency_warnings") or [])
                        msg=f"Poll cycle {work_seconds*1000:.0f} ms exceeded configured {interval*1000:.0f} ms cadence."
                        if msg not in warns: warns.append(msg)
                        self.state["latency_warnings"] = warns[-8:]
                    elif self.state.get("poll_count",0) > 2:
                        self.state["latency_status"] = "OK"
                # Target the configured cadence rather than adding a full interval
                # after a slow provider/DB cycle. If the provider itself takes longer
                # than the requested interval we report the real cadence honestly.
                self.stop_event.wait(max(0.0, interval - work_seconds))
        except Exception as exc:
            with self.lock:
                self.state["last_error"] = str(exc)[:500]
                self.state["last_message"] = "Live paper worker crashed safely; no broker orders were attempted."
        finally:
            self._finalize_session(force=True)
            with self.lock:
                self.state["running"] = False
                if self.state.get("phase") != "after_close":
                    self.state["phase"] = "stopped"

    def _start_session(self, settings: Dict[str, Any], now: datetime) -> None:
        self._positions.clear()
        self._prev_depth.clear()
        self._last_prices.clear()
        self._last_volumes.clear()
        self._price_history.clear()
        self._bars.clear()
        self._current_bar.clear()
        self._market_return_history.clear()
        self._last_market_ltp = 0.0
        self._session_vwap_num = 0.0
        self._session_vwap_den = 0.0
        self._session_volume_prev.clear()
        self._snapshot_buffer.clear()
        self._last_flush = time.monotonic()

        candidates, mapped, failures = self._resolve_universe(settings)
        self._last_map_try = time.monotonic()
        self._symbols = mapped
        self.state["credential_status"] = bool(mapped)
        session_payload = {
            "session_date": now.date().isoformat(),
            "started_at": now.replace(tzinfo=None),
            "status": "running",
            "provider": "INDstocks",
            "depth_levels": 5,
            "universe_count": len(candidates),
            "mapped_count": len(mapped),
            "config": {
                **{k: settings.get(k) for k in DEFAULT_SETTINGS.keys()},
                "universe_source": "Nifty LargeMidcap 250",
                "universe_selection_method": "official_250_with_liquid_priority_for_smaller_selection",
                "universe_symbols": list(candidates),
                "mapping_failures": failures[:50],
            },
        }
        self.state["session_id"] = self.create_session(session_payload)  # may raise SessionHeldElsewhere
        for k, v in (session_payload.get("resumed_counts") or {}).items():
            self.state[k] = v
        self._session = session_payload
        # Recover any OPEN paper positions from an application restart so the
        # research ledger continues to manage/close them rather than orphaning them.
        try:
            for row in self.load_open_trades() or []:
                self._positions[str(row.get("ticker"))] = dict(row)
        except Exception:
            pass
        self.state["session_date"] = now.date().isoformat()
        self.state["universe_count"] = len(candidates)
        self.state["mapped_count"] = len(mapped)
        if mapped:
            self.state["phase"] = "capturing_preopen" if now.time() < OPEN_TIME else "live_paper"
            self.state["last_message"] = f"Session armed: {len(mapped)}/{len(candidates)} instruments resolved."
        else:
            self.state["phase"] = "waiting_instruments"
            self.state["last_message"] = "Worker is up but 0 instruments could be mapped (INDstocks login/instrument list unavailable). Nothing is being recorded; retrying every 30 s."

    def _resolve_universe(self, settings: Dict[str, Any]):
        candidates = self.get_universe(int(settings.get("universe_limit") or 250))
        mapped: Dict[str, str] = {}
        failures: List[str] = []
        for symbol in candidates:
            try:
                mapped[symbol] = self.resolve_scrip(symbol)
            except Exception as exc:
                msg = str(exc)
                failures.append(f"{symbol}: {msg[:120]}")
                # Only "this one ticker is not in the instrument master" is a per-symbol problem.
                # Anything else (token refused, Cloudflare challenge, instrument download failed, network)
                # would fail identically for every remaining symbol - stop instead of hammering the provider.
                per_symbol = ("Unknown symbol" in msg) or ("no NSE listing" in msg) or ("No security_id" in msg)
                if not per_symbol:
                    failures.append(f"(stopped after first provider/auth failure; {len(candidates) - len(mapped) - len(failures) + 1} symbols not attempted)")
                    break
        return candidates, mapped, failures

    def _poll_once(self, settings: Dict[str, Any], now: datetime) -> None:
        # Close every open paper position before the exchange session boundary,
        # even if the provider returns no quote in this cycle. This prevents a
        # stale/missing quote from allowing a position to cross the session close.
        if now.time() >= FORCE_EXIT_TIME and self._positions:
            for _symbol in list(self._positions):
                _pos = self._positions.get(_symbol)
                _ltp = self._last_prices.get(_symbol)
                if _pos and _ltp and _ltp > 0:
                    self._manage_position(
                        _symbol,
                        str(_pos.get("scrip_code") or ""),
                        now,
                        float(_ltp),
                        self._prev_depth.get(_symbol) or {},
                        settings,
                        None,
                    )
        if not self._symbols:
            # Mapping failed at session start (usually INDstocks auth/instrument master unavailable).
            # Previously the worker sat here forever; retry mapping every 30 s and say why we wait.
            if time.monotonic() - getattr(self, "_last_map_try", 0.0) >= 30.0:
                self._last_map_try = time.monotonic()
                candidates, mapped, failures = self._resolve_universe(settings)
                if mapped:
                    self._symbols = mapped
                    with self.lock:
                        self.state["mapped_count"] = len(mapped)
                        self.state["universe_count"] = len(candidates)
                        self.state["credential_status"] = True
                    try:
                        if self.state.get("session_id"):
                            self.update_session(int(self.state["session_id"]), {"mapped_count": len(mapped), "universe_count": len(candidates)})
                    except Exception:
                        pass
                else:
                    with self.lock:
                        self.state["last_error"] = (failures[0] if failures else "no instruments resolved")[:300]
            with self.lock:
                self.state["phase"] = "waiting_instruments"
                self.state["last_message"] = "Nothing is being recorded: 0 instruments mapped (INDstocks login/instrument list unavailable). Retrying every 30 s - see the L5 panel for the exact error."
                self.state["depth_last_poll"] = {"at": _local_now().isoformat(), "valid": 0, "zero": 0, "total": 0, "mapped": 0}
            self._heartbeat()
            if not self._symbols:
                return
        codes = list(self._symbols.values())
        provider_started = time.monotonic()
        quotes = self.fetch_quotes(codes)
        provider_ms = (time.monotonic() - provider_started) * 1000.0
        quote_by_code = quotes or {}
        with self.lock:
            self.state["provider_latency_ms"] = round(provider_ms, 2)
            self.state["last_quote_requested"] = len(codes)
            self.state["last_quote_returned"] = len(quote_by_code)
            self.state["last_quote_coverage_pct"] = round((len(quote_by_code) / len(codes) * 100.0), 2) if codes else 0.0
        current_returns: List[float] = []
        market_ltp = 0.0
        if self.fetch_market_ltp is not None:
            try:
                market_ltp = float(self.fetch_market_ltp() or 0.0)
            except Exception:
                market_ltp = 0.0
        if market_ltp > 0:
            prev_market = float(getattr(self, "_last_market_ltp", 0.0) or 0.0)
            if prev_market > 0:
                self._market_return_history.append((market_ltp / prev_market - 1.0) * 100.0)
            self._last_market_ltp = market_ltp
            with self.lock:
                self.state["market_context_source"] = "NIFTY 50 live LTP"
        processed = 0
        depth_valid = 0
        depth_zero = 0
        signal_events = 0
        raw_direction_events = 0
        edge_pass_events = 0
        score_pass_events = 0
        l2_agreement_events = 0
        entry_rejects = 0
        poll_rejection_reasons: Dict[str, int] = {}
        invalid_data = 0
        unusual_data = []
        for symbol, code in self._symbols.items():
            q = quote_by_code.get(code) or {}
            if not q:
                continue
            ltp = _num(q.get("live_price"))
            volume = _num(q.get("volume"))
            if not math.isfinite(ltp) or ltp <= 0:
                invalid_data += 1
                if len(unusual_data) < 8: unusual_data.append(f"{symbol}: invalid/non-positive LTP")
                continue
            if not math.isfinite(volume) or volume < 0:
                invalid_data += 1
                if len(unusual_data) < 8: unusual_data.append(f"{symbol}: invalid volume")
                volume = 0.0
            depth = _parse_depth(q)
            spread_pct = float(depth.get("spread_pct") or 0.0)
            if not math.isfinite(spread_pct) or spread_pct < 0 or spread_pct > 5.0:
                unusual_data.append(f"{symbol}: unusual spread {spread_pct:.3f}%")
                invalid_data += 1
                spread_pct = 0.0
                depth["spread_pct"] = spread_pct
            depth_ok = bool(depth.get("depth_total_qty", 0.0) > 0 and depth.get("bid1", 0.0) > 0 and depth.get("ask1", 0.0) > 0)
            if depth_ok:
                depth_valid += 1
                with self.lock:
                    self.state["last_depth_at"] = now.isoformat()
            else:
                depth_zero += 1
            depth["depth_valid"] = depth_ok
            depth["depth_endpoint_ok"] = bool(q.get("_depth_endpoint_ok"))
            previous_depth = self._prev_depth.get(symbol) or {}
            depth["ofi_proxy"] = _ofi_proxy(previous_depth, depth)
            self._prev_depth[symbol] = depth
            prev_price = self._last_prices.get(symbol)
            if prev_price and prev_price > 0:
                current_returns.append((ltp / prev_price - 1.0) * 100.0)
            self._last_prices[symbol] = ltp
            self._price_history[symbol].append(float(ltp))
            self._last_volumes[symbol] = volume
            self._update_vwap(symbol, depth, ltp, volume)
            self._update_bar(symbol, now, ltp, volume, depth)
            features = self._build_features(symbol, now, ltp, volume, depth, previous_depth)
            result = self._score(features, now, settings)
            if not depth.get("depth_valid"):
                result["l2_pass"] = False
                result["direction"] = None
                result["rejection_reason"] = "depth_unavailable"
                result["data_quality_gate"] = "blocked_no_l5_depth"
            else:
                result["data_quality_gate"] = "l5_depth_valid"
            if result.get("raw_direction"):
                raw_direction_events += 1
            if result.get("edge_pass"):
                edge_pass_events += 1
            if result.get("score_pass"):
                score_pass_events += 1
            if result.get("l2_pass"):
                l2_agreement_events += 1
            if result.get("direction"):
                signal_events += 1
            self._record_snapshot(symbol, code, now, ltp, volume, depth, result, settings)
            self._manage_position(symbol, code, now, ltp, depth, settings, result)
            accepted, reject_reason = self._maybe_enter(symbol, code, now, ltp, depth, result, settings)
            if not accepted and reject_reason:
                entry_rejects += 1
                poll_rejection_reasons[str(reject_reason)] = poll_rejection_reasons.get(str(reject_reason), 0) + 1
            processed += 1

        persist_started = time.monotonic()
        self._flush_snapshots()
        persist_ms = (time.monotonic() - persist_started) * 1000.0
        if current_returns and not market_ltp:
            self._market_return_history.append(float(np.median(current_returns)))
            with self.lock:
                self.state["market_context_source"] = "cross-sectional constituent-return proxy"
        with self.lock:
            self.state["snapshot_count"] = int(self.state.get("snapshot_count") or 0) + processed
            self.state["signal_count"] = int(self.state.get("signal_count") or 0) + signal_events
            self.state["raw_direction_count"] = int(self.state.get("raw_direction_count") or 0) + raw_direction_events
            self.state["edge_pass_count"] = int(self.state.get("edge_pass_count") or 0) + edge_pass_events
            self.state["score_pass_count"] = int(self.state.get("score_pass_count") or 0) + score_pass_events
            self.state["l2_agreement_count"] = int(self.state.get("l2_agreement_count") or 0) + l2_agreement_events
            self.state["entry_reject_count"] = int(self.state.get("entry_reject_count") or 0) + entry_rejects
            self.state["last_poll_entry_rejections"] = dict(sorted(poll_rejection_reasons.items(), key=lambda item: (-item[1], item[0])))
            cumulative_rejections = dict(self.state.get("entry_rejection_reasons") or {})
            for reason, count in poll_rejection_reasons.items():
                cumulative_rejections[reason] = int(cumulative_rejections.get(reason, 0)) + int(count)
            self.state["entry_rejection_reasons"] = dict(sorted(cumulative_rejections.items(), key=lambda item: (-item[1], item[0])))
            self.state["depth_valid_count"] = int(self.state.get("depth_valid_count") or 0) + depth_valid
            self.state["depth_zero_count"] = int(self.state.get("depth_zero_count") or 0) + depth_zero
            total_depth_samples = self.state["depth_valid_count"] + self.state["depth_zero_count"]
            self.state["depth_coverage_pct"] = round((self.state["depth_valid_count"] / total_depth_samples) * 100.0, 3) if total_depth_samples else 0.0
            self.state["persist_latency_ms"] = round(persist_ms, 2)
            self.state["last_invalid_data_count"] = int(invalid_data)
            self.state["unusual_data_count"] = int(self.state.get("unusual_data_count") or 0) + len(unusual_data)
            warns=list(self.state.get("data_quality_warnings") or [])
            warns.extend(unusual_data)
            if len(quote_by_code) < len(codes) * 0.95:
                warns.append(f"Quote coverage {len(quote_by_code)}/{len(codes)} ({(len(quote_by_code)/len(codes)*100.0 if codes else 0):.1f}%).")
            if self.state["depth_coverage_pct"] < 90.0 and self.state.get("poll_count",0) > 2:
                warns.append(f"L5 coverage is {self.state['depth_coverage_pct']:.1f}%.")
            self.state["data_quality_warnings"] = warns[-12:]
            self.state["data_quality_status"] = "DEGRADED" if (invalid_data or (len(quote_by_code) < len(codes) * 0.95) or (self.state["depth_coverage_pct"] < 90.0 and self.state.get("poll_count",0)>2)) else "OK"
            self.state["depth_last_poll"] = {"at": _local_now().isoformat(), "valid": depth_valid, "zero": depth_zero, "total": depth_valid + depth_zero}
            # The run loop records the actual cycle estimate including request/processing time.
            self.state["open_positions"] = len(self._positions)
            self.state["last_poll_at"] = _local_now().isoformat()
            self.state["last_data_at"] = _local_now().isoformat()
            self.state["phase"] = "capturing_preopen" if now.time() < OPEN_TIME else ("live_paper" if now.time() < FORCE_EXIT_TIME else "squaring_off")
            self.state["last_error"] = None
            self.state["v12_policy"] = {
                "enabled": bool(settings.get("v12_mode", True)),
                "target_net_pct": float(settings.get("target_net_pct", 0.4487)),
                "gross_target_pct": _v12_gross_target_pct(settings),
                "round_trip_cost_pct": float(settings.get("round_trip_cost_pct", 0.1363)),
                "entry_latency_bars": int(settings.get("v12_entry_latency_bars", 1)),
                "safety_timeout_minutes": float(settings.get("v12_safety_timeout_minutes", 30)),
                "profit_lock": bool(settings.get("v12_profit_lock_enabled", True)),
                "research_probability_candidate": float(settings.get("v12_research_probability_candidate", 0.10)),
                "max_adverse_probability": float(settings.get("v12_max_adverse_probability", 0.80)),
                "require_calibration": bool(settings.get("v12_require_calibration", False)),
                "calibration_loaded": bool(settings.get("v12_calibration_profile")),
            }
            self.state["last_message"] = (f"Captured {processed} instruments; {len(self._positions)} paper positions open." if processed else
                                          f"Polled {len(self._symbols)} mapped instruments but received 0 quotes (provider/auth problem) - see the L5 panel.")
        self._heartbeat()

    def _update_vwap(self, symbol: str, depth: Dict[str, Any], ltp: float, volume: float) -> None:
        prev_vol = float(self._session_volume_prev.get(symbol, 0.0))
        delta = max(0.0, volume - prev_vol) if prev_vol else 0.0
        self._session_volume_prev[symbol] = volume
        if delta > 0:
            self._session_vwap_num += ltp * delta
            self._session_vwap_den += delta

    def _update_bar(self, symbol: str, now: datetime, ltp: float, volume: float, depth: Dict[str, Any]) -> None:
        minute = now.replace(second=0, microsecond=0)
        bar = self._current_bar.get(symbol)
        if bar is None or bar["minute"] != minute:
            if bar is not None:
                self._bars[symbol].append(dict(bar))
            self._current_bar[symbol] = {
                "minute": minute,
                "open": ltp,
                "high": ltp,
                "low": ltp,
                "close": ltp,
                "volume": 0.0,
                "last_cum_volume": volume,
            }
            bar = self._current_bar[symbol]
        prev_cum = bar.get("last_cum_volume", volume)
        if volume >= prev_cum:
            bar["volume"] += volume - prev_cum
        else:
            bar["volume"] += volume
        bar["last_cum_volume"] = volume
        bar["high"] = max(bar["high"], ltp)
        bar["low"] = min(bar["low"], ltp)
        bar["close"] = ltp

    def _build_features(self, symbol: str, now: datetime, ltp: float, volume: float, depth: Dict[str, Any], previous_depth: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        bar = self._current_bar[symbol]
        hist = list(self._bars[symbol])
        closes = [float(x["close"]) for x in hist]
        if not hist:
            closes = [float(bar["close"])]
        def ret_n(n: int) -> float:
            if len(closes) < n:
                return 0.0
            p = closes[-n]
            return (bar["close"] / p - 1.0) * 100.0 if p else 0.0
        range_pct = ((bar["high"] - bar["low"]) / bar["close"] * 100.0) if bar["close"] else 0.0
        body_pct = ((bar["close"] - bar["open"]) / bar["open"] * 100.0) if bar["open"] else 0.0
        span = bar["high"] - bar["low"]
        close_loc = (bar["close"] - bar["low"]) / span if span else 0.5
        vol_hist = [float(x.get("volume") or 0.0) for x in hist[-20:]]
        rng_hist = [((x["high"] - x["low"]) / x["close"] * 100.0) if x["close"] else 0.0 for x in hist[-20:]]
        vol_med = float(np.median(vol_hist)) if vol_hist else max(float(bar["volume"]), 1.0)
        rng_med = float(np.median(rng_hist)) if rng_hist else max(range_pct, 0.001)
        vwap = self._session_vwap_num / self._session_vwap_den if self._session_vwap_den else bar["close"]
        mrets = list(self._market_return_history)
        market_ret_1 = float(mrets[-1]) if mrets else 0.0
        market_ret_3 = float(sum(mrets[-3:])) if mrets else 0.0
        ret_1, ret_3, ret_5 = ret_n(1), ret_n(3), ret_n(5)
        return {
            "ret_1": ret_1,
            "ret_3": ret_3,
            "ret_5": ret_5,
            "range_pct": range_pct,
            "body_pct": body_pct,
            "close_location": close_loc,
            "vol_ratio_20": (bar["volume"] / vol_med) if vol_med else 0.0,
            "range_ratio_20": (range_pct / rng_med) if rng_med else 0.0,
            "vwap_distance_pct": ((bar["close"] / vwap) - 1.0) * 100.0 if vwap else 0.0,
            "rs_1": ret_1 - market_ret_1,
            "rs_3": ret_3 - market_ret_3,
            "market_ret_1": market_ret_1,
            "market_ret_3": market_ret_3,
            "ofi": depth.get("ofi_proxy", 0.0),
            "imbalance_l5": depth.get("imbalance_l5", 0.0),
            "imbalance_l1": depth.get("imbalance_l1", 0.0),
            "microprice_edge": depth.get("microprice_edge_pct", 0.0),
            "book_pressure": depth.get("book_pressure", 0.0),
            "spread_pct": depth.get("spread_pct", 0.0),
            "ofi_proxy": depth.get("ofi_proxy", 0.0),
            "microprice_edge_pct": depth.get("microprice_edge_pct", 0.0),
            "depth_total_qty": depth.get("depth_total_qty", 0.0),
            "l5_imbalance_delta": float(depth.get("imbalance_l5", 0.0)) - float((previous_depth or {}).get("imbalance_l5", depth.get("imbalance_l5", 0.0))),
            "ofi_delta": float(depth.get("ofi_proxy", 0.0)) - float((previous_depth or {}).get("ofi_proxy", depth.get("ofi_proxy", 0.0))),
            "microprice_delta": float(depth.get("microprice_edge_pct", 0.0)) - float((previous_depth or {}).get("microprice_edge_pct", depth.get("microprice_edge_pct", 0.0))),
            "spread_delta": float(depth.get("spread_pct", 0.0)) - float((previous_depth or {}).get("spread_pct", depth.get("spread_pct", 0.0))),
            "book_pressure_delta": float(depth.get("book_pressure", 0.0)) - float((previous_depth or {}).get("book_pressure", depth.get("book_pressure", 0.0))),
            "ticker": symbol, "captured_at": now, "ltp": ltp, "volume": volume,
        }

    def _score(self, features: Dict[str, Any], now: datetime, settings: Dict[str, Any]) -> Dict[str, Any]:
        model_profile = None
        model_enabled = bool(settings.get("v12_model_enabled", True))
        path = str(settings.get("v12_model_artifact_path") or "research/scalper/artifacts/v12_direction_edge_production.joblib")
        should_check = (
            not hasattr(self, "_v12_model_checked_path")
            or self._v12_model_checked_path != path
            or (model_enabled and getattr(self, "_v12_model_status", {}).get("status") == "disabled")
            or (time.monotonic() - float(getattr(self, "_v12_model_last_attempt_monotonic", 0.0)) >= 30.0
                and getattr(self, "_v12_model_status", {}).get("status") != "loaded")
        )
        if not model_enabled:
            self._v12_model_profile = None
            self._v12_model_checked_path = path
            self._v12_model_status = {"status": "disabled", "path": path, "sha256": None, "error": None}
        elif should_check:
            self._v12_model_last_attempt_monotonic = time.monotonic()
            self._v12_model_checked_path = path
            try:
                if not os.path.isfile(path):
                    raise FileNotFoundError(f"Model artifact not found at runtime: {path}")
                digest = hashlib.sha256()
                with open(path, "rb") as artifact_file:
                    for chunk in iter(lambda: artifact_file.read(1024 * 1024), b""):
                        digest.update(chunk)
                loaded_profile = joblib.load(path)
                if loaded_profile is None:
                    raise ValueError("joblib.load returned an empty model profile")
                self._v12_model_profile = loaded_profile
                self._v12_model_profile_path = path
                self._v12_model_status = {"status": "loaded", "path": path, "sha256": digest.hexdigest(), "error": None}
            except Exception as exc:
                self._v12_model_profile = None
                self._v12_model_status = {"status": "fallback_heuristic", "path": path, "sha256": None, "error": f"{type(exc).__name__}: {str(exc)[:240]}"}
        model_profile = getattr(self, "_v12_model_profile", None) if model_enabled else None
        status = getattr(self, "_v12_model_status", {"status": "not_checked", "path": path, "sha256": None, "error": None})
        with self.lock:
            self.state["v12_model_status"] = status.get("status", "unknown")
            self.state["v12_model_active"] = bool(model_enabled and model_profile is not None)
            self.state["v12_model_path"] = status.get("path")
            self.state["v12_model_sha256"] = status.get("sha256")
            self.state["v12_model_error"] = status.get("error")
            self.state["v12_model_last_checked_at"] = now.isoformat()
        cfg = ScalperConfig(
            target_pct=_v12_gross_target_pct(settings) if settings.get("v12_mode", True) else float(settings.get("target_pct", 0.60)),
            protection_pct=float(settings.get("protection_pct", 0.18)),
            max_hold_bars=int(settings.get("max_hold_minutes", 10)),
            round_trip_cost_pct=float(settings.get("round_trip_cost_pct", 0.1363)),
            entry_slippage_pct=float(settings.get("entry_slippage_pct", 0.015)),
            min_remaining_edge_pct=float(settings.get("min_remaining_edge_pct", 0.005)),
            max_entry_lag_bars=int(settings.get("max_entry_lag_bars", 0)),
            freshness_decay_per_bar=float(settings.get("freshness_decay_per_bar", 0.15)),
            min_signal_score=float(settings.get("min_signal_score", 65.0)),
            require_market_confirmation=True,
            require_relative_strength=True,
            # The real-data edge gate below uses L5 opposition + microprice confirmation
            # explicitly. Disable the generic combined-L2 veto so it cannot contradict the
            # discovered research rule (which specifically benefited from L5 opposition).
            use_l2_when_available=False,
            calibration_profile=settings.get("v12_calibration_profile"),
            require_calibrated_probability=bool(settings.get("v12_require_calibration", False)) if settings.get("v12_mode", True) else False,
            min_calibrated_net_probability=float(settings.get("v12_research_probability_candidate", 0.10)),
            max_adverse_probability=float(settings.get("v12_max_adverse_probability", 0.80)),
            v12_model_profile=model_profile,
            min_model_expected_edge_pct=float(settings.get("v12_min_model_expected_edge_pct", 0.0)),
            min_model_target_probability=float(settings.get("v12_min_model_target_probability", 0.70)),
        )
        row = pd.Series(features)
        result = score_event(row, cfg, entry_lag_bars=int(settings.get("v12_entry_latency_bars", 1)) if settings.get("v12_mode", True) else 0)
        # Replace the generic engine's legacy L2 labels with the actual data we have.
        micro_signal = 0.45 * float(features.get("ofi", 0.0)) + 0.35 * float(features.get("imbalance_l5", 0.0)) + 0.20 * float(features.get("microprice_edge", 0.0))
        result["l2_levels"] = 5
        result["l2_mode"] = "displayed_5_level_depth"
        result["v12_model_status"] = status.get("status", "unknown")
        result["v12_model_active"] = bool(model_enabled and model_profile is not None)
        result["v12_model_artifact_sha256"] = status.get("sha256")
        result["v12_model_load_error"] = status.get("error")
        # For the live edge gate and existing position management, micro_signal is
        # the directly observed microprice edge, not the legacy weighted L2 blend.
        result["micro_signal"] = float(features.get("microprice_edge_pct", 0.0) or 0.0)
        result["l2_pass"] = True
        result["data_honesty"] = "5-level displayed depth; OFI is a proxy, not event-level order-flow imbalance."
        return result

    def _record_snapshot(self, symbol: str, code: str, now: datetime, ltp: float, volume: float, depth: Dict[str, Any], result: Dict[str, Any], settings: Dict[str, Any]) -> None:
        interesting = bool(result.get("direction")) or symbol in self._positions
        compact = bool(settings.get("store_signal_snapshots_only"))
        if compact and not interesting:
            return
        raw = {
            "session_id": self.state.get("session_id"),
            "captured_at": now.replace(tzinfo=None),
            "ticker": symbol,
            "scrip_code": code,
            "ltp": ltp,
            "volume": volume,
            "bid_prices": [x["price"] for x in depth["bids"]],
            "bid_qtys": [x["qty"] for x in depth["bids"]],
            "ask_prices": [x["price"] for x in depth["asks"]],
            "ask_qtys": [x["qty"] for x in depth["asks"]],
            "spread_pct": depth["spread_pct"],
            "imbalance_l1": depth["imbalance_l1"],
            "imbalance_l5": depth["imbalance_l5"],
            "microprice": depth["microprice"],
            "microprice_edge_pct": depth["microprice_edge_pct"],
            "ofi_proxy": depth.get("ofi_proxy", 0.0),
            "book_pressure": depth["book_pressure"],
            "depth_total_qty": depth["depth_total_qty"],
            "signal_direction": int(result.get("direction") or 0),
            "signal_side": result.get("side"),
            "signal_confidence": float(result.get("confidence") or 0.0),
            "raw_direction": int(result.get("raw_direction") or 0),
            "raw_confidence": float(result.get("raw_confidence") or 0.0),
            "confidence_semantics": "model_score_0_100_not_probability",
            "calibration_status": result.get("calibration_status"),
            "v12_p_net_positive": result.get("v12_p_net_positive"),
            "v12_p_adverse_stop": result.get("v12_p_adverse_stop"),
            "v12_probability_pass": bool(result.get("v12_probability_pass", True)),
            "v12_adverse_risk_pass": bool(result.get("v12_adverse_risk_pass", True)),
            "edge_pass": bool(result.get("edge_pass")),
            "score_pass": bool(result.get("score_pass")),
            "l2_pass": bool(result.get("l2_pass")),
            "rejection_reason": result.get("rejection_reason"),
            "expected_move_pct": float(result.get("expected_move_pct") or 0.0),
            "remaining_edge_pct": float(result.get("remaining_edge_pct") or 0.0),
            "l2_mode": "5_level_displayed_depth",
            "depth_valid": bool(depth.get("depth_valid")),
            "depth_endpoint_ok": bool(depth.get("depth_endpoint_ok")),
            "data_honesty": "OFI_PROXY",
        }
        self._snapshot_buffer.append(raw)

    def _maybe_enter(self, symbol: str, code: str, now: datetime, ltp: float, depth: Dict[str, Any], result: Dict[str, Any], settings: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        if now.time() < OPEN_TIME or now.time() >= FORCE_EXIT_TIME:
            return False, "market_window"
        if symbol in self._positions:
            return False, "already_open"
        configured_max = int(settings.get("max_open_positions") or 0)
        capital_budget = float(settings.get("paper_capital_inr") or 200000.0)
        position_notional = float(settings.get("min_notional_inr") or 200000.0)
        effective_max = configured_max if configured_max > 0 else max(1, int(capital_budget // max(position_notional, 1.0)))
        with self.lock:
            self.state["effective_max_open_positions"] = effective_max
            self.state["capital_budget_inr"] = capital_budget
            self.state["capital_utilization_inr"] = len(self._positions) * position_notional
        if len(self._positions) >= effective_max:
            return False, "capital_limit"
        # Entry confirmation: require the directional thesis to persist across
        # consecutive real snapshots for the same symbol. This targets the
        # observed adverse-selection problem without tuning a price threshold.
        prev_signal = int(self._last_signal_direction.get(symbol, 0) or 0)
        current_signal = int(result.get("direction") or 0)
        if current_signal == 0:
            return False, result.get("rejection_reason") or "no_final_signal"
        if prev_signal != current_signal:
            self._last_signal_direction[symbol] = current_signal
            return False, "entry_confirmation_pending"
        self._last_signal_direction[symbol] = current_signal
        if int(result.get("direction") or 0) == 0:
            return False, result.get("rejection_reason") or "no_final_signal"

        # FROZEN POSITIVE-EDGE LIVE ENTRY GATE
        # Discovered from real S13/S14 research and kept identical to the tested rule:
        #   signal persists,
        #   spread <= 0.15%,
        #   L5 imbalance opposes the signal direction,
        #   price has continued in the signal direction over the prior 3 observations,
        #   microprice edge confirms the signal direction.
        # This is intentionally an entry filter only; exits, capital handling and
        # existing paper-ledger mechanics remain otherwise unchanged.
        if bool(settings.get("v12_research_gate_enabled", True)):
            side = 1 if int(result["direction"]) > 0 else -1
            max_spread = float(settings.get("v12_research_max_spread_pct", 0.15))
            spread = float(depth.get("spread_pct") or 0.0)
            if not math.isfinite(spread) or spread > max_spread:
                return False, "v12_research_spread"
            l5 = float(depth.get("imbalance_l5") or 0.0)
            if not math.isfinite(l5) or side * l5 >= 0.0:
                return False, "v12_research_l5_opposition"
            micro_edge = float(depth.get("microprice_edge_pct") or 0.0)
            if not math.isfinite(micro_edge) or side * micro_edge <= 0.0:
                return False, "v12_research_microprice"
            lookback = max(1, int(settings.get("v12_research_price_lookback", 3)))
            history = list(self._price_history.get(symbol) or [])
            if len(history) < lookback + 1:
                return False, "v12_research_price_confirmation_pending"
            prior = float(history[-(lookback + 1)])
            current = float(history[-1])
            if not math.isfinite(prior) or not math.isfinite(current) or side * (current - prior) <= 0.0:
                return False, "v12_research_price_confirmation"

        if settings.get("v12_mode", True) and settings.get("v12_require_calibration", False):
            if result.get("calibration_status") != "calibrated":
                return False, "calibration_missing"
            if not bool(result.get("v12_probability_pass")):
                return False, "calibrated_probability"
            if not bool(result.get("v12_adverse_risk_pass")):
                return False, "adverse_risk"
        score = float(result.get("confidence") or 0.0)
        if score < float(settings.get("min_signal_score", 65.0)):
            return False, "score"
        # The discovered gate uses microprice confirmation directly. Keep that exact
        # observed value in the trade ledger; the old weighted-L2 agreement veto is removed
        # because it contradicts the validated L5-opposition entry rule.
        side = 1 if int(result["direction"]) > 0 else -1
        micro_signal = float(depth.get("microprice_edge_pct") or 0.0)
        last_entry = self._last_entry_at.get(symbol)
        if last_entry and (now.replace(tzinfo=None) - last_entry).total_seconds() < 30:
            return False, "cooldown"
        gross_target_pct = _v12_gross_target_pct(settings) if settings.get("v12_mode", True) else float(settings.get("target_pct", 0.60))
        target = ltp * (1.0 + side * gross_target_pct / 100.0)
        stop_pct = float(settings.get("protection_pct", 0.18))
        stop = ltp * (1.0 - side * stop_pct / 100.0)
        trade_key = f"{self.state.get('session_id')}|{symbol}|{now.replace(tzinfo=None).isoformat()}"
        trade = {
            "trade_key": trade_key,
            "session_id": self.state.get("session_id"),
            "ticker": symbol,
            "scrip_code": code,
            "side": "LONG" if side > 0 else "SHORT",
            "entry_time": now.replace(tzinfo=None),
            "entry_price": ltp,
            "target_price": target,
            "stop_price": stop,
            "confidence": score,
            "confidence_semantics": "model_score_0_100_not_probability",
            "v12_p_net_positive": result.get("v12_p_net_positive"),
            "v12_p_adverse_stop": result.get("v12_p_adverse_stop"),
            "v12_calibration_status": result.get("calibration_status"),
            "expected_move_pct": float(result.get("expected_move_pct") or 0.0),
            "remaining_edge_pct": float(result.get("remaining_edge_pct") or 0.0),
            "l2_levels": 5,
            "entry_reason": (
                "V12 frozen positive-edge gate: persistent direction + spread<=0.15% + L5 opposition + "
                "3-observation price confirmation + microprice confirmation + 5-level displayed depth"
            ),
            "v12_mode": bool(settings.get("v12_mode", True)),
            "v12_target_net_pct": float(settings.get("v12_economic_lock_net_pct", settings.get("target_net_pct", 0.20))),
            "v12_gross_target_pct": float(gross_target_pct),
            "v12_economic_lock_net_pct": float(settings.get("v12_economic_lock_net_pct", 0.20)),
            "v12_profit_lock_arm_net_pct": float(settings.get("v12_profit_lock_arm_net_pct", 0.075)),
            "v12_profit_lock_trail_gross_pct": float(settings.get("v12_profit_lock_trail_gross_pct", 0.075)),
            "v12_profit_lock_trail_fraction": float(settings.get("v12_profit_lock_trail_fraction", 0.25)),
            "v12_thesis_flip_persistence": int(settings.get("v12_thesis_flip_persistence", 2)),
            "peak_gross_pct": 0.0,
            "economic_lock_active": False,
            "l5_flip_streak": 0,
            "v12_entry_latency_bars": int(settings.get("v12_entry_latency_bars", 1)),
            "entry_micro_signal": float(micro_signal),
            "entry_imbalance_l5": float(depth.get("imbalance_l5") or 0.0),
            "v12_research_gate": {
                "enabled": bool(settings.get("v12_research_gate_enabled", True)),
                "max_spread_pct": float(settings.get("v12_research_max_spread_pct", 0.15)),
                "price_lookback": int(settings.get("v12_research_price_lookback", 3)),
                "l5_opposition": True,
                "microprice_confirmation": True,
                "direction_persistence": True,
            },
            "v12_calibration_status": result.get("calibration_status"),
            "v12_p_net_positive": result.get("v12_p_net_positive"),
            "v12_p_adverse_stop": result.get("v12_p_adverse_stop"),
            "status": "OPEN",
            "notional_inr": float(settings.get("min_notional_inr", 200000.0)),
        }
        self._positions[symbol] = trade
        self._last_entry_at[symbol] = now.replace(tzinfo=None)
        with self.lock:
            self.state["capital_utilization_inr"] = len(self._positions) * position_notional
        self.persist_trade(dict(trade))
        with self.lock:
            self.state["paper_trade_count"] = int(self.state.get("paper_trade_count") or 0) + 1
        return True, None

    def _manage_position(self, symbol: str, code: str, now: datetime, ltp: float, depth: Dict[str, Any], settings: Dict[str, Any], result: Optional[Dict[str, Any]] = None) -> None:
        pos = self._positions.get(symbol)
        if not pos:
            return
        side = 1 if pos["side"] == "LONG" else -1
        target = float(pos["target_price"])
        stop = float(pos["stop_price"])
        exit_reason = None
        if side > 0:
            if ltp >= target:
                exit_reason = "TARGET"
            elif ltp <= stop:
                exit_reason = "STOP"
        else:
            if ltp <= target:
                exit_reason = "TARGET"
            elif ltp >= stop:
                exit_reason = "STOP"
        # Economic/price protection owns favorable trades before L5 thesis failure.
        if exit_reason is None and result:
            current_micro = float(result.get("micro_signal") or 0.0)
            gross_now = side * ((ltp / float(pos["entry_price"])) - 1.0) * 100.0
            cost_pct = float(settings.get("round_trip_cost_pct", 0.1363))
            slippage_pct = float(settings.get("entry_slippage_pct", 0.015))
            net_now = gross_now - cost_pct - slippage_pct

            if bool(settings.get("v12_profit_lock_enabled", True)):
                arm_net = float(settings.get("v12_profit_lock_arm_net_pct", 0.075))
                trail_floor = float(settings.get("v12_profit_lock_trail_gross_pct", 0.075))
                trail_fraction = float(settings.get("v12_profit_lock_trail_fraction", 0.25))
                peak_gross = max(float(pos.get("peak_gross_pct") or 0.0), gross_now)
                trail_gross = max(trail_floor, peak_gross * trail_fraction)
                if net_now >= arm_net:
                    if not pos.get("economic_lock_active"):
                        with self.lock:
                            self.state["economic_lock_activated"] = int(self.state.get("economic_lock_activated") or 0) + 1
                    pos["economic_lock_active"] = True
                    pos["peak_gross_pct"] = peak_gross
                if pos.get("economic_lock_active") and gross_now <= peak_gross - trail_gross:
                    exit_reason = "V12_EARLY_PROFIT_LOCK"
                    with self.lock:
                        self.state["economic_lock_exits"] = int(self.state.get("economic_lock_exits") or 0) + 1

            if exit_reason is None:
                opposite = bool(current_micro and np.sign(current_micro) != np.sign(side))
                streak = int(pos.get("l5_flip_streak") or 0)
                streak = streak + 1 if opposite else 0
                pos["l5_flip_streak"] = streak
                persistence = max(1, int(settings.get("v12_thesis_flip_persistence", 2)))
                # A transient book reversal cannot kill a valid momentum trade.
                # Only a persistent reversal while still net-negative can do so.
                if streak >= persistence and net_now <= 0.0:
                    # This streak is based on microprice_edge_pct, not raw L5 imbalance.
                    # Name the exit honestly so downstream audits do not misattribute it.
                    exit_reason = "V12_THESIS_FAIL_MICROPRICE_REVERSAL_PERSISTENT"
        age_min = (now.replace(tzinfo=None) - pos["entry_time"]).total_seconds() / 60.0
        safety_timeout = float(settings.get("v12_safety_timeout_minutes", settings.get("max_hold_minutes", 30))) if settings.get("v12_mode", True) else float(settings.get("max_hold_minutes", 10))
        if exit_reason is None and age_min >= safety_timeout:
            exit_reason = "V12_SAFETY_TIMEOUT" if settings.get("v12_mode", True) else "TIME_EXIT"
        # Never allow a paper position to survive the session boundary.
        # FORCE_EXIT_TIME is intentionally earlier than the exchange close so the
        # worker has a full buffer to persist the exit even when provider latency spikes.
        if now.time() >= FORCE_EXIT_TIME:
            exit_reason = exit_reason or "FORCE_CLOSE"
        if exit_reason is None:
            return
        gross = side * ((ltp / float(pos["entry_price"])) - 1.0) * 100.0
        net_pct = gross - float(settings.get("round_trip_cost_pct", 0.1363)) - float(settings.get("entry_slippage_pct", 0.015))
        notional = float(pos.get("notional_inr") or 0.0)
        net_inr = notional * net_pct / 100.0
        closed = dict(pos)
        closed.update({
            "exit_time": now.replace(tzinfo=None),
            "exit_price": ltp,
            "gross_pct": gross,
            "net_pct": net_pct,
            "net_pnl_inr": net_inr,
            "exit_reason": exit_reason,
            "status": "CLOSED",
            "holding_minutes": age_min,
            "metadata": {
                "confidence_semantics": "model_score_0_100_not_probability",
                "v12_calibration_status": result.get("calibration_status") if result else pos.get("v12_calibration_status"),
                "v12_p_net_positive": result.get("v12_p_net_positive") if result else pos.get("v12_p_net_positive"),
                "v12_p_adverse_stop": result.get("v12_p_adverse_stop") if result else pos.get("v12_p_adverse_stop"),
                "expected_stop_gross_pct": -abs(float(settings.get("protection_pct", 0.18))) if exit_reason == "STOP" else None,
                "actual_exit_gross_pct": gross,
                "stop_overshoot_pct": max(0.0, abs(gross) - abs(float(settings.get("protection_pct", 0.18)))) if exit_reason == "STOP" else 0.0,
                "round_trip_cost_pct": float(settings.get("round_trip_cost_pct", 0.1363)),
                "entry_slippage_pct": float(settings.get("entry_slippage_pct", 0.015)),
                "economic_lock_net_pct": float(settings.get("v12_economic_lock_net_pct", 0.20)),
                "profit_lock_trail_gross_pct": float(settings.get("v12_profit_lock_trail_gross_pct", 0.30)),
            },
        })
        if exit_reason == "V12_THESIS_FAIL_MICROPRICE_REVERSAL_PERSISTENT":
            closed["metadata"]["exit_trigger_source"] = "microprice_edge_pct"
            closed["metadata"]["exit_trigger_rule"] = "persistent_opposite_microprice_while_net_negative"
        self.persist_trade(closed)
        self._positions.pop(symbol, None)
        with self.lock:
            self.state["realized_net_pnl_inr"] = float(self.state.get("realized_net_pnl_inr") or 0.0) + net_inr
            self.state["open_positions"] = len(self._positions)
            exit_counts = dict(self.state.get("exit_reason_counts") or {})
            exit_counts[exit_reason] = int(exit_counts.get(exit_reason, 0)) + 1
            self.state["exit_reason_counts"] = dict(sorted(exit_counts.items()))

    def _flush_snapshots(self) -> None:
        if not self._snapshot_buffer:
            return
        now = time.monotonic()
        if len(self._snapshot_buffer) < 250 and now - self._last_flush < 2.0:
            return
        batch = self._snapshot_buffer
        self._snapshot_buffer = []
        self._last_flush = now
        try:
            self.persist_snapshot_batch(batch)
            with self.lock:
                self.state["stored_snapshot_count"] = int(self.state.get("stored_snapshot_count") or 0) + len(batch)
        except Exception as exc:
            with self.lock:
                self.state["last_error"] = f"snapshot_persist: {str(exc)[:300]}"
        return

    def _heartbeat(self) -> None:
        sid = self.state.get("session_id")
        if not sid:
            return
        payload = {
            "status": "running",
            "last_heartbeat_at": datetime.now(ZoneInfo("UTC")).replace(tzinfo=None),
            "snapshot_count": int(self.state.get("snapshot_count") or 0),
            "stored_snapshot_count": int(self.state.get("stored_snapshot_count") or 0),
            "signal_count": int(self.state.get("signal_count") or 0),
            "raw_direction_count": int(self.state.get("raw_direction_count") or 0),
            "edge_pass_count": int(self.state.get("edge_pass_count") or 0),
            "score_pass_count": int(self.state.get("score_pass_count") or 0),
            "l2_agreement_count": int(self.state.get("l2_agreement_count") or 0),
            "entry_reject_count": int(self.state.get("entry_reject_count") or 0),
            "open_positions": len(self._positions),
            "paper_trade_count": int(self.state.get("paper_trade_count") or 0),
            "realized_net_pnl_inr": float(self.state.get("realized_net_pnl_inr") or 0.0),
            "phase": self.state.get("phase"),
        }
        try:
            self.update_session(int(sid), payload)
        except Exception as exc:
            with self.lock:
                self.state["last_error"] = f"heartbeat: {str(exc)[:300]}"

    def _finalize_session(self, force: bool = False) -> None:
        if self._session is None and not self.state.get("session_id"):
            return
        now = _local_now()
        # Force-close any open paper positions only at actual session close/worker stop.
        if force and self._positions:
            for symbol in list(self._positions):
                pos = self._positions.get(symbol)
                ltp = self._last_prices.get(symbol)
                if pos and ltp:
                    depth = self._prev_depth.get(symbol) or {}
                    self._manage_position(symbol, pos.get("scrip_code") or "", now, ltp, depth, self.get_settings(), None)
        self._flush_snapshots()
        sid = self.state.get("session_id")
        if sid:
            try:
                self.update_session(int(sid), {
                    "status": "complete" if now.time() >= CLOSE_TIME else "stopped",
                    "finished_at": now.replace(tzinfo=None),
                    "snapshot_count": int(self.state.get("snapshot_count") or 0),
                    "stored_snapshot_count": int(self.state.get("stored_snapshot_count") or 0),
                    "signal_count": int(self.state.get("signal_count") or 0),
                    "paper_trade_count": int(self.state.get("paper_trade_count") or 0),
                    "realized_net_pnl_inr": float(self.state.get("realized_net_pnl_inr") or 0.0),
                })
            except Exception:
                pass
        self._session = None
        self.state["session_id"] = None
