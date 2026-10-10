"""Regression tests for honest V12 exit attribution and counters."""
from __future__ import annotations

import threading
import unittest
from datetime import datetime, timedelta

from research.scalper.scalper_live_paper import LivePaperWorker


class V12ExitAuditTests(unittest.TestCase):
    def setUp(self):
        self.worker = LivePaperWorker.__new__(LivePaperWorker)
        self.worker.lock = threading.RLock()
        self.worker.state = {"realized_net_pnl_inr": 0.0, "open_positions": 1}
        self.worker._positions = {
            "ABC": {
                "ticker": "ABC",
                "side": "LONG",
                "entry_time": datetime(2026, 10, 10, 10, 0, 0),
                "entry_price": 100.0,
                "target_price": 100.60,
                "stop_price": 99.82,
                "notional_inr": 200000.0,
                "l5_flip_streak": 1,
                "economic_lock_active": False,
                "peak_gross_pct": 0.0,
            }
        }
        self.persisted = []
        self.worker.persist_trade = lambda trade: self.persisted.append(dict(trade))

    def test_persistent_microprice_reversal_has_accurate_name_and_source(self):
        now = datetime(2026, 10, 10, 10, 1, 0)
        settings = {
            "v12_mode": True,
            "v12_profit_lock_enabled": False,
            "v12_thesis_flip_persistence": 2,
            "v12_safety_timeout_minutes": 30,
            "max_hold_minutes": 10,
            "round_trip_cost_pct": 0.1363,
            "entry_slippage_pct": 0.015,
            "protection_pct": 0.18,
            "v12_economic_lock_net_pct": 0.20,
            "v12_profit_lock_trail_gross_pct": 0.30,
        }
        self.worker._manage_position(
            "ABC", "ABC", now, 99.95, {}, settings, {"micro_signal": -0.01}
        )

        self.assertEqual(len(self.persisted), 1)
        trade = self.persisted[0]
        self.assertEqual(
            trade["exit_reason"],
            "V12_THESIS_FAIL_MICROPRICE_REVERSAL_PERSISTENT",
        )
        self.assertEqual(trade["metadata"]["exit_trigger_source"], "microprice_edge_pct")
        self.assertEqual(
            trade["metadata"]["exit_trigger_rule"],
            "persistent_opposite_microprice_while_net_negative",
        )
        self.assertEqual(
            self.worker.state["exit_reason_counts"],
            {"V12_THESIS_FAIL_MICROPRICE_REVERSAL_PERSISTENT": 1},
        )
        self.assertNotIn("ABC", self.worker._positions)


if __name__ == "__main__":
    unittest.main()
