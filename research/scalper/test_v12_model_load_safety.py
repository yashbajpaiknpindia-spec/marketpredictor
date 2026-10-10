"""Regression tests for V12 model-load safety in the live-paper worker."""
import unittest
from datetime import datetime
from unittest.mock import patch

from research.scalper.scalper_live_paper import LivePaperWorker


def make_worker():
    return LivePaperWorker(
        get_settings=lambda: {},
        is_trading_day=lambda *_: True,
        get_universe=lambda *_: [],
        resolve_scrip=lambda symbol: symbol,
        fetch_quotes=lambda *_: {},
        create_session=lambda *_: 1,
        update_session=lambda *_: None,
        persist_snapshot_batch=lambda *_: None,
        persist_trade=lambda *_: None,
        load_open_trades=lambda: [],
    )


def fake_score_event(*_args, **_kwargs):
    return {
        "direction": 1,
        "raw_direction": 1,
        "side": "LONG",
        "confidence": 90.0,
        "rejection_reason": None,
        "edge_pass": True,
        "score_pass": True,
        "l2_pass": True,
        "micro_signal": 0.01,
    }


class V12ModelLoadSafetyTests(unittest.TestCase):
    def test_enabled_but_missing_model_fails_closed_and_reports_reason(self):
        worker = make_worker()
        settings = {
            "v12_model_enabled": True,
            "v12_model_artifact_path": "/definitely/missing/v12-model.joblib",
        }
        with patch("research.scalper.scalper_live_paper.joblib.load", side_effect=FileNotFoundError("artifact missing")) as load, \
             patch("research.scalper.scalper_live_paper.score_event", side_effect=fake_score_event):
            result = worker._score({}, None, settings)
            self.assertEqual(result["direction"], 0)
            self.assertEqual(result["side"], "NONE")
            self.assertIn("model_unavailable", result["rejection_reason"])
            self.assertEqual(result["v12_model_status"], "UNAVAILABLE")
            self.assertEqual(worker.state["v12_model_load_status"], "UNAVAILABLE")
            self.assertIn("FileNotFoundError", worker.state["v12_model_load_error"])

            # A bad artifact must not cause a load exception for every symbol.
            worker._score({}, None, settings)
            self.assertEqual(load.call_count, 1)

    def test_open_positions_are_prechecked_from_fresh_bulk_quotes(self):
        worker = make_worker()
        worker._symbols = {"ABC": "123"}
        worker._positions = {"ABC": {"ticker": "ABC", "scrip_code": "123"}}
        worker._prev_depth = {}
        now = datetime(2026, 10, 10, 10, 0, 0)
        settings = {"protection_pct": 0.18}
        with patch.object(worker, "_manage_position") as manage:
            worker._precheck_open_positions({"123": {"live_price": 100.0}}, settings, now)
        manage.assert_called_once_with("ABC", "123", now, 100.0, {}, settings, None)
        self.assertEqual(worker.state["exit_precheck_count"], 1)
        self.assertGreaterEqual(worker.state["last_exit_precheck_ms"], 0.0)

    def test_snapshot_persists_entry_gate_rejection_reason(self):
        worker = make_worker()
        worker._record_snapshot(
            "ABC", "123", datetime(2026, 10, 10, 10, 0, 0), 100.0, 10.0,
            {
                "bids": [], "asks": [], "spread_pct": 0.01,
                "imbalance_l1": 0.0, "imbalance_l5": -0.2,
                "microprice": 100.0, "microprice_edge_pct": 0.01,
                "book_pressure": 0.0, "depth_total_qty": 100.0,
                "depth_valid": True, "depth_endpoint_ok": True,
            },
            {"direction": 0, "side": "NONE", "rejection_reason": "remaining_edge"},
            {"store_signal_snapshots_only": True},
            entry_reject_reason="entry_confirmation_pending",
            was_open_position=True,
        )
        saved = worker._snapshot_buffer[-1]
        self.assertEqual(
            saved["rejection_reason"],
            "remaining_edge;entry_gate:entry_confirmation_pending",
        )

    def test_loaded_model_is_reported_as_loaded(self):
        worker = make_worker()
        settings = {
            "v12_model_enabled": True,
            "v12_model_artifact_path": "/tmp/test-v12-model.joblib",
        }
        with patch("research.scalper.scalper_live_paper.joblib.load", return_value={"test": True}), \
             patch("research.scalper.scalper_live_paper.score_event", side_effect=fake_score_event):
            result = worker._score({}, None, settings)
        self.assertEqual(result["direction"], 1)
        self.assertEqual(result["v12_model_status"], "LOADED")
        self.assertEqual(worker.state["v12_model_load_status"], "LOADED")
        self.assertIsNone(worker.state["v12_model_load_error"])


if __name__ == "__main__":
    unittest.main()
