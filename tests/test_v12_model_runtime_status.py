"""Unit tests for explicit V12 runtime model status (no DB or market calls)."""
from __future__ import annotations

import os
import tempfile
import threading
import unittest
from datetime import datetime
from unittest.mock import patch

from research.scalper.scalper_live_paper import LivePaperWorker


class V12ModelRuntimeStatusTests(unittest.TestCase):
    def setUp(self):
        self.worker = LivePaperWorker.__new__(LivePaperWorker)
        self.worker.lock = threading.RLock()
        self.worker.state = {}

    def settings(self, path, enabled=True):
        return {
            "v12_model_enabled": enabled,
            "v12_model_artifact_path": path,
            "v12_mode": True,
            "target_pct": 0.60,
            "protection_pct": 0.18,
            "max_hold_minutes": 10,
            "round_trip_cost_pct": 0.1363,
            "entry_slippage_pct": 0.015,
            "min_remaining_edge_pct": 0.005,
            "max_entry_lag_bars": 0,
            "freshness_decay_per_bar": 0.15,
            "min_signal_score": 65.0,
            "v12_require_calibration": False,
            "v12_research_probability_candidate": 0.10,
            "v12_max_adverse_probability": 0.80,
            "v12_min_model_expected_edge_pct": 0.0,
            "v12_min_model_target_probability": 0.70,
            "v12_entry_latency_bars": 1,
        }

    @patch("research.scalper.scalper_live_paper.score_event", return_value={})
    def test_missing_artifact_is_visible_as_heuristic_fallback(self, _score):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "missing.joblib")
            result = self.worker._score({}, datetime.now(), self.settings(path))
        self.assertEqual(result["v12_model_status"], "fallback_heuristic")
        self.assertFalse(result["v12_model_active"])
        self.assertIn("FileNotFoundError", result["v12_model_load_error"])
        self.assertEqual(self.worker.state["v12_model_status"], "fallback_heuristic")

    @patch("research.scalper.scalper_live_paper.score_event", return_value={})
    def test_load_exception_is_visible_and_not_silently_swallowed(self, _score):
        with tempfile.NamedTemporaryFile() as artifact:
            artifact.write(b"not-a-real-joblib-file")
            artifact.flush()
            with patch("research.scalper.scalper_live_paper.joblib.load", side_effect=RuntimeError("bad artifact")):
                result = self.worker._score({}, datetime.now(), self.settings(artifact.name))
        self.assertEqual(result["v12_model_status"], "fallback_heuristic")
        self.assertIn("RuntimeError: bad artifact", result["v12_model_load_error"])

    @patch("research.scalper.scalper_live_paper.score_event", return_value={})
    def test_disabled_model_is_reported_as_disabled(self, _score):
        result = self.worker._score({}, datetime.now(), self.settings("unused.joblib", enabled=False))
        self.assertEqual(result["v12_model_status"], "disabled")
        self.assertFalse(result["v12_model_active"])
        self.assertIsNone(result["v12_model_load_error"])

    @patch("research.scalper.scalper_live_paper.score_event", return_value={})
    @patch("research.scalper.scalper_live_paper.joblib.load", return_value={"test": "profile"})
    def test_loaded_model_reports_sha256_fingerprint(self, _load, _score):
        with tempfile.NamedTemporaryFile() as artifact:
            artifact.write(b"frozen-test-artifact")
            artifact.flush()
            result = self.worker._score({}, datetime.now(), self.settings(artifact.name))
        self.assertEqual(result["v12_model_status"], "loaded")
        self.assertTrue(result["v12_model_active"])
        self.assertEqual(len(result["v12_model_artifact_sha256"]), 64)
        self.assertIsNone(result["v12_model_load_error"])


if __name__ == "__main__":
    unittest.main()
