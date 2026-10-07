import unittest
from research.scalper.synthetic_l5_lab import SyntheticConfig, generate_session, run_experiment, FEATURES, REGIMES, EVIDENCE_CLASS
from research.scalper.scalper_live_paper import extract_depth_levels
import numpy as np


class UltraScalpSyntheticTests(unittest.TestCase):
    def test_documented_l5_shape_parses_five_levels(self):
        q = {'market_depth': {'depth': [
            {'buy': {'quantity': '10', 'price': '100'}, 'sell': {'quantity': '12', 'price': '100.1'}},
            {'buy': {'quantity': '20', 'price': '99.9'}, 'sell': {'quantity': '22', 'price': '100.2'}},
            {'buy': {'quantity': '30', 'price': '99.8'}, 'sell': {'quantity': '32', 'price': '100.3'}},
            {'buy': {'quantity': '40', 'price': '99.7'}, 'sell': {'quantity': '42', 'price': '100.4'}},
            {'buy': {'quantity': '50', 'price': '99.6'}, 'sell': {'quantity': '52', 'price': '100.5'}},
        ]}}
        d = extract_depth_levels(q)
        self.assertEqual(d['valid_levels'], 5)

    def test_hidden_state_is_not_a_model_feature(self):
        self.assertNotIn('hidden_state', FEATURES)
        self.assertNotIn('hidden_true_flow', FEATURES)
        self.assertNotIn('hidden_edge_strength', FEATURES)
        df = generate_session(np.random.default_rng(7), 'bull_trend', 1, SyntheticConfig(bars_per_session=40))
        self.assertIn('hidden_state', df.columns)
        self.assertIn('hidden_true_flow', df.columns)
        self.assertNotIn('hidden_state', FEATURES)

    def test_every_regime_is_represented_in_all_session_splits(self):
        r = run_experiment(SyntheticConfig(sessions_per_regime=4, bars_per_session=120, seed=11), None)
        self.assertEqual(r['regime_count'], len(REGIMES))
        # 60/20/20 of four sessions => 2 train, 1 validation, 1 blind test per regime.
        self.assertEqual(r['train_sessions'], 2 * len(REGIMES))
        self.assertEqual(r['validation_sessions'], len(REGIMES))
        self.assertEqual(r['test_sessions'], len(REGIMES))
        self.assertEqual(set(x['regime'] for x in r['regime_test_metrics']), set(REGIMES))

    def test_train_validation_blind_test_are_separate_and_target_free(self):
        r = run_experiment(SyntheticConfig(sessions_per_regime=3, bars_per_session=120, seed=17), None)
        self.assertEqual(r['evidence_class'], EVIDENCE_CLASS)
        self.assertGreater(r['train_sessions'], 0)
        self.assertGreater(r['validation_sessions'], 0)
        self.assertGreater(r['test_sessions'], 0)
        self.assertTrue(r['blind_test'])
        self.assertTrue(r['hidden_state_excluded'])
        self.assertFalse(r['target_dependency'])
        self.assertIn('thresholds_frozen_after_validation', r)
        self.assertIn('latency_robustness', r)
        self.assertEqual([x['latency_bars'] for x in r['latency_robustness']], [0, 1, 2, 3])
        self.assertIn('no_edge', {x['regime'] for x in r['regime_test_metrics']})


if __name__ == '__main__':
    unittest.main()
