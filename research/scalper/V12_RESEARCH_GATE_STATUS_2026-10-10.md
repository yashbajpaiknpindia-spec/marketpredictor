# UltraScalp V12 research gate status — 2026-10-10

## Status

Research-only branch: `research/v12-fail-closed-quote-gate-2026-10-10`.

This work is not merged to `main` and has not been deployed to Render. Nothing in this document is a claim of profitable performance. The October 7–9 data has already been examined and cannot serve as a new untouched blind holdout.

## Confirmed historical results

- The October 7–8 candidate-scoring PF values 2.3432 and 3.7515 reproduce, but the frozen gate was discovered from those same sessions and many scored entries/outcomes occur after the live 15:25 cutoff. They are not blind-test portfolio PFs.
- The forward October 9 paper ledger for the live gate/exits recorded 189 closed trades, 3 winners, net P&L -₹65,560.65, PF 0.0412. This is a failed forward session, not evidence of a positive edge.
- The October 9 fixed-barrier diagnostic also failed (158 mature candidates, PF 0.2043), but it is candidate-level rather than an executable capital-constrained portfolio.
- Existing September 25 development ledgers are also negative:
  - `real_scalp_A_strict_25sep_trades.csv`: 2,648 trades, net sum -3.4870 percentage points, PF 0.1782.
  - `real_scalp_B_balanced_25sep_trades.csv`: 4,957 trades, net sum -6.4962 percentage points, PF 0.1590.
  - `real_scalp_improved_B_25sep_trades.csv`: 4,045 trades, net sum -5.7107 percentage points, PF 0.1350.
  These are historical development artifacts, not blind evidence.

## Entry-family development comparison (October 7–8)

These are exploratory, previously inspected development sessions, not blind-test evidence. The fixed-exit, cost-adjusted, capital-constrained replays were all negative:

| Entry family | Trades | Winners | PF | Approx. net P&L at ₹2 lakh notional |
|---|---:|---:|---:|---:|
| L5-aligned longer momentum | 22 | 7 | 0.4969 | -₹6,725 |
| Market-breadth continuation | 21 | 7 | 0.4816 | -₹5,862 |
| Trend pullback | 17 | 5 | 0.4045 | -₹4,952 |
| Book-led reversal | 15 | 4 | 0.2685 | -₹7,384 |
| L5 absorption, short lookback | 18 | 4 | 0.2195 | -₹10,903 |
| Book continuation, short lookback | 26 | 4 | 0.1264 | -₹14,234 |
| Momentum + microprice confirmation | 18 | 1 | 0.0716 | -₹11,716 |

The best result is only the least-bad candidate, not an edge. It is not acceptable to tune more thresholds on these same sessions and then claim a blind success.

## Exit-policy experiment (same used development sessions)

For L5-aligned longer momentum, a one-position-at-a-time replay compared fixed target/stop with immediate trailing at two distances. It used the conservative spread/cost overlay and stopped new entries at 15:20 with force-close by 15:25.

| Exit policy | Trades | Wins | PF | Net percentage points |
|---|---:|---:|---:|---:|
| Fixed target/stop | 22 | 7 | 0.4969 | -3.363 |
| Immediate trail, 0.30% floor | 25 | 6 | 0.3046 | -4.804 |
| Immediate trail, 0.075% floor | 38 | 4 | 0.0939 | -9.091 |

These are development-session results, not blind evidence. Both immediate-trail variants made the result worse, so immediate arming is not the default. The live trail arm remains configurable at the stored 0.075% net threshold; immediate arming can be tested separately, but it has not earned promotion.

## Code and data-integrity changes on this branch

1. The entry gate fails closed for non-positive/invalid spread, missing or crossed L1 quotes, incomplete five-level depth, and non-monotonic bid/ask levels.
2. Quote routing and the L5 health counter use the same full-book integrity check, so a malformed five-level ladder is not reported as valid merely because five rows exist.
3. The blind audit requires per-symbol snapshot-gap p95 no greater than 5 seconds. The October 9 measured p95 was about 10.3 seconds, so that session fails this freshness requirement even before profitability is considered.
4. Capture telemetry marks a book invalid when the five-level integrity check fails, rather than reporting top-of-book-only validity as full L5 validity.
5. The existing v1 calibration artifact is not policy-safe: it records a 0.22% protection level and no target-threshold metadata, while its fitting defaults imply a 0.60% gross target. The associated report describes a 0.1413% gross target. Those are different labels; the artifact cannot be used as proof for the positive-net-floor rule.
6. Calibration profiles are versioned as `v12-calibration-v2-first-barrier`. A profile is rejected if its horizon, costs, slippage, target, stop, or exit-policy label does not match the active policy. The old v1 profile is intentionally incompatible.
7. Calibration labels mean “target barrier reached before the protection barrier within the fixed horizon,” not “probability of net profit.” Labels are isolated by ticker/session/date, require a horizon observation within a bounded tolerance, and use explicit nanosecond timestamp arithmetic.
8. The current fitter refuses to label fixed-barrier outcomes as the live profit-lock/L5-flip policy. V12 calibration is mandatory regardless of a stale database toggle; without a compatible frozen profile, the experimental V12 entry path fails closed.
9. The configured `v12_direction_edge_production.joblib` artifact is not in the repository's `research/scalper/artifacts/` listing. It may have been installed separately on Render, but that is unverified. Previously, load errors were swallowed and the heuristic scorer could continue; the research branch now reports model status and rejects V12 entries when the configured model cannot load.
10. The worker now records source/model/config fingerprints in the session manifest and paper-trade metadata, freezes effective policy settings for the session, and blocks entries if the active model/config fingerprint changes. The trade upsert now updates final metadata on close; previously, close-time metadata could be lost on conflict.
11. `v12_blind_portfolio_audit.py` audits actual closed paper trades only. It rejects incomplete session coverage, overlapping positions, post-cutoff entries/exits, duplicate snapshots/trades, missing manifests, and mismatched source/model/config hashes. A manifest builder and empty template are provided; no current candidate has a valid manifest because the live model/profile is not yet verified.
12. Two routines named `blind_test_...` were not valid blind portfolio tests. They split rows instead of unique timestamps, simulated per-symbol positions without shared capital, did not consistently isolate ticker/session/date, and used exit approximations that differed from the live worker. The calibrated routine also mislabeled target-first probability as net-profit probability. On this branch, both return `blind_test=false`, state their limitations, split on unique timestamps, and isolate paths by ticker/session/date.
13. The historical calibration reports were reclassified as diagnostics, not blind portfolio proof. Their historical metrics were preserved but are explicitly barred from strategy promotion.
14. A focused GitHub Actions workflow runs V12 calibration, model, quote-integrity, depth-routing, replay, and blind-portfolio-audit regression tests. The latest completed code-bearing run passed **49 tests in 4.05 seconds**: https://github.com/yashbajpaiknpindia-spec/marketpredictor/actions/runs/37988011245. These tests validate code paths and audit integrity, not trading profitability.

## Research/holdout rules

- October 7–9 and September 21–25 artifacts are development/diagnostic data only.
- No parameter selection on a held-out session.
- Freeze source commit, model/profile hash, data manifest, cost assumptions, target/stop, time cutoffs, max concurrent capital, and execution-fill model before the holdout starts.
- A valid PF must come from a single capital-constrained, non-overlapping trade ledger, with spread/fees/slippage and delayed-observation execution included.
- The result must include all trades, wins/losses, net P&L, drawdown, PF, hold time, reject counts, and data freshness.
- PF > 2 is not accepted as proof by itself: require a meaningful sample and stability across more than one untouched session. A failed holdout is recorded as a failure and cannot be reused to tune the same candidate.

## Current conclusion

No tested real-data strategy has demonstrated a positive edge suitable for promotion. The strongest recent development experiment remained below PF 0.5. The next untouched market session is required to test a candidate frozen before that session; do not present the old sessions as a substitute.
