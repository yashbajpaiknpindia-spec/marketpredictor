# V12 three-session regression record — 2026-10-10

## Scope and integrity

- Branch: `research/v12-observability-safety-2026-10-10`
- This is a research-only change. No merge, deployment, live orders, production threshold changes, or database writes were made.
- The code changes in this record are **observability/attribution only**. They do not change entry selection, target/stop levels, cost assumptions, or the scoring policy, so they do not constitute a new profitability experiment.
- October 7–9 were already examined while developing V12. These are retrospective validation sessions, **not an untouched blind test**.
- The attempt to rerun the full snapshot-level SQL replay on 2026-10-10 timed out. Therefore the metrics below are the last completed fixed-rule screening replay, preserved as a comparison baseline—not a newly rerun result.

## Changes implemented

1. The persistent thesis-failure exit is now named `V12_THESIS_FAIL_MICROPRICE_REVERSAL_PERSISTENT`. The actual trigger uses the opposite sign of `microprice_edge_pct`, not direct L5 imbalance reversal.
2. Closed paper trades from this path include `metadata.exit_trigger_source=microprice_edge_pct` and `metadata.exit_trigger_rule=persistent_opposite_microprice_while_net_negative`.
3. Worker runtime state now accumulates `exit_reason_counts` for auditing exit-reason frequency.
4. CI now runs the model-runtime tests and the exit-attribution regression test.

## Code test results

GitHub Actions run [38035322594](https://github.com/yashbajpaiknpindia-spec/marketpredictor/actions/runs/38035322594) passed: **5 tests, 0 failures**.
- 4 model artifact/runtime-status tests
- 1 persistent microprice exit attribution/counter test

These tests establish behavior and attribution only; they do not establish trading profitability.

## Last completed fixed-rule retrospective screening replay

Policy: every 20th observation per ticker; persisted `raw_direction != 0`; gate spread <= 0.15%, L5 imbalance opposing direction, microprice edge confirming direction, and three-observation momentum confirmation. Look forward up to 20 observations; first +0.60% target or -0.18% stop wins, otherwise horizon exit. Deduct 0.15145 percentage points per trade (0.1363% round-trip costs + 0.01515% slippage reserve).

| Session | Gate-pass candidates | Target first | Stop first | Horizon exits | Avg gross % | Avg net % | PF estimate |
|---|---:|---:|---:|---:|---:|---:|---:|
| Oct 7 (session 14) | 244 | 11 | 18 | 215 | +0.0439 | -0.1076 | 0.2485 |
| Oct 8 (session 13) | 500 | 16 | 61 | 423 | +0.0061 | -0.1454 | 0.1640 |
| Oct 9 (session 1) | 672 | 15 | 113 | 544 | -0.0073 | -0.1588 | 0.1293 |
| Combined | 1,416 | 42 | 192 | 1,182 | — | -0.1453 weighted average | 0.1587 |

These are screening estimates, **not executable portfolio PFs**: candidate trades overlap, the replay does not model queue position or actual bid/ask depth fills, and persisted snapshots do not contain all inputs/predictions needed to reproduce the exact learned V12 model.

## Comparison with paper ledger

The previously audited live-paper ledger (not the fixed-rule replay) recorded:
- Oct 8: 80 trades, 4 net winners, paper P&L -₹30,842.95, PF about 0.0565.
- Oct 9: 189 trades, 3 net winners, paper P&L -₹65,560.65, PF about 0.0412.
- Combined: 269 trades, 7 net winners, paper P&L -₹96,403.60, PF about 0.0462.
- Oct 7 was imported snapshots and has no native paper-trade ledger.

Paper-ledger amounts are simulated paper P&L, not brokerage losses.

## Result and next decision

- **No demonstrated PF improvement yet.** This code change improves audit correctness and observability, not trade selection.
- The fixed-rule screen remains negative after costs on all three sessions.
- Do not promote this policy based on these results.
- Next profitability work must isolate one entry/exit hypothesis at a time, use candidate-level features/decision traces, and then freeze code/config/model hashes before testing on an untouched later session. The test should also enforce non-overlapping positions, data coverage/cadence checks, and realistic execution costs.
