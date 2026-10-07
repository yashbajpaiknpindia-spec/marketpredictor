# UltraScalp L5 Replay + Real-Edge Research Fix — 2026-10-07

## Why this change exists
Historical NSE data available to the project is OHLCV, while the production scalper can observe real 5-level displayed depth only while live. It is therefore invalid to present an invented 50-level order book as if it were historical truth.

## Evidence classes
- `TRUE_L5_REPLAY`: uses recorded live L5/LTP snapshots from a completed live paper session. This is the exact real microstructure evidence class.
- `EMPIRICAL_L5_PROXY`: reuses real observed L5 feature states/blocks from live sessions and aligns them to historical OHLCV. This is a calibrated proxy, not historical L5 truth, and must never be combined with TRUE_L5_REPLAY performance numbers.

## Current live-data findings from the latest capture supplied on 2026-10-07
- 100,261 L5 snapshots.
- 221 instruments represented.
- 99.4235% of rows passed the L5 validity gate.
- 179 paper trades, 157 closed and 22 still open in the supplied export.
- The paper process reached 79 concurrent positions because `max_open_positions=0` previously meant unlimited. This was not capital realistic for a ₹2,00,000 research budget.
- The observed batch cadence was materially slower than the requested 1 second once the universe expanded to 221 names. The later 221-stock portion had a median batch-to-batch gap of about 6.19 seconds. This is a provider/REST throughput constraint, not a claim that the configured interval was achieved.
- The recorded expected-move heuristic materially overestimated realized short-horizon movement in this sample.

## Fixes
1. Added an exact `TRUE_L5_REPLAY` endpoint and UI panel for completed captured sessions.
2. Added a real-L5 calibration endpoint that measures forward gross/net behavior of observed L5 features without feeding the live trader.
3. Added an explicit `EMPIRICAL_L5_PROXY` evidence contract so proxy backtests can never be mislabeled as historical L5 truth.
4. Added capital-safe concurrency fallback: when max open positions is 0, the paper worker derives a cap from the ₹2,00,000 budget and configured position notional (default => 1 concurrent position).
5. Poll scheduling now compensates for provider/processing time rather than adding a full sleep interval after a slow REST cycle. The UI still reports the actual effective cadence honestly.
6. No changes were made to the live alpha formula or to provider endpoint selection solely to improve today's performance. The real-data calibration must come before promoting a new alpha rule.

## What backtesting should mean going forward
- Use TRUE_L5_REPLAY for any session that was actually captured live.
- Use EMPIRICAL_L5_PROXY only for hypothesis generation over older OHLCV periods.
- Once enough TRUE_L5 sessions accumulate, train an empirical conditional-net-edge model using prior completed sessions and validate it walk-forward on later TRUE_L5 sessions. The live engine should consume that model only after a cold-start minimum is met.
- Do not use the old 0.4%/0.6% target as an assumption about every stock. The intended decision variable is conditional expected net edge after friction, with exits that respond to realized edge deterioration.
