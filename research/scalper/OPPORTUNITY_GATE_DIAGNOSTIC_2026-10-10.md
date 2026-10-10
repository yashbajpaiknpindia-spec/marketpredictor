# UltraScalp opportunity-gate diagnostic — 2026-10-10

## Purpose

Compare the current entry filter with two faster/looser variants on the three captured NSE sessions. This is a diagnostic for where candidate filtering loses directionally aligned large moves; it is **not** a profit-factor backtest and does not establish a tradable edge.

## Method

- Read the existing `scalper_live_snapshots` rows for Oct 7 (session 14), Oct 8 (session 13), and Oct 9 (session 1).
- Within each ticker, order by `captured_at, id`; sample every fifth observation.
- Label a sampled row as a target window if the next 20 observations include at least a 0.60% up or down LTP excursion. If both directions reach 0.60%, label the direction of the larger excursion.
- `aligned_target_rows` counts sampled candidate windows whose selected direction matches that future label. Windows overlap, so neither candidate nor target-window counts are unique opportunities or trade counts.
- The label uses LTP only; it does not model bid/ask fills, stop-before-target ordering, slippage, round-trip costs, capital overlap, cooldowns, or a true independent exit path.

## Results

| Session | Rule | Candidate windows | Direction-aligned target windows | Candidate windows with any target move | All target windows |
|---|---|---:|---:|---:|---:|
| Oct 7 | Current gate proxy | 117 | 24 | 25 | 808 |
| Oct 7 | Faster signal, no L5-opposition requirement | 138 | 18 | 20 | 808 |
| Oct 7 | Raw direction + tight spread + faster confirmation | 1,652 | 34 | 42 | 808 |
| Oct 7 | Final signal + tight spread + faster confirmation | 121 | 18 | 20 | 808 |
| Oct 8 | Current gate proxy | 109 | 28 | 33 | 1,297 |
| Oct 8 | Faster signal, no L5-opposition requirement | 132 | 25 | 50 | 1,297 |
| Oct 8 | Raw direction + tight spread + faster confirmation | 3,447 | 56 | 106 | 1,297 |
| Oct 8 | Final signal + tight spread + faster confirmation | 122 | 25 | 50 | 1,297 |
| Oct 9 | Current gate proxy | 329 | 12 | 18 | 2,693 |
| Oct 9 | Faster signal, no L5-opposition requirement | 534 | 14 | 22 | 2,693 |
| Oct 9 | Raw direction + tight spread + faster confirmation | 4,974 | 76 | 114 | 2,693 |
| Oct 9 | Final signal + tight spread + faster confirmation | 360 | 12 | 14 | 2,693 |

### Rule definitions

- **Current gate proxy:** stored final direction is nonzero; confidence at least 65; spread at most 0.15%; L5 imbalance opposes the direction; microprice edge confirms the direction; and price is moving in that direction over the prior three observations.
- **Faster signal, no L5 opposition:** stored final direction and confidence gate; spread at most 0.15%; microprice confirms; one-observation price change confirms. It removes the L5-opposition requirement and shortens momentum confirmation.
- **Raw direction + tight spread + faster confirmation:** raw direction is nonzero; spread at most 0.05%; microprice and one-observation price change confirm. This intentionally loose diagnostic omits the final score/edge gate.
- **Final signal + tight spread + faster confirmation:** same faster confirmation, but keeps the stored final signal and confidence threshold, with spread at most 0.05%.

## Interpretation

1. The current gate proxy behaved very differently by session: 24/117 aligned target windows on Oct 7, 28/109 on Oct 8, but only 12/329 on Oct 9. This is evidence of strong regime sensitivity, not a stable edge.
2. Removing L5 opposition and shortening confirmation did not materially recover direction-aligned target windows. It increased candidate volume on Oct 9 from 329 to 534 but only raised aligned windows from 12 to 14.
3. The raw-direction rule found more large moves, but produced thousands of candidate windows per session. It is too permissive to call an improvement and may admit many losing trades.
4. These results do not justify loosening the live entry gates. The next test must score first-touch outcomes after costs, control overlapping positions, and freeze the policy before the next unseen session.

## Current engineering actions

- V12 model-load failures are now explicit and fail closed in the draft branch; no silent heuristic fallback is allowed while the learned model is configured as enabled.
- Open-position stop/target/time checks are prioritized immediately after the bulk quote response. This reduces post-response processing delay but does not solve slow quote delivery.
- Per-snapshot entry-gate rejection reasons are being persisted in the existing rejection-reason field, and aggregate reason counts are exposed in worker state.
- No production merge or deployment was performed.
