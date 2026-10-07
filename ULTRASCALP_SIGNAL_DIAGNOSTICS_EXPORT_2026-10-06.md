# UltraScalp live-paper diagnostics and export fix — 2026-10-06

## What was fixed
- Live snapshots now retain the signal-gate diagnostics for each quote sample: raw direction, raw confidence, edge/score/L2 gate outcomes, and rejection reason.
- Live session heartbeat persists raw directional candidates, edge passes, score passes, L2 agreement passes, final signals, and entry rejections.
- The Scalper UI now shows those counters so `0 paper trades` can be distinguished from `0 candidates`.
- The Scalper page now exposes exports for today's/current-session five-level snapshots, paper trades CSV, and the latest session JSON.
- Snapshot export is streamed from PostgreSQL in batches and does not first load the entire dataset into application RAM.
- A bug in the cross-sectional fallback market-return collector was corrected: the previous LTP is read before overwriting the current LTP.

## Important diagnosis of the current session
The live paper scorer is intentionally strict. With the live defaults (0.1363% round-trip friction, 0.20% minimum remaining edge, 0.60% target, 0.22% protection), the current causal expected-move formula requires at least **0.3363%** expected gross movement before an event can pass the remaining-edge gate. That is a deliberate economic filter, not proof that the market had no movement.

The live collector is also **not the same mathematical scorer as the synthetic V8 environment**. V8 had richer synthetic order-flow/depth state (including latent flow and deeper book information). The live path can only use real L1/L5 displayed depth plus derived features and an OFI proxy. Therefore a zero-trade session must be diagnosed from the new gate counters rather than inferred from the snapshot count.

## Data stored per snapshot
- timestamp, ticker, scrip code, LTP, cumulative volume
- 5 bid prices and 5 bid quantities
- 5 ask prices and 5 ask quantities
- spread, L1/L5 imbalance, microprice, microprice edge
- OFI proxy, weighted book pressure, total displayed depth
- raw direction/confidence and final signal direction/side/confidence
- edge/score/L2 gate results and rejection reason
- expected move and remaining edge
- data-honesty/l2 mode markers

The stored data is **parsed five-level market-depth state**, not an invented 20/50/200-level order-event feed.
