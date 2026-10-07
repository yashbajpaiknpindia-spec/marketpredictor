# UltraScalp synthetic L5 edge lab — 2026-10-07

## What changed
- Added a controlled five-level synthetic order-book generator with noise, false imbalance, liquidity shocks, replenishment/cancellation behavior, open/midday/close regimes, reversal and false-breakout conditions.
- The hidden latent-flow state is used only by the market generator. It is **not** a feature available to the scanner.
- Added a causal train/test split by complete sessions. The model is fitted only on the training sessions and frozen before untouched test sessions.
- Added a small ridge edge model using only visible L5/LTP-like fields.
- Entry is based on predicted gross opportunity minus round-trip friction and spread. There is no fixed +0.60% target requirement.
- Exits can occur when the modelled edge disappears, protection is hit, or maximum hold is reached.
- Added a background synthetic scan with progress/status APIs.
- The Scalper page top four statistics now follow whichever scan is actually active. Synthetic and real evidence are never added together.
- Added regression tests for the documented L5 payload parser and synthetic hidden-state isolation.
- Real live collector remains paper-only and retains the real L5 capture/replay path.

## Evidence boundary
Synthetic output is a laboratory result only. It must never be presented as NSE historical performance. A negative result is still useful because it exposes weak signal/exit logic; a positive result requires validation on held-out TRUE_L5 live sessions.

## Runtime verification
- Python compile checks passed for app and Scalper research modules.
- JavaScript syntax check passed with Node.
- 3 targeted unit tests passed.
- Synthetic scan was executed end-to-end after fixing its progress callback; it completed without an exception.
- Full Flask boot was not executed in this build container because Flask is not installed there; therefore no claim of full runtime deployment verification is made here.
