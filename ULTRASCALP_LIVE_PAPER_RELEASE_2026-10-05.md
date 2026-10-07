# UltraScalp Live Paper Capture Release — 2026-10-05

## Added

- Real-market, paper-only UltraScalp worker.
- Automatic next-session arming from persisted database settings.
- 09:00 IST pre-open data capture; 09:15 IST paper-entry start; 15:25 IST new-entry cutoff and safety close.
- INDstocks `/market/quotes/full` batched capture with documented 5-level market depth + LTP + cumulative volume.
- L1/L5 imbalance, spread, microprice, microprice edge, displayed-depth pressure and OFI proxy derivation.
- Live NIFTY-50 LTP market context when the provider accepts the current index instrument code; cross-sectional return proxy is the fallback.
- Full parsed 5-level depth persistence into PostgreSQL for later research/replay.
- Paper trade ledger with idempotent open->closed updates.
- Provider connectivity test in the Scalper Engine UI.
- Data capability/glossary table explicitly distinguishing L1, L5, L10, L20 and L200.
- Recovery of open paper trades after an app restart on the same session.

## Frozen paper rules

- Target: 0.60%
- Protection: 0.22%
- Maximum hold: 10 minutes
- Round-trip friction: 0.1363%
- Additional tested entry slippage: 0.0% (to match the frozen synthetic V8 economics)
- Minimum remaining edge: 0.20%
- Minimum signal score: 65
- Entry lag: 0 bars
- Symmetric long/short logic
- Market confirmation: required
- Relative strength: required
- L2 confirmation: required when displayed depth is available
- Max open positions: 0 (unlimited research mode)
- Paper notional: ₹2,00,000 for P&L translation only; it is NOT a capital constraint.

## Safety

No order placement function is called. This release is a real-data capture and paper-simulation release only.

## Important provider boundary

Current INDstocks public documentation confirms 5-level market depth and a live quote WebSocket. It does not document 20-level/200-level depth or event-level add/cancel/execution order flow. Accordingly, the live engine labels its order-flow field `OFI_PROXY` and does not claim synthetic L50/event-stream parity.
