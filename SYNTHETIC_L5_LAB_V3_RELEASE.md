# UltraScalp Synthetic L5 Causal Market Lab v3

## Phase 1 — market generator
- 30 regimes, including explicit `no_edge` and `dead_zone` environments.
- Five displayed bid/ask levels with dynamic cancellation, replenishment and queue depletion.
- Deceptive/spoof-like displayed liquidity, false breakouts, order-flow flips, shocks and recoveries.
- Cross-market observable context (NIFTY-like return/volatility and VIX-like state) without exposing hidden generator state.
- Stock-specific streams plus randomized liquidity/volatility/flow conditions.
- Hidden latent flow, true regime and future return are evaluator-only fields.

## Phase 2 — evaluation harness
- Train, validation and completely frozen blind-test sessions remain separated.
- Entry thresholds are selected only on validation.
- Execution uses latency, spread, slippage, adverse selection and round-trip friction.
- Entry is based on positive remaining net edge; there is no required +0.60% target.
- Exits can occur when the modeled edge disappears, protection is hit, or the time budget expires.
- Regime-level and latency robustness output remains separate from real L5 evidence.
- The application never mixes synthetic results with TRUE_L5/live-paper performance.

## Important
This build performs **coding only**. No v3 synthetic simulation was executed while building this release. The user runs the lab from the Scalper page.
