# MarketPredictor UltraScalp — Synthetic L5 Edge Lab v2

The application now has a proper synthetic market laboratory before scarce real-L5 sessions are used for promotion decisions.

### What is now built
- 25 market regimes, including an explicit `no_edge` environment.
- Dynamic five-level displayed order book with cancellations, replenishment, aggressive queue depletion, variable spread/depth and deceptive imbalance events.
- Per-session randomized liquidity/volatility/flow characteristics.
- Visible-only causal feature set; hidden latent flow and future returns are generator-only.
- Train → validation → frozen blind test split inside every regime.
- Validation-only tuning of entry gates.
- Point-in-time execution with configurable 0–3 bar latency, spread, slippage, adverse selection and round-trip friction.
- Edge-based entry/exit; no assumed fixed +0.60% target.
- Blind-test regime table, no-edge participation check and latency robustness panel in the Scalper UI.
- The Scalper top four metrics continue to follow whichever scan is actually active; synthetic and real L5 evidence are not merged.

### Verification
38 targeted UltraScalp/scalper regression tests pass. The full pytest suite cannot be collected in the local build container because `psycopg2` and `ijson` are not installed there; they are pinned in the deployable `requirements.txt`. Flask production boot was also not run locally because Flask is absent from the build container.

A default synthetic smoke run completed end-to-end. Its output is stored as a verification artifact and remains explicitly classified as synthetic laboratory evidence, not NSE performance evidence.
