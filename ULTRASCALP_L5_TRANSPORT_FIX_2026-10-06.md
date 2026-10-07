# UltraScalp L5 Transport Fix — 2026-10-06

## Root cause
The live worker fetched `/market/quotes/full` but did not call the dedicated INDstocks `/market/quotes/mkt` market-depth endpoint. The parser therefore saw empty `market_depth.depth` for the production feed and persisted zero L1/L5 fields.

## Fix
- `/market/quotes/full` remains the source for LTP/volume.
- `/market/quotes/mkt` is now fetched explicitly for every live batch.
- The dedicated `market_depth` payload is merged into the full-quote row by `scrip_code`.
- A depth-validity gate blocks paper entries when best bid/ask and displayed L5 depth are missing.
- Live status now exposes valid-depth coverage and effective REST polling cadence.
- Provider test now reports the number of actually valid L5 levels returned by the dedicated endpoint.
- REST L5 polling has a hard 1,000 ms minimum to remain within the provider's documented 5 quote-requests/second limit.

## Validation
- Python compilation: PASS
- Targeted Scalper tests: 21/21 PASS
- ZIP integrity: PASS
- Synthetic provider-response parsing test data matches the documented `/market/quotes/mkt` depth structure.

## Scope
Paper-only. No order-placement endpoint was added or enabled.
