# UltraScalp L5 transport hardening — 2026-10-07

The 2026-10-07 snapshot export showed 100% zero L1/L5 depth while LTP/volume continued to update. The existing transport already called the documented `/market/quotes/mkt` endpoint, so this release hardens the response adapter instead of merely changing the endpoint again.

Changes:
- Normalize REST quote response keys across `NSE_123`, `NSE:123`, and bare token variants.
- Treat dedicated `/market/quotes/mkt` depth as authoritative when it contains a non-empty `depth` list.
- Use documented `/market/quotes/full` embedded `market_depth` only as a fallback when it contains actual levels.
- Add `/api/scalper/live/test-provider` diagnostics that show HTTP status, matched response key, market-depth presence, depth-level count, first level, and row keys without exposing credentials.
- Emit compact per-batch counters: requested, full rows, depth rows, valid L5 rows.
- Paper-entry L5 gate remains fail-closed; no depth means no paper trade.

The official INDstocks documentation currently defines `/market/quotes/mkt` as the 5-level market-depth endpoint and shows the response shape `data[SEGMENT_TOKEN].market_depth.depth`.
