# UltraScalp L5 root-cause audit — 2026-10-07

## What the deployed UI proves

The captured provider diagnostics showed two different conditions:

- RELIANCE probe: HTTP 429 while token/auth was blocked.
- HDFCBANK / ICICIBANK probes: HTTP 200, matched the expected `NSE_<security_id>` response key, `market_depth` existed, but the diagnostic reported zero extracted levels.

Therefore this was not safe to diagnose as a simple instrument-key mismatch.

## What the application audit found

1. The live worker and the provider diagnostic used two different depth parsers. That could make the UI and worker disagree about the same raw provider payload.
2. The parser assumed the documented casing/nesting more strongly than necessary. INDstocks documents `data[SEGMENT_TOKEN].market_depth.depth[]`, but the adapter did not recursively inspect arbitrary nested/case variants.
3. `Test INDstocks now` first ran the normal quote path and then separately called the market-depth diagnostic once per tested symbol. With three symbols that could create multiple quote requests during the worker's own polling cycle. INDstocks documents Quote APIs at 5 requests/second and token generation at 1/minute.
4. The provider diagnostic did not expose where the depth container was found or whether the provider's `depth` container itself was empty. This made a provider-empty response look similar to an application-parser failure.

## Changes in this release

- Harden `extract_depth_levels()` to search nested JSON recursively (bounded depth), normalize key casing, accept side aliases and keyed/flat level layouts, and still require real positive bid/ask price AND quantity.
- Harden the direct provider diagnostic to report `market_depth_keys`, `depth_key`, `depth_container_type`, `depth_container_count`, `depth_path`, and `valid_levels`, plus a safe market-data-only row excerpt.
- Make `Test INDstocks now` a rate-safe single-symbol probe: one `/market/quotes/full` request plus one direct `/market/quotes/mkt` request. It no longer starts the normal batch quote path and then repeats `/market/quotes/mkt` N times.
- Keep the paper-entry L5 fail-closed gate unchanged.

## External API contract checked

INDstocks documents:

- `GET /market/quotes/full`
- `GET /market/quotes/ltp`
- `GET /market/quotes/mkt`
- `scrip-codes` in `SEGMENT_INSTRUMENTTOKEN` format, e.g. `NSE_3045`
- `market/quotes/mkt` returns `data[NSE_<id>].market_depth.depth` with five buy/sell levels.

No endpoint was changed in this release.
