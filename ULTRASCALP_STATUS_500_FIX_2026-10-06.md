# UltraScalp status 500 / JSON hardening — 2026-10-06

Fixed the live-paper API/UI failure where the browser received Flask's HTML HTTP 500 page and reported `Server returned non-JSON`.

Changes:
- Strict JSON normalization now handles PostgreSQL Decimal/datetime, numpy scalars, bytes, sets, frozensets, deques, and unknown objects safely.
- Added `_scalper_json_response()` using explicit JSON serialization with `allow_nan=False`.
- Added a Scalper API-only 500 error handler that always returns JSON rather than Flask HTML.
- Scalper live settings/status/start/stop/trades/provider-test endpoints use the guaranteed JSON response path.
- Live UI no longer lets a trades-panel error prevent the main live-status/banner from rendering.
- UI keeps a last-known ARM intent locally and shows a clear status banner if the server is temporarily unavailable.
- Existing paper-only boundary remains unchanged; no broker order placement is added.

Validation: 17 targeted tests passed; Python compilation passed; archive integrity checked.
