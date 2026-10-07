# UltraScalp live status/schema self-heal fix — 2026-10-06

## Root cause
`/api/scalper/live/status` queried `scalper_live_sessions` directly. The main background schema bootstrap had not yet created the UltraScalp tables in the deployed database, so the status endpoint returned HTTP 500 even though PostgreSQL itself was ready.

## Fix
- Added `_ensure_scalper_live_schema(conn)` that idempotently creates:
  - `scalper_live_settings`
  - `scalper_live_sessions`
  - `scalper_live_snapshots`
  - `scalper_paper_trades`
  - required indexes
- Settings, status, and session creation use the same self-healing schema path.
- Live status returns JSON and remains HTTP 200 when the scalper data-store hydration has a transient problem; it exposes `scalper_schema_ready` and `data_store_error` instead of hiding the problem behind an HTML/500 page.
- Preserves existing settings; no destructive migrations or deletes.
- Fixed the `updated_at DEFAULT NOW()` DDL syntax in the self-healing migration.

## Validation
- Python compile: passed
- Static regression tests: 11 passed
- Schema DDL smoke test with a fake PostgreSQL connection: passed; all 4 tables and indexes were emitted and committed
- ZIP integrity: verified after packaging
