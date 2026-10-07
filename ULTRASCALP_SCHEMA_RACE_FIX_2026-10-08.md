# UltraScalp schema readiness race fix — 2026-10-08

## Problem
An existing MarketPredictor PostgreSQL database could be marked globally READY while the additive UltraScalp live-paper schema was still being created in a background thread. Session History could therefore run during that window and report:

`Scalper database schema is not ready; request-path DDL is disabled.`

## Fix
- Existing production databases now run the additive compatibility migration synchronously under the existing PostgreSQL advisory migration lock.
- `DB_READY` is set only after the Scalper schema contract is verified.
- Request paths remain DDL-free.
- No existing rows are rewritten, deleted, truncated, or backfilled.
- New L5 snapshots continue using `zlib-json-v1` compressed `BYTEA`; legacy JSONB ladder rows remain untouched.

## Validation
- `app.py` compiles successfully.
- 21 focused Scalper/schema/storage/V12 tests pass.
