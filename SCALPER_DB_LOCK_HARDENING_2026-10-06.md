# UltraScalp DB lock hardening — 2026-10-06

## Root cause confirmed from PostgreSQL logs

The production log showed `INSERT INTO scalper_live_settings ...` waiting **916 seconds** for a `RowExclusiveLock`, while another process was requesting/holding an `AccessExclusiveLock` on the same relation. A normal `SELECT * FROM scalper_live_settings WHERE id=1` also waited almost 5 seconds behind that lock. The resulting client resets/EOFs were downstream symptoms of requests and transactions being held open behind schema DDL.

## Changes in this build

1. UltraScalp request/worker paths are now **read-only for schema checks**. `_ensure_scalper_live_schema()` never runs `CREATE TABLE`, `ALTER TABLE`, or advisory-lock migration work during a request.
2. Missing Scalper schema now fails fast as `schema_not_ready`/database-unavailable rather than turning a web request into a blocking migration.
3. Reading Scalper settings no longer performs a write when the singleton row is absent.
4. The shared PostgreSQL request pool sets:
   - `lock_timeout = 5000ms`
   - `statement_timeout = 120000ms`
   - `idle_in_transaction_session_timeout = 60000ms`
   This prevents ordinary web requests from sitting behind a PostgreSQL table lock indefinitely.
5. Existing-database compatibility repair remains background-only, but now uses:
   - `lock_timeout = 2000ms`
   - `statement_timeout = 30000ms`
   - `idle_in_transaction_session_timeout = 60000ms`
   It is therefore opportunistic rather than able to block normal application traffic for many minutes.
6. Existing PostgreSQL data is not deleted, truncated, dropped, or recreated by this hardening patch.

## Expected production behavior

The site may report `schema_not_ready` for a short time if a new additive schema migration is genuinely required, but it must not freeze the normal request pool behind AccessExclusiveLock. `/api/db-status` remains the source of truth for connectivity vs schema readiness.

## Validation

- Python compilation: all project Python modules passed.
- Static lock-safety checks: request-path Scalper schema function contains no executable DDL or advisory transaction lock.
- Request pool contains bounded lock/statement/idle-transaction limits.
- Background compatibility repair contains a 2-second lock timeout.
