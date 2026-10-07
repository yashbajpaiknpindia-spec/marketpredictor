# UltraScalp DB Deadlock Hotfix — 2026-10-06

## Problem diagnosed from Render logs

Concurrent Gunicorn workers were calling `_ensure_scalper_live_schema()` from normal Scalper GET/status/settings paths. The function executed `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS` on every request. PostgreSQL DDL takes strong relation locks; two workers could therefore deadlock while both attempted additive DDL on the same Scalper relation.

Observed production symptom:

- `SCALPER_LIVE settings read failed: deadlock detected`
- both processes waiting for `AccessExclusiveLock` on the same relation
- API responses could still be HTTP 200 while the browser repeatedly waited/retried, making the site appear stuck/loading

## Fix

Normal request paths now use a **read-only information_schema contract check** and never run Scalper DDL when the schema is already complete.

For an older database that is missing a Scalper table/column:

1. One process acquires a PostgreSQL transaction-scoped advisory lock.
2. It re-checks the schema after acquiring the lock.
3. Only that process performs the additive DDL.
4. Other workers wait on the advisory lock and re-check rather than executing concurrent DDL.
5. A per-process ready flag avoids repeated catalog work after the first successful check.

No Scalper table is dropped, truncated, recreated, or migrated destructively.

## Existing data safety

The hotfix does not delete existing Scalper snapshots, sessions, or paper trades. The existing `DATABASE_URL` continues to be used. The session delete feature remains explicit and user-triggered.

## Provider endpoint audit

The live collector continues to use the documented INDstocks endpoints:

- `GET /market/quotes/full` — full quote
- `GET /market/quotes/mkt` — 5-level market depth
- `GET /market/quotes/ltp` — LTP

The application batches the 250-stock LargeMidcap universe within the documented quote batch capacity and maintains a 1-second minimum polling interval in REST mode.

## Validation

Targeted Scalper/static validation after the fix: **22/22 passed**.

Python compilation: passed.

A full live provider/API call and production Flask boot were not performed in this container because production credentials and the live Render process are not available here.
