# UltraScalp V12 DB-safe bootstrap + compressed L5 storage

Date: 2026-10-08

## What was fixed

1. Existing production PostgreSQL databases now receive the missing UltraScalp live-paper schema through an additive compatibility migration.
2. The compatibility migration creates only missing tables, indexes, and columns. It does not update, delete, truncate, rewrite, or backfill historical MarketPredictor rows.
3. If the global schema marker is already current but the UltraScalp schema is missing, bootstrap now detects that condition and launches the same additive repair rather than falling through to the older monolithic migration.
4. The Scalper schema readiness check remains read-only on request paths. It waits for the dedicated compatibility repair instead of running DDL from a web request.
5. New L5 snapshots store the raw five-level ladder in a compressed `BYTEA` payload using `zlib-json-v1`. Existing rows retain their original JSONB ladders unchanged.
6. CSV exports and exact L5 replay restore compressed ladders into the same bid/ask array shape used by legacy rows.
7. Save/Arm now verifies settings after the PostgreSQL write, including enabled/auto_start and the V12 model/risk values. A mismatch is returned as an error instead of being reported as successful.
8. The V12 minimum model expected-edge floor remains exactly 0.0%, so a configured zero floor is no longer silently changed.
9. UltraScalp worker start/snapshot handling was cleaned up so state snapshots do not hold the worker state lock while performing database reads.

## Preserved behavior

- Positive blind direction + edge V12 policy is unchanged.
- Immediate favorable-move trail is unchanged from the preceding release.
- V12 target, stop, friction, slippage, timeout, probability gate, edge gate, and trailing settings remain as configured.
- Paper-only mode remains enforced.
- Historical MarketPredictor data is not rewritten by the compatibility migration.

## Validation

- 18 targeted DB/UltraScalp safety and V12 tests passed.
- Full Scalper/UltraScalp test sweep: 55 passed; 4 integration tests could not run locally because the test environment did not have Flask installed.
- 113 Python source files parsed successfully.
- `app.py` and `research/scalper/scalper_live_paper.py` compiled successfully.
- Browser JavaScript extracted from `templates/index.html` passed `node --check`.
