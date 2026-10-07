# UltraScalp deployment data-safety checklist — 2026-10-06

## What is persistent
UltraScalp live-paper data is stored in the existing PostgreSQL database, not in the Render web-service filesystem:

- `scalper_live_settings`
- `scalper_live_sessions`
- `scalper_live_snapshots`
- `scalper_paper_trades`

A normal Render application rebuild/redeploy does **not** delete these rows as long as the service continues to use the same `DATABASE_URL`.

## Code-safety checks performed
- No `DROP TABLE`/`TRUNCATE`/`DELETE FROM` statements target the four Scalper tables.
- Scalper schema bootstrap uses `CREATE TABLE IF NOT EXISTS` and additive `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` only.
- The storage "delete all market/research" path does not include the Scalper tables.
- Render blueprint does not create a replacement PostgreSQL database; `DATABASE_URL` remains an external service variable.
- Snapshot/paper-trade exports are available from the Scalper page.

## Before deploying
1. Export the current full 5-level snapshot CSV and latest session JSON from the Scalper page.
2. Optionally run `python scripts/verify_scalper_db_safety.py` against the same `DATABASE_URL` and save the output.
3. Confirm the Render service still points to the existing PostgreSQL `DATABASE_URL`. Do not replace it with a new database.

## After deploying
1. Open Scalper Engine.
2. Confirm the live-paper session and stored snapshot count are still present.
3. Run `python scripts/verify_scalper_db_safety.py` again if you want an exact before/after row-count check.
4. Export a session JSON again if you need an audit checkpoint.

## Important
The ZIP does **not** contain the live PostgreSQL rows. That is intentional. Including them in the application bundle would be neither necessary nor safe. The database is the source of truth.
