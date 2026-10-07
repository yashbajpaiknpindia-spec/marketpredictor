# UltraScalp 250-universe / estimator / settings hardening — 2026-10-07

## Fixed

- Fixed Scalper settings read path to use the read-only Scalper schema contract consistently.
- Changed settings persistence from UPDATE-only to atomic INSERT...ON CONFLICT upsert so a schema-without-singleton-row can still save.
- Added server-side save round-trip verification for `universe_limit`.
- Removed legacy 40-stock defaults from the Scalper settings/session fallback.
- Live worker fallback universe is now 250.
- Added a dirty-settings guard so the 3-second status poll cannot overwrite a user's unsaved 50/75/100/150/200/250 selection.
- Normal “Save settings” now preserves the currently selected Enabled/Disabled state instead of silently disabling capture.
- Save & Arm explicitly enables capture.
- Estimator now resolves the actual eligible LargeMidcap-250 count and reports requested vs resolved counts.
- Estimator no longer performs `COUNT(*)` over `scalper_live_snapshots`. It uses the session ledger's stored snapshot totals plus `pg_total_relation_size`, with a short cache.
- Session history no longer groups/counts millions of snapshot/trade rows on every 3-second page refresh. It uses durable session counters.
- Status no longer counts all today's paper trades on every 3-second status request.
- Effective poll interval now records the observed poll-cycle duration including request/processing time plus configured wait.

## Verification

- Python compile checks passed for modified Python files.
- Inline browser JavaScript passed `node --check`.
- Targeted static Scalper test suite updated for the read-only schema design.
