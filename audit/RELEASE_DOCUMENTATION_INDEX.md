# Release Documentation Index

The v3.4.15 ZIP intentionally carries the existing version reports rather than replacing them with one rewritten history file.

## Included report groups

- V3 Alpha / combined research status
- V3.1 data backfill and final round
- V3.2 engine specification, requirements, post-fix audit, simulation, provider/fallback, export, persistence, low-memory, deployment/stress and 20-strategy replacement reports
- V3.3.0–V3.3.3 boot/database diagnostics and state reports
- V3.4.0–V3.4.15 database, gate, Replay, forensic, strategy-independence, UX, data-reuse, attribution and speed reports
- Existing strategy PF source ledger and application README
- Replay #23 forensic source artifact under `audit/`

## Master documents added in v3.4.15

- `audit/RELEASE_HISTORY_AND_REPLAY_FINDINGS.md` — chronological version map + replay findings + safety invariants
- `audit/REPLAY_FINDINGS_REGISTER.md` — compact finding → evidence → fix → version register
- `audit/RELEASE_DOCUMENTATION_INDEX.md` — this index
- `audit/RELEASE_HISTORY_MANIFEST.json` — machine-readable report inventory
- `audit/replay_run_23_20260927_210409_forensics.zip` — raw Replay #23 forensic artifact

## v3.4.16 addition

- `V3.4.16_REPLAY_DOWNLOAD_OOM_FIX.md` — Replay #24 download crash diagnosis, evidence interpretation, root cause and bounded-memory ZIP implementation.
