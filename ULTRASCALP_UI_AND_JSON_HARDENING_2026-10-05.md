# UltraScalp live-paper hardening — 2026-10-05

## Fixes
- Normalize psycopg2 Decimal/date/time values before returning scalper JSON responses.
- Add additive/self-healing `scalper_live_settings` schema migration so older databases gain any missing settings columns without rewriting existing data.
- Replace blind browser `response.json()` calls in the scalper live controls with a robust JSON reader that reports HTTP status and non-JSON server responses.
- Persisted `enabled=true, auto_start=true` is now recovered automatically after a single-worker web-service restart.
- Scalper Live UI now has a single, prominent live banner and separate top/main status IDs; the previous duplicate DOM id was removed.
- Arm/save requests can request browser notification permission and emit one-time notifications on ARMED and LIVE state transitions, while preserving in-page toasts.
- The UI distinguishes `ARMED`, `PREOPEN CAPTURE`, and `LIVE · PAPER` states.

## Synthetic reference correction
The 96.26% figure was the **bull regime** result inside synthetic V8 (589 trades). The overall V8 lag-0 result was **84.9468% wins across 5,547 trades**. The V8 run was built on the later 50-level synthetic L2-style microstructure environment; the 96.26% number is not an overall win rate and is not real-NSE performance.
