# INDstocks auth + request-storm fix - 2026-10-07

Files changed: indstocks_client.py, app.py, research/scalper/scalper_live_paper.py, templates/index.html
New tests: tests/test_indstocks_auth_hardening.py, tests/test_scalper_end_to_end_mock_provider.py
New table (auto-created, tiny): indstocks_auth_state (one row).

## Why nothing was recorded
Capture needs a token before it can fetch anything. `POST /generate/token` was answered with a
Cloudflare "Just a moment..." HTTP 429 page, so no token -> no instruments -> 0 mapped -> 0 snapshots.
The L5 parser and the snapshot writer were not at fault (see tests: documented payloads -> 5 levels -> rows persisted).

## What kept the token endpoint blocked (all fixed)
1. Token and cooldown lived only in process memory. Every deploy/restart minted again, and during a Render deploy the
   old and new instance both minted. INDstocks keeps ONE live TOTP token and allows 1 mint / 60 s, so instances
   invalidated each other (403 -> force refresh -> invalidates the other ...).  -> token + cooldown now shared in PostgreSQL,
   one minter at a time (advisory lock), restarts reuse the stored token.
2. "Test INDstocks now" bypassed the back-off every 20 s. -> at most once per 65 s, never inside the provider's 60 s rule.
3. A failed instrument-master download was retried for EVERY symbol (250 symbols x 2 CSV downloads). -> failure memory (60 s)
   and a forced reload at most once per 10 min; the worker stops on the first provider/auth error instead of looping.
4. A 403 from Cloudflare on a data endpoint was treated as "token revoked" and triggered a mint. -> only real token rejections remint.
5. Failure delays: Cloudflare challenge 120 s -> 900 s, wrong MPIN/TOTP >= 120 s (5 wrong codes = 15 min lockout).

## Request budget (docs: Quote APIs 5/s and 100,000/day)
Before: full + mkt + NIFTY LTP = 3 calls/poll = ~70,200/session at 1 s polling.
Now: /market/quotes/mkt is only called for stocks whose full quote lacks a complete 5-level ladder (skipped entirely when
full already carries it) -> as low as 2 calls/poll (~46,800/session).

## New option: INDSTOCKS_ACCESS_TOKEN
Set a dashboard-generated token (24 h) in Render env. No /generate/token call is made while it is set. If it expires/is revoked,
the app says so (and falls back to TOTP minting only if INDSTOCKS_API_KEY/MPIN/TOTP_SECRET are also set).
Kill switch for the DB store: INDSTOCKS_TOKEN_DB_STORE=0.
Note: the shared token is stored in your own PostgreSQL (table indstocks_auth_state), same trust level as the env var.
