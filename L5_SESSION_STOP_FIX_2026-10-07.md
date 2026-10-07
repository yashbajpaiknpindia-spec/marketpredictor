# UltraScalp fix 2026-10-07 - L5 visibility, duplicate sessions, Stop, Test buttons
Files changed (copy over the same paths in your repo):
- app.py
- indstocks_client.py
- research/scalper/scalper_live_paper.py
- templates/index.html
No DB schema change, no DDL, nothing outside the Scalper tab touched.

## Update 2 - INDstocks token 429 (Cloudflare) handling
- indstocks_client.py: failed token requests now back off (30s, 60s ... max 10 min, honours Retry-After) instead of being retried by every quote call; errors are compacted (no HTML dumps); token_status() exposes safe auth health.
- scalper_live_paper.py: if scrip mapping fails at session start, it is retried every 30 s (previously the worker waited forever with 0 symbols).
- app.py / index.html: L5 panel shows "AUTH BLOCKED" with the exact reason + countdown; Test button reports token refusal clearly (no quote/depth call is made).

## Update 3
- Worker with 0 mapped instruments now reports phase "waiting_instruments" (UI: NOT RECORDING) instead of looking LIVE.
