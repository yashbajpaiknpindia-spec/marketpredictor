# UltraScalp UI persistence and polling fix — 2026-10-05

## Fixed
- Scalper live paper settings now expose a durable DB-backed `ARMED FOR NEXT NSE SESSION` state after refresh.
- Added persisted save timestamp and a live UI status-check timestamp so arming is not represented only by a transient toast.
- Provider test now records/displays its check time in the UI.
- Save & Arm verifies that the returned persisted setting is actually `enabled=true` before showing success.
- Default live quote batching increased from 30 to 60 instruments per full-quote request. This keeps a 40-name universe inside the provider's documented 5 quote-requests/second limit even with 500 ms polling: one full-quote request + one NIFTY LTP request per poll = up to 4 quote requests/second.
- Polling controls now label 500 ms as aggressive and 1,000 ms as recommended for REST depth + database load.

## Provider facts checked against current INDstocks docs
- `/market/quotes/full` supports up to 1,000 instruments and includes live price, volume and market depth.
- `/market/quotes/mkt` provides 5-level market depth.
- Quote API rate limit is 5 requests/second.
- WebSocket market-data streaming is the preferred method for low-latency continuous quotes.

## Validation
- 14 targeted tests passed.
- Python compilation passed for app, scalper engine/worker and INDstocks client.
