# UltraScalp live-data + paper-capture plan

## What the current INDstocks integration can actually provide

The current INDstocks API documentation confirms:

- `/market/quotes/full` returns live quote information and includes market depth.
- `/market/quotes/mkt` returns **5-level market depth** for one or more instruments.
- The price WebSocket provides **real-time LTP/quote streaming**.
- Historical REST data is OHLCV, not historical order-book replay.

For the MarketPredictor live scalper, the collector uses `/market/quotes/full` in small batches because the same response contains LTP, cumulative volume and the documented 5-level depth. The WebSocket is not used as the depth transport because the current public WebSocket documentation does not document 20/200-level depth messages.

## What tomorrow's collector stores

For each captured symbol/timestamp the database can store:

- LTP and cumulative volume
- bid prices and quantities for five levels
- ask prices and quantities for five levels
- spread
- L1 imbalance
- L5 imbalance
- microprice and microprice edge
- displayed-depth pressure
- displayed-depth change / OFI proxy
- signal direction and confidence
- expected move and remaining-edge calculation
- the exact UltraScalp configuration used

Paper trades store entry, target, protection, exit, holding time, gross/net return and rupee P&L on the selected reporting notional.

## What is NOT in tomorrow's INDstocks feed

Do not label these as present unless another provider is connected:

- L10 depth
- L20 depth
- L50 depth
- L200 depth
- order-add/cancel events
- true queue position
- canonical event-level OFI
- exchange-provided aggressive-buy/aggressive-sell flags
- reconstructed historical order book

The collector therefore labels its OFI field **OFI_PROXY**. It is a displayed-depth change proxy, not canonical event-level OFI.

## Automatic paper run

After the Scalper Engine is armed:

1. The worker waits for the next NSE trading session.
2. It begins live capture at 09:00 IST when pre-open capture is enabled.
3. Paper entries become eligible at 09:15 IST.
4. The frozen research rules are used: 0.60% target, 0.22% protection, 10-minute maximum hold, 0.1363% round-trip friction, zero additional tested entry-lag, 0.20% minimum remaining-edge gate, score threshold 65, symmetric long/short logic and market/relative-strength confirmation.
5. No broker order endpoint is called.
6. New entries stop before the close; open paper positions are force-closed at the session safety cutoff.
7. Snapshots, decisions and exits are persisted for later replay.

## Storage modes

The current live-paper mode uses **full parsed 5-level depth capture** rather than raw provider JSON. That keeps the data sufficient for research while avoiding unnecessary duplicate response payloads.

Because one-second capture across many stocks can create a large database, the default live universe is 40 liquid NSE names. The universe setting can be increased, but storage grows roughly with:

`stocks × samples_per_second × session_seconds`.

## Evidence rule

Synthetic L2 results remain a controlled architecture experiment. Tomorrow's live session is a separate real-market evidence class. Parameters should not be tuned on the same session that is later used as the final proof of performance.
