# UltraScalp 2026-10-06 — LargeMidcap 250 + storage estimator + session deletion

## What changed

- UltraScalp live/paper universe is now bounded to the authoritative **Nifty LargeMidcap 250** membership (100 large-cap + 150 mid-cap), rather than the old fixed ~40-stock list.
- Universe selector supports **20 / 40 / 50 / 75 / 100 / 150 / 200 / 250** stocks.
- For selections below 250, the existing curated liquid-core list is used only as a deterministic priority ordering. It is not represented as a live liquidity score; all selected names remain inside the official LargeMidcap 250 boundary.
- If the official LargeMidcap 250 allowlist cannot be resolved or does not overlap the configured NSE universe, the scalper **fails closed** instead of scanning a random/full-market universe.
- Live quote batching is now **250 instruments/request**. INDstocks documents up to 1000 instruments for quote calls, so the scalper no longer creates unnecessary 60-symbol batches.
- The three live quote paths are explicitly:
  - `GET /market/quotes/full` — LTP/volume/full quote
  - `GET /market/quotes/mkt` — dedicated 5-level market depth
  - `GET /market/quotes/ltp` — NIFTY 50 market-context LTP
- REST polling remains hard-clamped to **>= 1000 ms**.
- At 250 names and 1000 ms polling, the worker uses 3 quote API calls per poll (full + depth + NIFTY LTP), which remains below the documented 5 quote requests/second limit and under the documented 100,000/day quote-call limit for one NSE session including the 15-minute preopen capture.
- The UI now estimates before arming:
  - regular-session snapshot rows;
  - rows including the 09:00–09:15 preopen capture;
  - estimated PostgreSQL footprint;
  - quote API calls and remaining documented daily headroom.
- Storage estimates use the observed `pg_total_relation_size / snapshot_count` footprint when rows already exist; a conservative 1.5 KiB/snapshot planning value is used only when the table is empty.
- Added session history with per-session snapshot count, estimated DB footprint, snapshot/trade exports, and **Delete**.
- Session deletion is transactional and only deletes the selected row from `scalper_live_sessions`; `scalper_live_snapshots` and `scalper_paper_trades` are removed by their existing `ON DELETE CASCADE` foreign keys. Running sessions cannot be deleted until capture is stopped.
- Session configuration now records the exact selected universe and selection method for reproducibility.
- Corrected the global INDstocks client default request gap to 0.21s, keeping the client below the documented 5-request/second Quote/Data API limit unless an environment override is deliberately configured.

## Endpoint audit

The provider paths in `indstocks_client.py` were checked against current INDstocks documentation on 2026-10-06:

- `/market/quotes/full`
- `/market/quotes/ltp`
- `/market/quotes/mkt`
- `/market/instruments?source=equity|index`

The dedicated depth endpoint is `/market/quotes/mkt` and returns five bid/ask levels. The worker does not claim 20/200-level depth or canonical event-level OFI.

## Validation

- Python compilation: `app.py`, `indstocks_client.py`, and `research/scalper/scalper_live_paper.py` passed `py_compile`.
- Inline JavaScript passed `node --check` after extraction from `templates/index.html`.
- All Scalper tests passed with `PYTHONPATH=.`: **25 passed**.
- Full Flask runtime boot was not performed in this container because the deployment environment's Flask stack is not installed here; source/static/unit validation was completed instead.
