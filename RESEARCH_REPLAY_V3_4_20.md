# MarketPredictor Research Replay — v3.4.20

## What changed

This release separates **strategy research** from **portfolio capacity**. The 20 strategy definitions in `strategies.py` are unchanged.

### Research Sweep

A Research Sweep:

- evaluates all 20 base strategies independently in both LONG and SHORT directions;
- does not discard a valid signal because another strategy used capital, risk budget, position slots, cooldowns, minimum order value, or expected-profit gates;
- keeps realistic execution mechanics: next-open fills, slippage, transaction costs, strategy target/stop, intrabar conflict handling, target exits and end-of-day flattening;
- uses a fixed research notional for comparable per-signal economics instead of account-capital scarcity;
- records context on completed trades so later analysis can condition performance on regime, time, volatility, extension and relative strength;
- does not change strategy definitions or parameters.

## Loss and profit protection contract

For research replay only, losses are left to the strategy's original stop. The replay does not manufacture early losses through post-entry risk rechecks, time-decay exits, or late-session weakness exits.

The research profit trail is separate from that original loss stop. It only arms after the favorable peak has cleared the configured activation percentage **and** a real net-profit threshold after costs. Once armed, the protection floor is cost-aware so a fixed percentage lock cannot accidentally turn a cost-covering move back into a guaranteed net loss.

Live/Paper exit activation semantics are not changed by the research-only net-profit arm gate.

## Performance telemetry

Every replay persists:

- elapsed time;
- estimated total duration;
- estimated remaining duration;
- estimated finish timestamp;
- scan-cycle progress and rate;
- profit-factor checkpoint based on **net P&L after costs**;
- PF sample state (`no_trades`, `forming`, `stable_sample`, `complete`).

Completed historical runs that predate the timing columns are presentation-backfilled from `started_at`/`finished_at` when those timestamps exist.

## Long-range replay and export

Requests longer than the normal single-run session cap are automatically segmented. The parent run owns the full date range and consolidates child-run results, while each child stays within the bounded replay window.

A long/segmented export uses a disk-backed streaming ZIP. Trades, events, capital snapshots, reference data and candle cache are streamed in bounded batches rather than materialized into one giant Python object.

## Web-worker protection

Research/tournament execution is launched in a separate spawned process. Gunicorn remains responsible for HTTP status/polling/UI traffic, while the CPU-heavy historical feature and strategy loop runs outside the web worker.

Stop/pause controls use process-safe Events, and the persisted replay row remains the status source of truth.

## Strategy-selection stage

The replay remains an **evidence collector**, not a cross-strategy competition. The application already contains the context-conditioned evidence functions used by the normal selector. This release deliberately avoids changing those selector/strategy rules until the longer independent research sweep provides enough out-of-sample evidence to validate the selector.

## Validation in this build

Passed:

- `python -m py_compile` for the application source;
- JavaScript syntax check for the main template;
- `tests/test_replay24_execution.py`;
- `tests/test_replay_download_safety_static.py`;
- `tests/test_db_pool_local.py`;
- all Python files compile successfully;
- `strategies.py` SHA-256 matches the uploaded original exactly.

The repository's `tests/test_export_memory.py` cannot collect in this environment because the optional `ijson` dependency is not installed. That is an environment/dependency limitation, not a failing assertion in the modified replay code.
