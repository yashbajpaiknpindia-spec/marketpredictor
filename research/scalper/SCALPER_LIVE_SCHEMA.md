# UltraScalp live tables

`scalper_live_settings` — persisted arm/config state. `enabled=true` means the worker may run automatically in PAPER mode.

`scalper_live_sessions` — one row per live-paper worker session, with provider, depth level, counts, heartbeat and result summary.

`scalper_live_snapshots` — time-stamped five-level displayed depth and derived microstructure fields. Arrays contain five bid/ask prices and quantities.

`scalper_paper_trades` — idempotent paper trade ledger. The `trade_key` is unique so an open trade can be updated to its final exit without duplicating the trade.

The tables are additive and created with `CREATE TABLE IF NOT EXISTS`; they do not delete or rewrite the existing MarketPredictor database tables.
