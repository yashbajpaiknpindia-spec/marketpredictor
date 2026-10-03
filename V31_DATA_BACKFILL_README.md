# V3.1 Data Backfill

V3.1 does not fabricate 1-minute candles. It accepts genuine 1m CSV/CSV.GZ/Parquet files and can resample those bars into completed 5m structural bars.

## Exact 2026 historical routes

### DhanHQ V2
Set `DHAN_ACCESS_TOKEN` in the environment and use `download_dhan_intraday()`. Dhan documents 1/5/15/25/60-minute historical data for up to five years, with a 90-day polling window for minute data.

### Upstox V3
Set `UPSTOX_ACCESS_TOKEN`. Upstox V3 documents 1-minute historical data from January 2022, with a one-month maximum retrieval window for 1–15 minute intervals.

## Public research archives

The public Hugging Face minute dataset covers 2022–2026 and is very large. The public GitHub NSE F&O underlying dataset covers April 2024–April 2026. Neither should be treated as a substitute for exact Jun–Sep 2026 execution history unless the actual bytes overlap that period and pass the audit.

## Validation

1. Load genuine 1m bars.
2. Audit duplicate `(day,symbol,timestamp)` keys, OHLC consistency, negative volume, regular-session coverage and timestamp timezone.
3. Freeze raw-data hash.
4. Derive completed 5m bars from the genuine 1m source.
5. Run the same V3 walk-forward research semantics without changing labels or decision rules.

No model is promoted merely because an expanded dataset produces a green in-sample result.
