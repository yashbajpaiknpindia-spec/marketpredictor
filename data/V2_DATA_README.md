# V2 historical data contract

The engine's intended research contract is:

`REAL 1-minute OHLCV -> completed 5-minute signal bars -> next 1-minute execution`

The system must never synthesize 1-minute execution bars from 5-minute OHLCV.

## Accepted research layouts

- Application historical export: `historical_data/YYYY-MM-DD/SYMBOL.csv`
- A combined OHLCV CSV containing `timestamp,open,high,low,close,volume,symbol,day`
- A provider-specific 1-minute archive converted to the canonical columns above

Set `V2_HISTORICAL_1M_ROOT` to the real 1-minute archive location when using a minute corpus. The current workspace run did not claim 1-minute history because no verified 1-minute bytes were available locally.

## External source registry

See `historical_data_sources.json` for DhanHQ, Upstox V3, NSE and research-only public-dataset references.
