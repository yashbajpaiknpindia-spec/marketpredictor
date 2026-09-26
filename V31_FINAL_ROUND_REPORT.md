# V3.1 Final Round: Data-First + Alpha-First Research Report

## Objective
Strengthen the combined intraday NSE/BSE cash-equity engine, expand research where the available data supports it, introduce genuine-1m ingestion paths, and test whether the new selection logic creates a robust positive net edge.

## Current verified corpus
- 61 sessions
- 225 symbols
- 1,029,375 five-minute rows
- 2026-06-24 through 2026-09-18
- 0 duplicate day/symbol/timestamp keys
- 0 invalid OHLC rows
- 0 negative-volume rows
- No synthetic 1-minute candles were introduced

The corpus remains 5-minute-only. Genuine historical 1-minute bytes, point-in-time universe/sector membership, intraday NIFTY/India VIX/sector series, bid/ask, tick/order-book and historical pre-open data are not present in the sandbox corpus.

## V3.1 engineering changes
1. `v3_core.py` is now versioned as `v3.1.0-data-first-alpha-first`.
2. Added `v3_data_acquisition.py` for genuine 1m CSV/CSV.GZ/Parquet ingestion and validation.
3. Added completed-5m resampling from genuine 1m input only; incomplete 5m buckets are dropped.
4. Added authenticated DhanHQ V2 and Upstox V3 1-minute fetch adapters.
5. Added public research-source catalog for the available 1m archives.
6. Added `/api/engine-v3/status` metadata describing 1m data requirements and acquisition routes.
7. Live authorization remains OFF until a strict untouched OOS net-positive edge is demonstrated.

## Research round results
### Candidate-meta selection
A fixed candidate-family library was generated first, then a causal meta-classifier was trained only on historical candidate rows. Thresholds were selected on validation and evaluated on later test periods.

6 expanding chronological folds:
- 1,250 selected OOS candidates
- Mean net after mandatory cost + 5 bps stress: **-0.1218%**
- Win rate: **36.0%**
- Profit factor: **0.648**
- Gross mean: **+0.0343%**
- Mean mandatory fee: **0.1061%**

Fold test means: -0.0836%, -0.2055%, -0.2457%, -0.1515%, +0.0066%, -0.1237%.

Result: **reject**.

### Cross-sectional paired long/short
Four fixed ranking composites were tested with 6/12-bar horizons and extreme tails. Pair trading was charged for both legs and stressed separately for slippage.

- 1,290 OOS pairs
- Mean net after paired cost + stress: **-0.3376%**
- Win rate: **34.96%**
- PF: **0.425**

Result: **reject**.

### Extreme directional cross-sectional ranking
Top/bottom tails were selected directionally using fixed ranking composites.

- 1,290 OOS selections
- Mean net after mandatory cost + 5 bps stress: **-0.1793%**
- Win rate: **39.22%**
- PF: **0.548**

Result: **reject**.

### Gap exhaustion / opening dislocation
Large gap fade/continuation hypotheses were fixed in advance; validation selected the rule/horizon and tests were chronological.

- 318 OOS selections
- Mean net after mandatory cost + 5 bps stress: **-0.2226%**
- Win rate: **37.74%**
- PF: **0.374**

Result: **reject**.

## Baseline retained for comparison
V2.3 broad portfolio:
- 120 trades
- Gross: +₹2,845.61
- Mandatory fees: -₹8,044.88
- 5 bps stress: -₹7,569.06
- Final stress result: **-₹12,768.33**

V2.3 classifier portfolio:
- 110 trades
- Gross: +₹656.56
- Mandatory fees: -₹7,272.87
- 5 bps stress: -₹6,843.33
- Final stress result: **-₹13,459.64**

## Data acquisition routes verified externally
### DhanHQ V2
Dhan documents 1, 5, 15, 25 and 60-minute historical candles for up to five years, with minute-data retrieval limited to a 90-day window per request. Authenticated access requires an access token.

### Upstox V3
Upstox documents minute historical candles from January 2022. For 1-15 minute intervals, the historical retrieval window is one month per request. Authenticated access requires a bearer token.

### Public archives
A Hugging Face dataset advertises 1-minute NSE stock/index data covering 2022-2026 at very large scale. A separate public GitHub dataset advertises 214 NSE F&O underlying stocks at 1-minute resolution from 2024-04 through 2026-04. These are research sources and need independent integrity/coverage checks before being used as production evidence.

The available public binaries could be verified as existing sources in web research, but their multi-GB payloads could not be downloaded into this sandbox. Therefore no claim is made that those 1-minute bytes were used in the simulations above.

## Promotion decision
No V3/V3.1 model is proven or production-authorized.

The correct next data operation is to load a genuine 1-minute historical set that overlaps the exact 2026-06-24 to 2026-09-18 test period, preferably from an authenticated provider. Once loaded, the same frozen V3.1 labels, cost model, walk-forward folds and blind-test protocol can be rerun without changing semantics.

## Why the search is not being artificially forced green
Repeatedly searching the same holdout until a positive number appears would turn research into curve fitting. A positive edge must survive chronological OOS validation and realistic costs, then an untouched blind test. The engine therefore remains locked rather than promoting a false edge.
