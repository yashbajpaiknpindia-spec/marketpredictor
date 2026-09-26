# V2.2 Deep Six-Phase Simulation Report

## Scope

This run upgrades the engine around a strict `real 1m execution -> completed 5m signal` architecture, separates predictive alpha from trading costs and slippage, adds a continuous ridge alpha layer, uses expanding purged walk-forward folds, and keeps research and production authorization separate.

The largest unique historical corpus available in the current workspace is the 61-session NSE Large/Mid export covering 2026-06-24 through 2026-09-18. It contains 225 stocks with data and 13,725 stock-day files, but it is 5-minute data. It does not contain usable historical 1-minute candles, NIFTY/India VIX/sector-index intraday series, or point-in-time universe membership. Therefore this exact run is a **5m signal + 5m compatibility execution test**, not a 1m execution proof. No synthetic 1m candles were created.

## Changes implemented

### Phase 1 — Data & execution foundation

- Canonical completed-5m signal bars from either direct 5m input or real 1m input.
- Strict 1m-to-5m resampling requires five source minutes; incomplete final 5m buckets are dropped.
- Duplicate timestamps removed.
- Live/Paper/Replay V2 feature path now normalizes 1m input to real 5m signal bars.
- Raw 1m data remains available for the execution layer when present.
- Artificial minimum cost floor removed.
- Mandatory fees and slippage are separate fields.

### Phase 2 — Market/regime context

- Breadth, median market return, cross-sectional dispersion and sector-relative features are part of the alpha feature vector.
- The engine can continue to reject unclear/no-edge states rather than manufacture trades.

### Phase 3 — Edge discovery

- Research evaluates signal conditions independently before portfolio deduplication.
- Forward outcomes include 1m-equivalent/short-horizon fields where the underlying resolution permits; this current corpus directly evaluates 5m-derived 5/15/30/60-minute horizons.
- Features include momentum, VWAP deviation, relative strength vs market/sector, volatility, range position, opening-range distance, candle-body/close-location and volume/ATR state.

### Phase 4 — Edge model

- Continuous ridge regression predicts signed 60-minute forward return from the feature vector.
- Validation chooses an evidence threshold only from the validation window.
- Production model authorization requires positive out-of-sample evidence; the current model is explicitly `proven=false`.

### Phase 5 — Risk/execution

- Next available bar entry.
- ATR-based stop with hard bounds.
- 1.6R target.
- Portfolio cap: max four concurrent positions, five trades/day and 1% capital-risk constraint.
- Daily loss guard at 2% of ₹2,00,000.
- Stop/target same-bar conflicts are conservative.
- Slippage is charged once and reported separately.

### Phase 6 — Validation

- Expanding walk-forward folds.
- One-day purge between train, validation and test windows.
- Unseen test observations are never used to train the model.
- Robustness reporting includes win rate, profit factor, drawdown and top-winner dependence.

## Exact simulation result

Starting capital: **₹2,00,000**.

The broad continuous alpha model produced **17,365 out-of-sample observations** across the walk-forward test windows.

- Mean net forward return after mandatory costs: **-0.1062% per selected observation**.
- Median: **-0.1063%**.
- Win rate: **37.29%**.
- Profit factor: **0.534**.
- Aggregate selected-observation net forward return: **-1,843.53 percentage points** (observation-level sum, not account P&L).
- Positive test days: **17.24%** of days containing selected observations.

The compatibility trade simulation produced **145 portfolio trades** after the four-position/five-trades-per-day/risk constraints:

- Gross strategy P&L: **+₹2,159.17**.
- Mandatory brokerage/statutory/exchange fees: **-₹7,478.50**.
- Slippage at 5 bps: **-₹7,125.61**.
- Net P&L: **-₹12,444.94**.
- Net P&L before slippage: **-₹5,319.34**.
- Win rate: **28.97%**.
- Profit factor: **0.367**.
- Maximum drawdown: **₹12,606.57**.

The result is therefore **not positive edge**. In particular, the raw gross movement was slightly positive in the compatibility trade simulation, but it was far too small to cover realistic round-trip friction.

## Interpretation

The current evidence points to two different problems:

1. **The predictive model is not producing positive conditional edge out of sample.**
2. **The trades it does select have gross movement that is too small relative to real trading friction.**

That is why simply adding more stops, indicators, or confidence points would be the wrong next optimization.

## What this run does prove

- The engine can now maintain a clean distinction between 5m signal construction and real 1m execution when 1m history exists.
- The live/paper/replay V2 feature path no longer interprets raw 1m bars as if each were a 5m bar.
- The model can be prevented from trading until it has actual out-of-sample evidence.
- Costs can be audited separately from signal quality.

## What it does not prove

It does **not** prove that a 1m-execution strategy has no edge, because the currently executable historical corpus is still 5m. It also does not establish point-in-time universe correctness because the supplied historical universe is a current snapshot applied backward.

For a final 1m execution claim, the application now accepts real minute archives and has a source registry for DhanHQ, Upstox V3, NSE data products and research-only public minute datasets. Official documentation confirms that DhanHQ exposes 1-minute intraday historical candles and that Upstox V3 exposes minute candles from January 2022 with 1-minute retrieval windows; NSE also publishes official historical India VIX/index resources and commercial 1-minute/5-minute snapshot products.

## Production status

**Production authorization: OFF.**

The diagnostic alpha model is stored with `proven=false` and cannot bypass the V2 gate. This is intentional: the current evidence does not justify live deployment.
