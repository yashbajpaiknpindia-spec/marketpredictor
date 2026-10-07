# V3 Alpha-First Intraday Engine
## Research + Engineering Specification

**Scope:** NSE/BSE cash equities only; large/mid-cap universe; intraday only; no equity F&O execution.

**Capital research baseline:** ₹2,00,000.

**Design principle:** The system is allowed to produce zero trades. A trade is permitted only when the research layer demonstrates sufficient expected opportunity, the entry is not late, the economics clear mandatory costs with headroom, and portfolio/risk constraints are satisfied.

---

## 1. Objective

Build an intraday engine whose primary optimization target is:

> **Expected net opportunity per unit of risk after mandatory costs, with execution realism and strict out-of-sample validation.**

The engine must not optimize for:
- number of trades,
- raw signal count,
- bullish/bearish confidence alone,
- in-sample accuracy,
- gross backtest P&L alone,
- or forced daily activity.

The production authorization state must remain **disabled** until a research model passes all required validation gates.

---

## 2. Non-negotiable design rules

1. **No lookahead.** Every feature uses only information available before the decision timestamp.
2. **Completed-bar semantics.** A 5-minute signal feature is calculated only from completed 5-minute bars.
3. **Next executable price.** Historical labels and simulations align entry to the next executable event, not to the information-generating bar's close.
4. **Real 1-minute data only.** Do not manufacture 1-minute OHLCV from 5-minute bars and call it genuine execution data.
5. **Point-in-time data where possible.** Universe and sector membership must be time-correct; current membership must not silently be applied backward.
6. **Independent signal research.** Do not collapse all signal families into one score before measuring whether each family has independent predictive value.
7. **Portfolio deduplication.** Multiple signals on the same stock/direction are treated as potentially one underlying exposure.
8. **Dynamic costs.** Brokerage, exchange charges, statutory charges, taxes, and other applicable costs come from a configurable broker/market-cost model; no arbitrary fixed cost floor.
9. **Slippage is separate.** Mandatory charges and slippage are reported separately and are not double-counted.
10. **No forced trades.** Zero candidates is an acceptable and often preferable outcome.
11. **Blind testing.** Final untouched data is never used to choose features, thresholds, or exits.
12. **Production model state is explicit.** Every trained model has `research_only`, `candidate`, `validated`, or `production_authorized` status.

---

## 3. System architecture

```text
DATA INGESTION
    |
    +--> Data quality / clock / corporate-action checks
    |
    v
CANONICAL 1m MARKET DATA
    |
    +--> Completed 5m structural bars
    +--> Cross-sectional universe state
    +--> Market/index state
    +--> Sector state
    +--> Event/calendar state
    |
    v
REGIME ENGINE
    |
    +--> Market regime
    +--> Volatility regime
    +--> Sector regime
    +--> Session phase
    |
    v
CANDIDATE GENERATION
    |
    +--> Momentum / trend
    +--> Breakout / expansion
    +--> Compression
    +--> Mean reversion
    +--> VWAP structure
    +--> Relative strength
    +--> Market-sector-stock alignment
    +--> Opening behavior
    +--> Failed-breakout behavior
    |
    v
ALPHA LAYER
    |
    +--> Direction model
    +--> Remaining-move model
    +--> Favorable-excursion model
    +--> Adverse-excursion model
    +--> Time-to-event model
    |
    v
TRADE ECONOMICS
    |
    +--> Gross expected move
    +--> Mandatory round-trip cost
    +--> Slippage stress
    +--> Required headroom
    |
    v
PORTFOLIO / RISK ENGINE
    |
    +--> Correlation deduplication
    +--> Exposure limits
    +--> Position sizing
    +--> Daily loss protection
    +--> Chase / late-entry protection
    |
    v
1m EXECUTION ENGINE
    |
    +--> Entry timing
    +--> Order state machine
    +--> Stop / target / structure exits
    +--> Time stop
    |
    v
TRADE FORENSICS
    |
    +--> Candidate log
    +--> Decision log
    +--> Execution log
    +--> Cost ledger
    +--> MFE / MAE
    +--> Exit attribution
    +--> Research feedback dataset
```

---

# 4. Modes and parity

All modes use the same decision semantics:

### Live
- real-time 1m stream
- completed 5m structural updates
- real order/execution adapter

### Paper
- live market data
- same decision code
- simulated execution

### Replay
- historical 1m stream played chronologically
- same clock/event semantics as live
- no access to future rows
- controls for speed, pause, resume, stop

### Backtest
- event-driven historical simulation
- same feature/decision functions
- walk-forward training/validation/test partitions

Only the **data source and execution adapter** change between modes.

---

# 5. Data contract

## 5.1 Required market data

### Core
- symbol
- exchange
- timestamp
- open
- high
- low
- close
- volume

### Preferred execution data
- bid
- ask
- bid size
- ask size
- trade count
- spread estimate

### Market context
- NIFTY broad benchmark
- relevant large-cap index where applicable
- India VIX or equivalent verified volatility series
- sector indices

### Reference data
- point-in-time eligibility universe
- point-in-time sector classification
- corporate-action adjustments
- trading-session calendar
- symbol mapping / ticker changes

### Event data
- earnings/event calendar where legitimately available
- exchange announcements where usable
- abnormal corporate-event flags

Do not fabricate missing history.

---

# 6. Data-quality layer

Every session is checked before research use.

### Hard checks
- duplicate timestamps
- non-monotonic timestamps
- impossible OHLC relationships
- negative volume
- missing mandatory prices
- exchange/session mismatch
- gaps during expected trading hours
- symbol mapping failures
- corporate-action discontinuities

### Completeness flags
Each symbol/session receives:

```text
bars_expected
bars_present
coverage_pct
missing_bar_count
longest_gap_minutes
quality_status
```

Research and production can reject symbols with insufficient coverage.

---

# 7. Canonical time model

The engine maintains two separate clocks:

### Structural clock
5-minute completed bars.

### Execution clock
1-minute observations when genuine 1m history/live data exists.

A structural decision at `T` can only use data whose observation period ended at or before `T`.

If a 5m bar covers 09:20:00–09:24:59, the signal is considered available only after that bar is complete.

Historical entry then occurs at the **next executable event**, normally the next 1m bar's open in a simplified backtest.

More realistic execution can use next 1m tradeable price/quote simulation once quote data exists.

---

# 8. Feature architecture

Features are grouped by information source rather than by indicator name.

## 8.1 Price/return state
- 1-bar return
- 3-bar return
- 6-bar return
- 12-bar return
- session return
- return from open
- rolling volatility
- ATR percentage
- realized volatility

## 8.2 Structure
- range position
- distance from breakout level
- opening-range distance
- prior high/low distance
- close location
- candle body strength
- wick asymmetry
- compression score
- expansion score

## 8.3 Volume
- relative volume
- volume acceleration
- volume versus ATR/range
- abnormal-volume flags

## 8.4 VWAP
- distance from VWAP
- slope/context of VWAP
- time spent above/below VWAP
- VWAP reclaim/loss
- extension versus volatility

## 8.5 Cross-sectional features
- stock return percentile
- volume percentile
- relative strength vs benchmark
- relative strength vs sector
- breadth
- dispersion

## 8.6 Market/sector alignment
- stock vs market return
- stock vs sector return
- sector breadth
- benchmark trend state
- volatility regime

## 8.7 Session phase
- opening window
- morning trend
- midday
- afternoon
- closing window

These are features, not automatically trade rules.

---

# 9. Regime engine

The regime engine should be descriptive and causal.

### Market regime dimensions
- trend direction
- trend strength
- volatility
- breadth
- dispersion

### Sector regime
- sector trend
- relative strength
- breadth
- volatility

### Stock regime
- trending
- compressing
- expanding
- mean-reverting
- overextended

Represent regime as a vector, e.g.:

```text
market_trend = up
market_vol = high
breadth = strong
sector_strength = strong
session_phase = morning
stock_state = expansion
```

The system should permit different alpha relationships under different regimes.

---

# 10. Candidate families

All candidate families are researched independently.

## Family A — Momentum continuation
Look for persistent directional movement with evidence that continuation remains available.

## Family B — Breakout expansion
Break of a defined structure with abnormal participation and sufficient remaining range.

## Family C — Compression-to-expansion
Low realized volatility followed by expanding range/volume.

## Family D — VWAP structure
Reclaims, losses, controlled extensions, and mean-reversion contexts.

## Family E — Mean reversion
Only tested where there is historical evidence of reversion under the specific regime.

## Family F — Relative strength
Stock outperforming sector/benchmark with confirmation.

## Family G — Market-sector-stock alignment
Signal strength increases only when all relevant levels align.

## Family H — Opening behavior
Opening range expansion, continuation, failure, and gap-context behavior.

## Family I — Failed breakout / trap
Breakout fails and reverses with defined structure.

## Family J — Volatility/event-conditioned behavior
Only activated when the relevant data exists and the event regime is historically validated.

Each family must generate a candidate record before any shared ranking.

---

# 11. Candidate generation rules

Candidate generation must be permissive enough for research.

The purpose of this stage is:

> **Find potentially interesting situations, not decide which trade wins.**

Each candidate receives:

```text
candidate_id
session_date
timestamp
symbol
direction
family
regime_snapshot
raw_features
reference_price
structure_level
```

No future outcome is available to the live candidate object.

---

# 12. Alpha layer: predict a trade, not merely direction

This is the central V3 upgrade.

Each candidate should produce multiple forecasts.

## 12.1 Direction probability

```text
P(up)
P(down)
P(flat / insufficient move)
```

## 12.2 Remaining move

Estimate expected movement still available after the candidate forms.

```text
expected_remaining_move_pct
expected_remaining_move_rupees
```

## 12.3 Favorable excursion

Predict:

```text
P(MFE >= 0.20%)
P(MFE >= 0.40%)
P(MFE >= 0.60%)
P(MFE >= target)
```

Thresholds must be research parameters, not assumptions.

## 12.4 Adverse excursion

Predict:

```text
P(MAE >= stop_distance)
expected_MAE
```

## 12.5 Time-to-event

Estimate:

```text
expected_bars_to_target
expected_bars_to_failure
P(target_before_stop)
```

This allows the engine to reject a trade that is directionally attractive but too slow or too small to cover costs.

---

# 13. Labels

For a decision timestamp `t`:

```text
entry = next executable price after t
```

For a maximum horizon `H`:

```text
forward_return = future_close(H) / entry - 1
```

Additionally compute path-dependent labels:

```text
MFE = max(high path in H) / entry - 1
MAE = min(low path in H) / entry - 1
```

For short research, invert the signs appropriately.

Also compute barrier/event labels:

```text
hit_target_before_stop
hit_stop_before_target
neither_hit_by_time_stop
```

Important: the exact target/stop parameters must be frozen before the corresponding OOS test.

---

# 14. Multiple horizons

Do not hard-code only one 6-bar outcome.

Research:

```text
1 bar
2 bars
3 bars
6 bars
9 bars
12 bars
```

The model should learn whether the opportunity is:

- immediate,
- short-lived,
- persistent,
- or effectively nonexistent.

The production executor can still use a strict maximum hold, but the research layer needs the entire opportunity curve.

---

# 15. Entry timing model

The engine should distinguish:

### Setup exists
from

### Entry is still good.

Useful derived features:

- distance already traveled from setup origin
- percentage of expected move already realized
- extension from VWAP
- extension from breakout level
- current range percentile
- acceleration in the last 1–3 minutes
- volume exhaustion
- distance to structural invalidation

A candidate should be rejected as `late_entry` when expected remaining opportunity is too small relative to distance already traveled and trading costs.

---

# 16. Economic model

For every candidate:

```text
expected_gross_pct
expected_gross_rupees
mandatory_round_trip_cost_rupees
mandatory_round_trip_cost_pct
stress_slippage_rupees
stress_slippage_pct
required_edge_hurdle
expected_net_before_slippage
expected_net_under_stress
```

The model must never use a magical fixed ₹40 or similar floor.

### Economic acceptance concept

```text
expected_gross
    >= mandatory_cost
       + safety_margin
```

A stronger production formulation:

```text
expected_net_before_slippage >= minimum_net_edge
AND
expected_gross / mandatory_cost >= minimum_cost_multiple
```

Stress slippage remains an additional reported scenario.

---

# 17. Dynamic position sizing

Base risk starts from account-level risk budget.

Example research baseline:

```text
capital = ₹2,00,000
risk_per_trade <= 1.0%
```

But position size is constrained by:

```text
risk budget
stop distance
maximum notional
liquidity
portfolio exposure
sector exposure
same-symbol exposure
```

Formula:

```text
shares = floor(rupee_risk_budget / rupee_stop_distance)
```

Then cap against notional and liquidity limits.

Position size must never be increased solely because model confidence is high.

---

# 18. Portfolio selection

After candidate scoring, the system performs deduplication.

### Example

```text
Reliance / momentum / BUY
Reliance / breakout / BUY
Reliance / sector-strength / BUY
```

These are aggregated into one underlying exposure.

Portfolio selection should consider:

- expected net edge
- risk
- symbol correlation
- sector concentration
- market concentration
- signal-family concentration
- existing positions

The objective is not to maximize the number of accepted signals.

---

# 19. Risk controls

Hard controls:

### Account
- starting capital: ₹2,00,000 research baseline
- maximum daily loss
- emergency stop

### Per trade
- maximum risk percentage
- maximum notional
- maximum stop distance
- maximum chase distance

### Portfolio
- maximum concurrent positions
- maximum sector exposure
- maximum single-symbol exposure
- correlated exposure cap

### Activity
- daily trade cap is a safety ceiling, not a quota
- cooldown after repeated failures in the same setup
- no forced re-entry

### Market conditions
- data-quality halt
- exchange/session halt
- extreme spread/illiquidity halt

---

# 20. Entry state machine

```text
CANDIDATE
  -> PRECHECK
  -> ECONOMIC_CHECK
  -> PORTFOLIO_CHECK
  -> ARMED
  -> EXECUTING
  -> OPEN
```

At any point:

```text
REJECTED
CANCELLED
EXPIRED
```

The system must record exactly why.

---

# 21. Exit architecture

Exits are independent research objects.

## Hard risk stop
Protect against catastrophic adverse moves.

## Structural invalidation
Exit when the condition that justified the trade is broken.

Examples:
- breakout level lost
- VWAP structure invalidated
- trend structure broken
- market/sector confirmation disappears

## Profit objective
A target can be derived from the distribution of expected favorable excursion rather than a universally fixed 1.5R rule.

## Time stop
Exit when the expected opportunity fails to materialize inside the forecast horizon.

## Emergency adverse-move cut
Test whether a fast adverse move predicts failure and whether earlier exit improves OOS expectancy.

Every exit must carry a reason code.

---

# 22. Exit research matrix

Test exit families independently:

```text
hard stop only
hard stop + fixed target
hard stop + structure exit
hard stop + time stop
hard stop + trailing logic
structure-first
MFE-based adaptive exit
```

Do not choose the best exit from the same data used to prove the alpha.

Exit selection must occur inside training/validation and remain frozen during the test fold.

---

# 23. Cost accounting

The trade ledger separates:

### Strategy P&L
Price movement before transaction costs.

### Mandatory costs
Brokerage and applicable statutory/exchange/transaction charges from the configured broker model.

### Slippage
Execution-model estimate/stress shown separately.

### Net before slippage

```text
strategy_pnl - mandatory_costs
```

### Net under slippage stress

```text
strategy_pnl - mandatory_costs - stress_slippage
```

Never subtract the same cost twice.

---

# 24. Slippage model

Start with explicit stress scenarios rather than pretending a single number is truth.

Recommended research scenarios:

```text
0 bps
2 bps
5 bps
10 bps
```

Once genuine quote/order-book information exists, calibrate slippage by:

- stock liquidity
- trade size
- time of day
- spread
- volatility
- order type

The engine should report the result across scenarios.

---

# 25. Research framework

## Step 1 — Universe construction
Build the eligible universe for each date using point-in-time membership where possible.

## Step 2 — Feature construction
Generate features causally.

## Step 3 — Candidate generation
Generate all plausible candidate families.

## Step 4 — Label construction
Create forward returns/MFE/MAE/barrier/time labels using the next executable reference.

## Step 5 — In-sample discovery
Search broadly for stable relationships.

## Step 6 — Validation
Select thresholds/models only on training + validation.

## Step 7 — Purged walk-forward OOS
No training leakage across fold boundaries.

## Step 8 — Regime analysis
Break results down by market/sector/session regime.

## Step 9 — Cost stress
Measure gross, net before slippage, and stress-net.

## Step 10 — Portfolio simulation
Deduplicate and enforce exposure/risk constraints.

## Step 11 — Blind test
Freeze everything and test once on untouched data.

## Step 12 — Paper execution
Use real-time conditions before production.

---

# 26. Required validation statistics

Every model/family must report:

### Core
- sample count
- mean return
- median return
- win rate
- profit factor
- expectancy
- standard deviation

### Risk
- max drawdown
- average drawdown
- worst trade
- worst session
- losing streak

### Opportunity
- average MFE
- median MFE
- average MAE
- median MAE
- target-hit rate
- stop-hit rate
- time-to-target distribution

### Economics
- gross P&L
- mandatory costs
- net before slippage
- slippage stress
- net after stress
- cost as % of gross P&L

### Stability
- per-fold performance
- per-regime performance
- per-session-phase performance
- per-symbol concentration
- per-sector concentration
- parameter sensitivity

---

# 27. Statistical stability gates

A strategy is not accepted because one aggregate number is positive.

Minimum research gate:

1. Positive OOS expectancy.
2. Positive OOS profit factor.
3. Net positive after mandatory costs.
4. Still economically acceptable under defined slippage stress.
5. No single fold or tiny trade cluster explains the result.
6. Results survive modest parameter perturbations.
7. No material single-symbol or single-sector dependency.
8. No material dependence on one exceptional day.
9. Sufficient sample size.
10. Untouched blind test remains positive after all rules are frozen.

If any material gate fails, status remains **RESEARCH ONLY**.

These are acceptance criteria, not assumptions that the strategy will pass.

---

# 28. Walk-forward design

Use expanding training windows with purge periods.

Generic structure:

```text
TRAIN -> PURGE -> VALIDATION -> PURGE -> TEST
```

Repeat chronologically.

The exact day counts can vary with data availability, but the final test set must never influence feature/threshold selection.

---

# 29. Anti-overfitting controls

Every experiment stores:

```text
experiment_id
feature_version
label_version
cost_model_version
universe_version
execution_model_version
strategy_version
hyperparameters
train_range
validation_range
test_range
random_seed
code_commit
```

This makes experiments reproducible.

Do not silently overwrite a previous experiment.

---

# 30. Ablation testing

For every promising model, run:

```text
all features
- market context
- sector context
- volume
- VWAP
- structure
- cross-sectional
- session-phase
```

Measure whether each block adds out-of-sample value.

If removing a feature block improves OOS results, the block is not automatically retained merely because it looks sophisticated.

---

# 31. Permutation / placebo tests

The research layer should include robustness tests such as:

- feature permutation
- shuffled candidate timestamps where appropriate
- label randomization sanity check
- symbol permutation checks
- regime permutation checks

A model that performs similarly on randomized labels is not useful alpha.

---

# 32. Leakage tests

Automated checks should fail the experiment when:

- future timestamps enter features
- normalization uses future observations
- ranking is calculated using future rows
- current universe membership is applied without explicit versioning
- future corporate events are visible before their information timestamp
- threshold selection touches test data

---

# 33. Execution engine

The execution loop should not scan the whole market wastefully while managing an open trade.

Separate processes/services:

```text
MARKET SCANNER
TRADE MANAGER
RISK MANAGER
EXECUTION MANAGER
DATA LOGGER
```

The scanner can update candidates while the trade manager focuses on open positions.

Latency targets should be measured rather than claimed.

Metrics:

```text
market event received -> feature update
feature update -> decision
 decision -> order submission
order submission -> acknowledgement
ack -> fill
```

Log p50/p95/p99 latency.

---

# 34. One-minute execution design

The system should not require a complete 5-minute candle to execute every trade.

Correct behavior:

```text
5m structure identifies opportunity
        |
        v
1m execution watches entry condition
        |
        +--> enter when timing condition is satisfied
        |
        +--> cancel when opportunity becomes late
```

This solves the conceptual conflict between:

- structural context, and
- timely execution.

The engine remains causally grounded in completed structural information but can execute on the 1-minute stream.

---

# 35. No 1-second fantasy

Do not create fake sub-minute precision from 1-minute or 5-minute candles.

If the data only tells us that the stock traded through a price somewhere during a minute, the backtest cannot claim exact intraminute fill order unless the execution assumptions explicitly model that ambiguity.

When tick/quote data is unavailable, report the limitation.

---

# 36. Candidate ranking

Do not rank solely on model probability.

A research ranking vector should include:

```text
expected_net_edge
edge_confidence
remaining_move
risk_to_opportunity
probability_target_before_stop
execution_quality
regime_match
correlation_penalty
```

Conceptually:

```text
trade_score =
    economic_edge
    * confidence
    * execution_quality
    * regime_fit
    / risk
    - correlation_penalty
```

The exact formula must be validated; the formula itself is not assumed to create alpha.

---

# 37. Candidate rejection taxonomy

Required standardized rejection codes:

```text
INSUFFICIENT_DATA
DATA_QUALITY_FAIL
OUTSIDE_SESSION
UNIVERSE_FAIL
REGIME_FAIL
NO_ALPHA
LOW_CONFIDENCE
LOW_REMAINING_MOVE
LATE_ENTRY
COST_NOT_CLEARED
SLIPPAGE_NOT_ACCEPTABLE
CHASE_LIMIT
RISK_LIMIT
DUPLICATE_EXPOSURE
SECTOR_LIMIT
CORRELATION_LIMIT
DAILY_LOSS_LIMIT
TRADE_LIMIT
EXECUTION_UNAVAILABLE
MODEL_NOT_AUTHORIZED
```

---

# 38. Trade lifecycle log

Every trade must have a complete event timeline.

```text
candidate_created
candidate_scored
economic_check
portfolio_check
armed
entry_signal
order_submitted
order_acknowledged
filled
stop_updated
target_updated
structure_changed
exit_signal
order_exit
filled_exit
finalized
```

Timestamps must be precise to the native data resolution.

---

# 39. Export architecture

The engine should export at least these files.

### `candidates.csv`
Every candidate before filtering.

### `decisions.csv`
Accepted/rejected decision with reason.

### `trades.csv`
Completed trades.

### `orders.csv`
Order-level execution history.

### `one_minute_market.csv`
Relevant 1m market observations used by replay/backtest where available.

### `five_minute_features.csv`
Every structural feature used in decisions.

### `model_predictions.csv`
All prediction outputs.

### `cost_ledger.csv`
Every cost component separately.

### `portfolio_state.csv`
Portfolio exposures through time.

### `risk_events.csv`
Every risk block/halt/limit activation.

### `experiment_manifest.json`
Exact model/data/code versions.

### `fold_results.csv`
Per-fold OOS metrics.

### `regime_results.csv`
Per-regime metrics.

### `daily_results.csv`
Day-by-day P&L and trade activity.

### `rejected_candidates.csv`
Full rejection reasons.

---

# 40. Crystal-clear trade record

Example schema:

```json
{
  "trade_id": "...",
  "timestamp_signal": "...",
  "timestamp_entry": "...",
  "timestamp_exit": "...",
  "symbol": "...",
  "exchange": "NSE",
  "direction": "BUY",
  "family": "compression_expansion",
  "market_regime": "...",
  "sector_regime": "...",
  "signal_score": 0.0,
  "p_up": 0.0,
  "expected_remaining_move_pct": 0.0,
  "expected_mfe_pct": 0.0,
  "expected_mae_pct": 0.0,
  "p_target_before_stop": 0.0,
  "expected_hold_bars": 0.0,
  "entry_price": 0.0,
  "exit_price": 0.0,
  "quantity": 0,
  "notional": 0.0,
  "gross_pnl": 0.0,
  "brokerage": 0.0,
  "taxes_and_fees": 0.0,
  "mandatory_cost_total": 0.0,
  "slippage": 0.0,
  "net_before_slippage": 0.0,
  "net_after_slippage": 0.0,
  "mfe_realized": 0.0,
  "mae_realized": 0.0,
  "hold_minutes": 0.0,
  "exit_reason": "...",
  "replay_or_live": "...",
  "model_version": "...",
  "cost_model_version": "...",
  "code_commit": "..."
}
```

---

# 41. Dashboard requirements

The website/UI retains the existing operational concepts but adds research transparency.

### Live screen
- market status
- open positions
- candidate stream
- entry score
- expected remaining move
- economic gate
- current risk
- P&L

### Candidate detail
Show:
- why candidate exists
- why direction chosen
- expected move
- cost hurdle
- rejection/acceptance reason
- regime state

### Trade detail
Show:
- original thesis
- actual path
- MFE / MAE
- exit reason
- gross P&L
- each cost component
- final net P&L

### Replay
- play/pause
- speed controls
- start/stop
- selected sessions
- market clock
- open trades
- candidate list
- downloadable raw/reconstructed research exports

### Research
- fold results
- family results
- regime results
- parameter robustness
- cost sensitivity
- blind test status

---

# 42. Daily research report

For each day:

```text
eligible symbols
symbols with quality failures
candidate count
accepted count
rejected count by reason
trade count
gross P&L
mandatory costs
net before slippage
slippage stress
net stress P&L
max intraday drawdown
best trade
worst trade
largest sector exposure
largest symbol exposure
```

This prevents a final monthly number from hiding catastrophic days.

---

# 43. Strategy-family report

For each family:

```text
candidate_count
accepted_count
trade_count
mean_oos_return
win_rate
PF
expectancy
MFE
MAE
cost_ratio
net_before_slippage
net_under_stress
fold_consistency
regime_dependency
```

A family is not promoted from a tiny positive sample.

---

# 44. The research database

Recommended tables:

```text
market_bars_1m
market_bars_5m
universe_history
sector_history
market_context
sector_context
features
candidates
predictions
orders
fills
trades
costs
portfolio_snapshots
risk_events
experiments
folds
model_versions
```

Each record carries a version/experiment identifier where appropriate.

---

# 45. Model governance

Model statuses:

```text
research_only
candidate
validated
production_authorized
retired
```

Only `production_authorized` models can generate live orders.

Promotion requires a recorded validation artifact.

Rollback must be instant.

---

# 46. Economic gate logic

Pseudo-flow:

```python
expected_gross = predicted_remaining_move * position_notional
mandatory = cost_model.round_trip(position)
hurdle = max(
    minimum_net_edge_rupees,
    mandatory * minimum_cost_multiple + mandatory
)

if expected_gross < hurdle:
    reject("COST_NOT_CLEARED")

expected_net = expected_gross - mandatory
stress_net = expected_net - slippage_stress

if stress_net < minimum_stress_net_edge:
    reject("SLIPPAGE_NOT_ACCEPTABLE")
```

The actual constants are configuration parameters that must be validated empirically.

---

# 47. Research target hierarchy

The engine should optimize in this order:

1. **Find a genuine predictive relationship.**
2. **Determine how much opportunity remains at entry.**
3. **Determine probability of favorable vs adverse path.**
4. **Choose an exit architecture appropriate to that distribution.**
5. **Verify economic viability after mandatory costs.**
6. **Stress-test execution.**
7. **Only then optimize portfolio allocation.**

Do not optimize portfolio sizing before alpha survives stages 1–5.

---

# 48. What we must NOT do in V3

Do not:

- add random indicators until P&L turns green;
- optimize directly on the final test set;
- use today's universe for historical periods without labeling it;
- manufacture 1m data from 5m and call it real;
- call gross P&L profit;
- bury brokerage/taxes inside a single opaque number;
- count slippage twice;
- require a minimum number of daily trades;
- select only the best few stocks before measuring the broader opportunity set;
- assume a 60–70% win rate is necessary or guaranteed;
- assume one winning strategy from three trades is statistically established;
- assume ML automatically creates alpha;
- tune exits after seeing the blind test;
- let an open position wait for a full market rescan before risk management acts.

---

# 49. Implementation phases

## Phase 1 — Foundation
- canonical data schema
- session clock
- data-quality engine
- point-in-time universe interface
- cost model
- experiment/versioning

## Phase 2 — Research engine
- 1m/5m feature pipeline
- regime engine
- independent candidate families
- complete labels
- candidate exports

## Phase 3 — Alpha research
- direction model
- remaining-move model
- MFE/MAE model
- time-to-event model
- family-by-family evaluation

## Phase 4 — Economics
- cost hurdle
- slippage scenarios
- liquidity checks
- late-entry gate

## Phase 5 — Portfolio/risk
- deduplication
- correlation control
- sizing
- daily risk
- exposure constraints

## Phase 6 — Execution
- 1m state machine
- order adapter
- stop/target/structure exits
- latency telemetry

## Phase 7 — Validation
- expanding walk-forward
- purged folds
- ablation
- placebo/permutation
- parameter robustness
- blind test

## Phase 8 — Operations
- live/paper/replay parity
- dashboards
- downloads
- model governance
- audit/replay

---

# 50. Production promotion gate

A V3 model can move to live only when the following artifact exists:

```text
model_version
training_range
validation_range
test_range
universe_definition
feature_hash
label_hash
cost_model_version
execution_model_version
fold_results
regime_results
cost_sensitivity
blind_test_results
paper_execution_results
```

And all required gates pass.

Otherwise:

```text
MODEL_NOT_AUTHORIZED
```

---

# 51. Final success definition

V3 is not considered successful because:

- the UI looks good,
- the model sounds intelligent,
- the backtest has a green chart,
- or gross strategy P&L is positive.

The system succeeds only when it can demonstrate, on unseen historical data and then in paper execution, that its accepted trades have sufficient net expectancy to survive transaction costs and execution stress while staying within risk limits.

The strongest possible system output on a bad day remains:

```text
NO TRADE

Reason:
No candidate cleared the required out-of-sample economic edge.
```

That is a valid and desired result.

---

# 52. Immediate V3 build order

1. Freeze the current research results as baseline.
2. Build the canonical 1m/5m data interface without inventing history.
3. Build the point-in-time universe/sector interfaces.
4. Implement the complete candidate ledger.
5. Implement MFE/MAE/barrier/time labels.
6. Implement regime vectors.
7. Run independent family research before any global ranking.
8. Build remaining-move and favorable/adverse excursion models.
9. Add dynamic economic filtering.
10. Add portfolio deduplication and correlation limits.
11. Add 1m entry timing.
12. Run walk-forward and blind tests.
13. Export every candidate, prediction, decision, cost and trade.
14. Only then consider production authorization.

---

## V3 principle in one sentence

> **Do not ask the market whether a stock is bullish; ask whether a specific setup still has enough statistically demonstrated, executable, net-positive opportunity left to justify risking capital right now.**
