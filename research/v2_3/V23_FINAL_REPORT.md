# MarketPredictor V2.3 — Deep Edge-First Research Report

## Purpose
This version addresses the V2.2 failures by aligning research labels with the next executable bar, evaluating a large candidate universe before portfolio deduplication, separating signal alpha from exit logic, and enforcing an explicit economic hurdle.

## Verified historical corpus
- 61 NSE sessions: 2026-06-24 through 2026-09-18
- 225 symbols with data
- 1,029,375 five-minute rows
- Current workspace did not contain verified historical 1-minute bytes, so the numerical simulation below is **5-minute compatibility execution**. No synthetic 1-minute data was created.

## Walk-forward structure
Six expanding folds were used. Each fold has 24+ training days initially, a one-day purge before validation, 5 validation days, another one-day purge, and 5 unseen test days; the final fold covers 2026-09-11 through 2026-09-18.

## Model A — continuous regression
The model predicts signed forward return from the executable next-bar-open reference. Validation threshold is selected only on the validation window.

All six unseen folds remained negative after mandatory costs plus 5 bps stress:

| Fold | Test selected | Mean net % | Win rate | PF |
|---:|---:|---:|---:|---:|
| 1 | 1,968 | -0.1501% | 29.37% | 0.403 |
| 2 | 6,927 | -0.1547% | 27.04% | 0.333 |
| 3 | 5,951 | -0.1735% | 26.70% | 0.328 |
| 4 | 8,929 | -0.1073% | 34.11% | 0.468 |
| 5 | 625 | -0.1541% | 33.92% | 0.415 |
| 6 | 26,948 | -0.2121% | 22.94% | 0.245 |

Aggregate selected observations: **51,348**; mean net forward return **-0.1786%**, win rate **26.25%**, PF **0.304**.

## Model B — large-move probability classifier
A separate classifier tested whether the setup had a sufficiently large future move, combined with a direction model. It also failed all unseen folds. Aggregate: **3,732 observations**, mean net **-0.1468%**, win rate **38.72%**, PF **0.606**.

## Portfolio-style compatibility simulation
Using next 5m open execution, 1.0x ATR stop (bounded), 1.5R target, six-bar maximum hold, 3 concurrent positions and 4 trades/day:

### Continuous-model selected portfolio
- Trades: **120**
- Gross P&L: **₹2,845.61**
- Mandatory fees: **₹8,044.88**
- 5 bps slippage: **₹7,569.06**
- Net P&L: **₹-12,768.33**
- Net before slippage: **₹-5,199.27**
- Win rate: **35.83%**
- PF: **0.505**
- Max drawdown: **₹13,901.93**

### Large-move classifier selected portfolio
- Trades: **110**
- Gross P&L: **₹656.56**
- Mandatory fees: **₹7,272.87**
- 5 bps slippage: **₹6,843.33**
- Net P&L: **₹-13,459.64**
- Net before slippage: **₹-6,616.31**
- Win rate: **38.18%**
- PF: **0.531**
- Max drawdown: **₹16,404.82**

## Critical findings
1. The broad predictive model does not have positive unseen edge on this corpus.
2. The alternative “large move” target improves neither the unseen expectancy nor the economics enough to trade.
3. Gross opportunity is too small to pay mandatory round-trip costs in the portfolio simulations.
4. Slippage is **not** the primary cause: both portfolio variants are already negative before slippage.
5. The current historical archive is not sufficient to prove a genuine 1-minute execution edge because it contains 5-minute data only and lacks verified intraday market-context series.
6. Extreme-VWAP-reversion patterns show regime-dependent pockets of positive gross movement in this sample, but they are not stable enough across unseen folds to authorize production.

## Production rule
The V2.3 engine remains **production_authorized = false**. A strategy must demonstrate positive net edge after mandatory fees, pass multi-fold unseen validation, and pass a separate 5 bps execution stress before being eligible for production.

## Data honesty
The engine never fabricates 1-minute candles, point-in-time universe membership, sector history, bid/ask, ticks, order book, or pre-open data when they are unavailable.

## Next research requirement
The next decisive dataset upgrade is a genuine 1-minute historical archive with point-in-time universe/sector membership and reliable intraday NIFTY/VIX/sector context. The engine is already structured to consume that data without changing the signal semantics.
