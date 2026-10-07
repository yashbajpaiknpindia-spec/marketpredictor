# UltraScalp next phase — 2026-10-05

- Hypothesis: OHLCV scalper lacks microstructure/queue/order-flow information.
- Built a proxy pressure score from 1m return, 3m return, relative strength vs NIFTY, volume z-score, range z-score, candle close-location, VWAP distance.
- A fast labeler showed an apparently positive fade cluster, but this was rejected because the labeler could resolve simultaneous target/stop events incorrectly.
- First-touch validation on the strongest apparent cluster (test period, 6-stock cohort) was negative after 0.1363% round-trip friction:
  - q90, target 0.40%, stop 0.20%, hold 8: 4,439 trades, avg -0.12639%, sum -5.6104, win 23.81%
  - q95: 2,239 trades, avg -0.12448%, sum -2.7872, win 26.31%
  - q97.5: 1,167 trades, avg -0.12161%, sum -1.4192, win 28.36%
- Therefore the apparent positive result was a backtest artifact, not a discovered edge.
- Key next direction: microstructure-aware event engine using real tick/L2/order-book data if available; synthetic order-book data can validate code paths and stress tests only, never profitability claims.
