# MarketPredictor UltraScalp Public-Model Research Checkpoint

Dataset: real MarketPredictor 1-minute NSE archive, 2026-06-01 to 2026-09-25.
Initial broad public-family comparison was frozen to a six-stock high-activity cohort across the full available history to keep the experiment reproducible in the runtime.

All reported net figures use the same 0.1363 percentage-point round-trip friction assumption used in this research run.

## Results
- Fixed-target public baselines (ORB5, ORB15, momentum, VWAP momentum), target 0.25%, stop 0.10%, 5-bar max: all negative. Best aggregate avg net/trade about -0.1373% (ORB5/ORB15 range).
- Public-style abnormal-demand momentum with dynamic trailing: 5,888 trades, 24.95% wins, -0.148355% average net/trade.
- Public-style short-horizon mean reversion/VWAP fade: 318 trades, 33.33% wins, -0.120187% average net/trade.
- Momentum + NIFTY confirmation + dynamic trailing: 300 trades, 26.33% wins, -0.151961% average net/trade.

These are research diagnostics, not claims about live profitability.
