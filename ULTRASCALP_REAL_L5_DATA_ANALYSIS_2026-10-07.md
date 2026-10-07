# Real L5 Data Analysis — 2026-10-07

Source files supplied by the user:
- `scalper_l5_snapshots (4).csv`
- `scalper_paper_trades (1).csv`

Observed:
- 100,261 snapshots
- 221 instruments
- 873 capture timestamps
- 512 timestamps contained 40 instruments (early part of the capture)
- 361 timestamps contained 221 instruments
- 99.4235% of snapshots passed the L5 validity gate
- 179 paper trades; 157 closed; 22 open in the supplied trade export
- Closed-trade average gross +0.01691%, average net -0.11939%, net win rate 28.03%, PF 0.334
- Closed exits: 106 TIME_EXIT, 43 STOP, 8 TARGET
- The paper process reached 79 concurrent positions because unlimited mode had no capital constraint.
- Mean expected move for closed trades was ~0.5564% while mean absolute realized gross move was ~0.2051%, so the heuristic was about 2.71x larger than realized absolute movement in this sample.
- L5 snapshot features showed weak short-horizon predictive relationships in this session; this does not establish that L5 is useless generally, only that this captured sample does not validate the old fixed target/heuristic edge.

Interpretation:
- The L5 feed and parser are now functioning; this session is not a zero-depth failure.
- The weak paper result is therefore an alpha/calibration/latency problem, not a missing-data problem.
- The current REST collection is too slow for a 221-name high-frequency microstructure scan. The true recorded timestamps show multi-second cycles in the 221-name portion.
- Backtesting must therefore preserve evidence class and observed latency. A 50-level synthetic book should not be presented as historical truth.
