# UltraScalp Synthetic L5 Edge Lab v2 — 2026-10-07

## Purpose
Build a controlled laboratory for testing whether the event-driven scalper architecture can discover conditional positive net edge before scarce TRUE_L5 live sessions are used. This is synthetic research only. It is not historical NSE data and it cannot authorize live trading.

## Market generator
- 25 separate market/event regimes, including `no_edge`, weak/moderate/strong edge, bull/bear trend, range/chop, acceleration/deceleration, volatility expansion/compression, reversal, false breakout, spoof-like imbalance, liquidity shock, open shock, midday drift, close acceleration, gap follow/fade, order-flow flip, thin book, wide spread and cross-current conditions.
- Five visible bid levels and five visible ask levels.
- Dynamic queue quantities with cancellations, replenishment and aggressive-flow depletion.
- Variable spread, depth, volume and volatility.
- Transient deceptive imbalance is present in selected regimes.
- Session parameters are randomized so every session is not the same market with a new random seed.
- Hidden latent flow is used only to generate the market. It is never a model feature.

## Model / execution protocol
1. Generate complete sessions.
2. Split sessions separately inside every regime into train, validation and blind test. This keeps every regime represented in the blind test without row-level leakage.
3. Fit a visible-only ridge edge model on train rows.
4. Tune the minimum edge/probability gates only on validation.
5. Freeze those gates and the model before the blind test.
6. Enter using only information available at the decision timestamp. Latency shifts execution to a later observable bar.
7. Charge round-trip friction, spread, entry slippage and adverse selection.
8. Exit when the observed edge disappears, protection is hit or the maximum hold is reached. There is no fixed 0.60% target assumption.
9. Report blind-test performance per regime and across 0–3 bar latency.
10. Check the explicit no-edge regime for unwanted participation.

## Evidence boundary
The application keeps `SYNTHETIC_L5_CONTROLLED_LAB_V2` separate from `TRUE_L5_REPLAY`, `EMPIRICAL_L5_PROXY` and `TRUE_L5_LIVE_PAPER`. Synthetic output is an architecture test. It is useful even when negative because it identifies weak feature, entry or exit logic.

## Build verification
- Python compilation of `app.py` and the synthetic lab passed.
- Browser inline JavaScript extracted from `templates/index.html` passed `node --check`.
- 38 targeted UltraScalp/scalper regression tests passed.
- Full pytest collection could not run in the build container because the container does not have `psycopg2` and `ijson` installed; these packages are present in the packaged `requirements.txt`.
- Deterministic synthetic smoke run (4 sessions/regime, 180 bars/session) completed in about 8.45 seconds: 25 train + 25 validation + 25 blind-test sessions; 258 blind-test trades; 43.41% win rate; +0.1848% average net/trade; PF 2.15. This is synthetic laboratory output only, not a market-performance claim.
- Latency smoke results remained positive in this controlled run at 0–3 bars, but this does not establish robustness on real NSE L5.

## Production boundary
No live/paper alpha was changed by these synthetic results. The next promotion step remains TRUE_L5_REPLAY / later walk-forward TRUE_L5 sessions.
