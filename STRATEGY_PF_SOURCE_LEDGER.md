# MarketPredictor — Current 20-Strategy Published PF Ledger

These are **external published claims/benchmarks**, not MarketPredictor results. “Claimed PF” is recorded only where a source explicitly reports a Profit Factor. A dash means no directly comparable PF was located in the source set used for this release.

| # | Strategy | Claimed PF | Source / scope | Comparability |
|---|---|---:|---|---|
| 1 | Gap Fade Mean Reversion | 1.31 | CuriousObservator; 11.2y, 8,079 trades, 118 NSE equities, daily OHLC, 20 bps round-trip | Closest concept; exact gap thresholds/filters are not fully disclosed |
| 2 | Gap Fade + Market Filter | — | No standalone published PF for this exact MarketPredictor variant | Our overlay; compare only with our OOS result |
| 3 | Gap Fade + Volume Confirmation | — | No standalone published PF for this exact variant | Our overlay; compare only with our OOS result |
| 4 | ORB 15-Minute | 1.60 | Backx broad 15–30 minute ORB on NIFTY 50 + Bank Nifty | Broad ORB benchmark, not exact stock-intraday implementation |
| 5 | ORB 30-Minute | 1.60 | Backx broad 15–30 minute ORB on NIFTY 50 + Bank Nifty | Broad ORB benchmark, not exact stock-intraday implementation |
| 6 | ORB + Volume + Market Confirmation | 2.75 (IS) | Dhan NIFTY Algo Trading Lab; best full-sample ORB name | Explicitly collapsed OOS (field PF 0.07); not a validated edge |
| 7 | VWAP Mean Reversion | 1.24 | Kitefacts “VWAP Reversal” comparison | Generic platform benchmark; rules not fully disclosed |
| 8 | VWAP Trend Continuation | 1.66 | Kitefacts “VWAP Continuation” comparison | Generic platform benchmark; rules not fully disclosed |
| 9 | EMA + RSI Momentum | 2.75 (related EMA cross) | AyushChangedia; EMA Cross PF 2.75 from 3 trades | Related benchmark only; 3-trade sample is not robust evidence |
| 10 | EMA Trend Pullback | 1.64 | DailyBulls 20 EMA Pullback; 458 trades | Daily-style basket, not exact intraday rules |
| 11 | Supertrend + ATR Trend | 2.40 | DailyBulls selected weekly-filtered daily Supertrend + 2.5 ATR trail; 127 trades | Daily/weekly long-only benchmark |
| 12 | Bollinger Mean Reversion | 0.92 | AyushChangedia Indian study | Related benchmark; exact implementation/sample differ |
| 13 | Bollinger Squeeze Breakout | — | No directly comparable published PF located | No external PF claim |
| 14 | Previous-Day High/Low Breakout | — | No directly comparable published PF located | No external PF claim |
| 15 | Donchian Breakout | — | No directly comparable published PF located | No external PF claim |
| 16 | ATR Expansion Breakout | — | No directly comparable published PF located | No external PF claim |
| 17 | ADX + DI Trend | — | No directly comparable published Indian PF located for this exact implementation | No external PF claim |
| 18 | 52-Week Breakout + Volume | 3.61 (self-reported) | Adarsh Gautam Jha; ~10,600 simulated trades, ~2000–2026 weekly backtester | Self-reported, weekly, not independently verified, not intraday |
| 19 | Relative-Strength Breakout | 1.59 | Kitefacts generic comparison | Generic platform benchmark; rules not fully disclosed |
| 20 | Cross-Sectional Momentum + Hysteresis | — | Harsh Pandhe SSRN: after-cost CAGR 45.12%, Sharpe 1.849, MDD -25.41% at B=60 | No PF reported; monthly Nifty 500 research, not intraday |

## Primary source URLs

- https://github.com/CuriousObservator/Systematic-Intraday-Mean-reversion-Strategy
- https://backx.in/blog/opening-range-breakout-strategy
- https://github.com/builditwithgk/dhan-nifty-algo-trading-lab
- https://kitefacts.com/
- https://github.com/AyushChangedia/market-strategy
- https://dailybulls.in/20-ema-pullback-strategy-backtest/
- https://dailybulls.in/supertrend-strategy-backtest/
- https://in.linkedin.com/in/adarsh-gautam-jha-94a2aa36
- https://papers.ssrn.com/sol3/papers.cfm?abstract_id=7389238

## Comparison rule for MarketPredictor

The external PF is a benchmark only. The later MarketPredictor Replay result must be reported separately with its own universe, dates, costs, slippage, execution policy, OOS split and trade count. No external PF is copied into the selector or used as a gate.
