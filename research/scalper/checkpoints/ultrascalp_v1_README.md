# MarketPredictor UltraScalp v1

This is a separate research engine for very-short-horizon, event-driven scalping.

## Core idea

No named strategy is required. The engine looks for a **fresh market event**:

- unusually fast short-term price movement
- acceleration rather than drift
- abnormal volume participation
- range expansion
- fresh local breakout
- relative strength/weakness versus the market
- optional sector/VIX confirmation

It then produces an **edge score** (expected net return estimate), not a probability. A trade is considered only when the event is strong and the learned edge score clears the economic hurdle.

## Execution contract

- Entry: next 1-minute open after the trigger bar.
- Target: +0.25% by default.
- Hard protection: -0.10% by default.
- Maximum holding time: 5 bars.
- Fast thesis-failure exit: 2 bars without minimum favourable progress.
- Intrabar ambiguity: stop wins when stop and target both touch in the same bar.
- Costs: 0.1063% mandatory + 0.03% slippage baseline; the stress hurdle remains 0.1663% for stricter research promotion.

## No-lookahead design

Features use only current and past data. Labels use future bars only after features are frozen. Weekly walk-forward training uses only weeks strictly before the test week.

## Files

- `ultra_scalper_v1.py`: engine, causal feature builder, event trigger, execution/label logic, edge-score model.
- `run_weekly_walkforward.py`: rolling weekly training/testing harness.

## Research policy

The model is intentionally **not authorized for live trading**. The goal is to test whether an aggressive, event-driven scalp process survives chronological out-of-sample testing after realistic costs.
