# UltraScalp V12 Direction + Expected Edge Fix — 2026-10-08

## Major fixes
- Fixed silent prediction/index alignment bug: feature construction is ticker-sorted for causal rolling features, and model outputs are now restored to exact input row order before replay/execution.
- Direction/edge model is trained only on actual V12 candidate rows (`signal_direction != 0`), not arbitrary raw-direction rows.
- Expected-edge model evaluates the economically meaningful +0.20% net lock opportunity (0.3513% gross including 0.1363% friction + 0.015% slippage), while execution retains the +0.60% gross target and -0.18% hard stop.
- Model direction is selected by side-specific expected economic edge; probability is an explicit downstream gate.
- Default learned-model gate: P(target) >= 0.70, direction margin 0.02, expected edge >= 0.
- Immediate L5 thesis-flip is disabled for the learned-model blind policy. Once +0.20% net is established, a 0.30 percentage-point gross trailing profit lock protects the trade.
- Live-paper model defaults synchronized to 0.70 minimum target probability and 0 minimum expected edge.

## Chronological blind test
Dataset: 173,633 TRUE L5 snapshots, 221 tickers.
Split: first 60% train (104,179 rows), later 40% untouched blind (69,454 rows).

Policy:
- +0.60% gross target
- -0.18% hard stop
- 0.1363% round-trip friction
- 0.015% entry slippage reserve
- +0.20% net profit-lock activation
- 0.30 percentage-point gross trailing lock
- 30-minute horizon
- P(target) >= 0.70
- no L5-flip exit in learned-model policy

Blind result:
- 15 trades
- 6 wins / 9 losses
- 40.0% win rate
- +0.07793% average net/trade
- PF 1.3655
- +1.1690 percentage points aggregate per-trade net
- Avg winner +0.72784%
- Avg loser -0.35533%
- 3 target exits, 7 stops, 4 timeouts, 0 flip exits
- Mean predicted edge +0.12637%

Important: this is positive on the untouched blind sample, but only 15 trades. It is evidence of a positive result, not proof of durable live profitability. More independent L5 sessions are still required.

## Regression
Targeted V12 suite: 12 passed.
