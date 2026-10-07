# UltraScalp V12 Live-Paper Settings

## Scope
This release adds the validated V12 execution policy to the existing UltraScalp live/paper worker and UI only. Existing MarketPredictor strategy/replay/data code is not replaced.

## Live V12 policy
- Net target: **+0.135%** after configured round-trip friction.
- Round-trip friction: **0.1363%**.
- Entry slippage: **0.0000%** default.
- Executable gross target under the current accounting: **+0.2713%**.
- Entry latency stress/candidate: **1 bar** in the benchmark; live REST execution remains timestamped from the actual provider poll.
- Safety timeout: **30 minutes**.
- Target-first exit is retained.
- Price-risk protection remains the existing live baseline **0.22%**.
- Profit lock is **thesis-based after the trade is net-positive**: L5 pressure weakening can bank the gain rather than waiting for an arbitrary timeout.
- Research probability candidate: **70%** is displayed as a research threshold only; the current live scorer is not a calibrated probability model, so this value is not silently misrepresented as one.
- Real live feed remains **5-level displayed depth**; OFI remains a proxy, not event-level historical order flow.
- Paper-only boundary remains enforced. No broker order-placement path is added.

## Validated V12 benchmark (separate from live target)
30 stocks × 9 original synthetic regimes × 3 fresh blind sessions = **810 stock-session streams**, 390 one-minute bars/session, L5-style book, one-bar latency, 0.1363% friction, +0.10% net target, event-driven exits, and no fixed 10-minute exit.

- Total trades: **27,994**
- Weighted average net/trade: **+0.0392%**
- Bull trend: **+0.1146% avg net, PF 3.00**
- Bear trend: **+0.1197% avg net, PF 3.28**
- False breakout: **+0.0363% avg net, PF 1.36**
- Main weak families: weak edge **−0.1240%**, reversal **−0.0670%**, spoof imbalance **−0.1916%**, liquidity shock **−0.1455%**.

The requested **+0.135% live setting is therefore a new paper/research candidate, not a claimed validated production edge**.
