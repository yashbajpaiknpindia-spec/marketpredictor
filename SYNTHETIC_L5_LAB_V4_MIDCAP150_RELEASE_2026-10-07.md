# UltraScalp Synthetic L5 Causal Market Lab — Midcap150 build

## What this build guarantees
- Synthetic evidence remains isolated from TRUE_L5 replay and live-paper evidence.
- The configured stock universe is the complete NIFTY Midcap 150 universe resolved through the application's NSE-index constituent path.
- The API refuses to start a `midcap150` run if it cannot resolve exactly 150 constituents.
- Coverage is deterministic and batched for Render safety. Batch 0/1/2 with a 50-stock batch size covers all 150 constituents.
- Five displayed bid/ask levels are generated for every synthetic stream.
- Queue quantities change through cancellation, replenishment, aggressive depletion and replenishment.
- Spread, liquidity, volatility, volume and stock-specific behaviour vary by stream.
- Cross-sectional market-factor correlation is present; stock-specific noise remains separate.
- Event, spoof/deceptive-liquidity and shock probabilities are actual generator controls.
- 30 regimes include no-edge/dead-zone, trend, range/chop, reversal, false breakout, spoof, liquidity shock, open/midday/close, gap, order-flow flip, thin/wide spread, cross-current and news/shock conditions.
- Hidden latent state and true future return are never part of the model feature contract.
- Train, validation and blind test are separated by whole sessions.
- Entry uses positive remaining net edge after round-trip friction, spread, entry slippage and adverse selection; there is no fixed +0.60% target.
- Exit occurs when observed model edge disappears, protection is hit or the time budget expires.
- Latency robustness is checked separately and is sampled deterministically for bounded runtime.

## Important evidence boundary
This is a synthetic causal laboratory. A positive synthetic result is an architecture test, not evidence of profitable NSE trading. Promotion remains gated by TRUE_L5 replay and later live-paper evidence.

## Build verification
- `py_compile` passed for `app.py` and `research/scalper/synthetic_l5_lab_v3.py`.
- 13 targeted static regression tests passed.
- Three inline JavaScript blocks passed `node --check`.
- Small causal smoke experiment passed with all 30 regimes represented in train/validation/blind test.
- Small 150-universe scale smoke confirmed the configured universe can carry 150 symbols while using deterministic coverage batching.
