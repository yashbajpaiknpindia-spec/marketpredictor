# UltraScalp V12 Economic Target + Hard Loss Gate Fix — 2026-10-08

## Why this change
The prior forensic build correctly made calibration advisory, but it still allowed the replay to treat cost-clearing +0.005% as the executable target. That is a research hurdle, not a sensible trading objective.

## Frozen policy for this build
- Gross target: **+0.60%**
- Round-trip friction: **0.1363%**
- Slippage reserve: **0.015%**
- Net target: **+0.4487%**
- Hard price-loss gate: **-0.18% gross**
- Planned net loss including friction/slippage: **-0.3313%**
- For ₹25,000 notional: approximately **+₹112.18 target** vs **-₹82.83 planned maximum loss**.
- Economic entry gate: modelled remaining edge must support the full +0.4487% net target; positive-after-friction alone is insufficient.
- Profit-lock weakening exit cannot arm until net profit is at least +0.20%.
- Hard loss check is evaluated before L5 thesis-flip/profit-lock exits.

This is a risk/economic-policy correction, not a claim that V12 has positive edge. Directional prediction remains the largest performance problem shown by the chronological audit.

## Same frozen blind test after the policy fix
- Chronological split: 60% train / 40% blind.
- Train rows: 104,179; blind rows: 69,454.
- Blind trades: **279**.
- Win rate: **27.96%**.
- Profit factor: **0.6025**.
- Average net/trade: **-0.10056%**.
- Average winner: **+0.54516%** net.
- Average loser: **-0.35114%** net.
- Stop exits: **93**.
- Stop overshoot is reported rather than hidden; average observed stop overshoot was 0.2519 percentage points because snapshots can jump through the stop between observations.

The result is still negative. The policy therefore should not be presented as production-profitable. The next model change should target directional prediction / entry quality, not loosen the loss gate or return to a +0.005% target.
