# UltraScalp V12 Forensic Fix — 2026-10-08

## Why this build changed
The prior calibration build used a hard 70% calibrated P(net-positive) gate. On the real 173,633-row L5 session that produced zero candidates. A candidate-by-candidate audit showed that this was unsafe: the dataset contained substantial favorable excursions and the calibration itself had a direction-labeling flaw for short signals.

## Verified findings
- 173,633 real L5 snapshots, 221 stocks.
- 2,442 V12 candidate observations in the forensic candidate universe.
- 658 candidates subsequently reached >= +0.25% in V12's signal direction within the 5-minute diagnostic horizon.
- 98 were specifically +0.25% to < +0.30%; 560 were >= +0.30%.
- 1,833 had >= +0.005% favorable movement.
- 1,290/2,442 candidates had the larger immediate 5-minute move opposite the V12 signal direction.
- 673 hindsight-favorable candidates were exited by the one-snapshot L5 thesis-flip rule before their favorable excursion was captured.

## Fixes implemented
1. Calibration labels are now direction-aware: future return is multiplied by the signal direction.
2. Calibration is fit on non-zero signal candidates rather than neutral/no-signal observations.
3. Positive outcome means gross signal-direction return strictly exceeds round-trip friction plus the 0.005% measurement buffer; this is not a fixed profit target.
4. Calibration is now **advisory by default**. It records P(net-positive) and P(adverse-stop), but cannot erase a valid V12 candidate unless an explicit research gate is enabled.
5. The UI now labels calibration as advisory and defaults the gate to disabled.
6. The replay blind-test helper uses the same candidate-preserving behavior by default.
7. The existing L5 thesis-flip exit was NOT changed because 2- and 3-snapshot persistence improved target capture but did not improve overall expectancy on this dataset. It remains a separate research item.

## Same-dataset blind rerun after fix
Chronological split: 60% training / 40% blind.

- Training rows: 104,179
- Blind rows: 69,454
- Training candidates: 728
- Blind candidates retained: 1,714
- Calibrated blind P(net-positive) range: 4.76% to 36.36%
- Candidates at >=70% calibrated probability: 0
- Simulated trades: 561
- Win rate: 28.70%
- Average net/trade: -0.10796%
- Sum of per-trade net percentages: -60.56 percentage points (not a portfolio return)
- Profit factor: 0.4295
- Exits: 160 target, 95 stop, 261 L5 thesis-flip, 45 timeout

## Interpretation
The fix solved the zero-candidate failure and corrected the calibration label semantics, but it does NOT establish positive V12 edge on this single session. The blind result remains negative. That is intentionally preserved as evidence.

The next model work should focus on directional prediction and the expected-edge estimator, while retaining the candidate-preserving calibration architecture. Do not lower/raise thresholds based on this blind result.
