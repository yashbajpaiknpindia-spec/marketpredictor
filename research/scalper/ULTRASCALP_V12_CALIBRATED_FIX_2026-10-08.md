# UltraScalp V12 — calibrated probability/risk fix

Date: 2026-10-08

## Changes

- Legacy V12 confidence is explicitly labeled as a model score, never a probability.
- Added chronological, leakage-safe empirical calibration for `P(net-positive)`.
- Added an independent `P(adverse-stop)` probability gate.
- V12 paper/live entry requires a frozen calibration profile by default.
- Added UI controls for calibrated probability and adverse-risk thresholds.
- Added DB persistence for the calibration profile and calibration settings.
- Added stop first-observation forensics: expected stop, actual gross result, overshoot and metadata classification inputs.
- Preserved the positive-net objective: +0.005% is only the minimum numerical buffer; larger positive net outcomes remain valid.

## Blind test

Same 173,633-row real L5 dataset, chronological 60/40 split. Calibration was fit only on the first 104,179 rows and frozen before evaluating the final 69,454 rows.

At the configured 70% calibrated P(net-positive) gate: **0 trades**. This is intentional: the training calibration profile's observed P(net-positive) bins were only about 12.2%–17.0%, so accepting 70% would be unsupported by the evidence.

A separate 15% sensitivity run (not used to tune the official blind result) produced 591 trades, 29.10% win rate, PF 0.3166 and average net -0.1502%. Therefore lowering the gate merely to manufacture trade count is not justified.

Stop diagnostics in that sensitivity run: 137 stop exits; mean stop overshoot 0.2813 percentage points beyond the nominal 0.22% price barrier. This is recorded as an execution/data-cadence diagnostic rather than hidden inside the stop-loss statistic.
