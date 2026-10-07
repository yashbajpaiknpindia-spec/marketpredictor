# Rapid Scan — Simplified Strategy Evidence UI

Updated from the v3.4.32.5 Rapid Strategy Forensics v5 package.

## What changed
- Replaced the wide 15-column Strategy Decision table with one simple card per strategy.
- Every card shows:
  - Signals, win rate, profit factor, average net per signal
  - Evaluation label and explanation
  - Reached selected profit marker
  - Profit-first / stop-first
  - Recovered after stop / never recovered
  - Stop-first then later +1% / +2%
  - Profit reached then finished loss
  - Pure loss
  - MFE, MAE, reversal rate and evaluation errors as secondary details
- Marker labels are dynamic and use the actual Rapid Scan run configuration, so the card can say e.g. `−0.125% / +0.40%` or whatever was configured for that run.
- A short reading guide explains the denominators so conditional percentages are not mistaken for overall percentages.
- The old technical table is still available under a collapsed `Show technical table` section for debugging/export validation.
- The old duplicate Recovery Diagnosis table is hidden from the normal user view.

## Validation
- Python syntax checks passed for app.py, rapid_forensics.py and strategies.py.
- Inline JavaScript syntax check passed.
- 15 existing UI/static tests passed.
