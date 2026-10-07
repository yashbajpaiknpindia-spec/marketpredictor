# UltraScalp V12 — Production UI / Telemetry / Polling Fix — 2026-10-08

## Main-page changes
- Removed the Synthetic L5 Laboratory controls, synthetic run button, synthetic polling hooks, synthetic regime table, and synthetic benchmark card from the main UltraScalp Engine page.
- Main UltraScalp workspace is now real-data / TRUE_L5 / paper-first.
- Synthetic laboratory backend remains packaged for research compatibility but is not surfaced or polled by the production page.

## Live V12 policy wiring
- Execution target: +0.60% gross.
- Economic profit-lock activation: +0.20% net.
- Profit-lock trail: 0.30 percentage points gross.
- Hard stop: -0.18% gross.
- Round-trip friction reserve: 0.1363%.
- Entry slippage reserve: 0.015%.
- Safety timeout: 30 minutes.
- Frozen V12 target-probability gate: >= 70%.
- Minimum model expected net edge: 0.0%.
- Frozen V12 model is enabled by default for live-paper capture.

## Telemetry / data-integrity fixes
The live worker now reports:
- total poll duration
- provider/API latency
- database persistence latency
- requested vs returned quote coverage
- cumulative L5 coverage
- poll cadence overruns
- invalid/unusual data count
- data-quality status and warning list
- latency status and warning list
- economic-lock activations/exits

The worker continues the configured cadence without adding a full interval after a slow provider call, while explicitly reporting when the provider/DB cycle overruns the requested cadence.

## Bug fixes
- Persisted settings schema now contains the V12 model/threshold/economic-lock fields.
- Legacy 0.005/0.135/0.4487 target values are migrated to the current 0.20% economic-lock policy where applicable.
- Existing V12 sessions using the old 10-minute default are migrated to the validated 30-minute safety timeout when still on V12 mode.
- Main-page JavaScript no longer starts synthetic polling when opening the Scalper tab.
- Main-page scan statistics now come from the live worker only.
- Removed disconnected legacy engine-control widgets that could display settings not actually used by the live worker.
- Added regression coverage for the production-first UI and live telemetry.

## Validation
- Targeted UltraScalp/live-paper/UI tests: **34 passed**.
- Extracted browser JavaScript: `node --check` passed.
- Main-page synthetic controls/poll hooks: confirmed absent.
- HTML duplicate-ID check: no real duplicate IDs introduced.

Note: a broader test subset that imports `app.py` could not run in the current test environment because Flask is not installed there. This is an environment dependency issue, not a reported application regression.
