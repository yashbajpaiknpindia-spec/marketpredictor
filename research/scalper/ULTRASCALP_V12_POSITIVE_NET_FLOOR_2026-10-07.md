# UltraScalp V12 — Positive-Net Floor Update

Date: 2026-10-07

## Change

The live/paper UltraScalp V12 objective no longer requires a +0.135% net profit target. The engine now targets **any strictly positive net opportunity after round-trip friction**.

Default measurement buffer: **+0.005% net**. This is a floor for numerical/UX clarity, not a profit ceiling; larger positive outcomes are accepted.

With the default 0.1363% round-trip friction and zero entry slippage:

- Minimum net buffer: +0.0050%
- Executable gross hurdle: +0.1413%
- A +0.1413% gross move leaves approximately +0.0050% net.
- Larger gross moves are equally valid; the engine does not require +0.2713% gross.

The live V12 remaining-edge gate is also reduced from 0.20% to +0.005% net remaining edge so the new objective is not defeated by a second oversized gate.

## Preserved

V12 1-bar latency, 30-minute safety timeout, 5-level displayed-depth data honesty, L5 thesis-failure/profit-lock exits, paper-only execution boundary, and the validated +0.10% synthetic benchmark are unchanged.
