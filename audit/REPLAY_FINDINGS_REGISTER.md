# Replay Findings Register — Source-of-Truth Decisions

This is the compact operational register for Replay-driven fixes.

| Finding | Evidence | Applied fix | Release |
|---|---|---|---|
| Run headline return could be wildly misleading | Replay result/report fields used summed per-trade % | Account return = `(ending capital-starting capital)/starting capital` | v3.4.13 |
| Bare export symbols did not always match `.NS` replay symbols | Export ZIP filenames were bare NSE symbols | Normalize/match both forms before provider fetch | v3.4.13 |
| Profit protection could wait for target-distance logic | Replay #23 losing trades often reached +0.4% to +0.6%+ MFE | Absolute +0.40% protection; +0.60/+0.80 lock progression; runner above +0.80% | v3.4.13 |
| Trailing-stop intrabar conflict could ignore armed trailing stop | Replay exit evidence | Effective stop includes stored trailing stop; conservative stop-first handling retained | v3.4.13 |
| Stopped 5-day run could look like a 5-day result | Replay #23 selected 5 dates but ran only 1 | Persist/display selected vs actual replayed dates | v3.4.13 |
| Run settings could be lost from forensic export | Replay #23 `settings_snapshot` was null | Persist exact run/universe/settings snapshots | v3.4.13 |
| Per-strategy PF was unavailable/trustworthy only in theory | Replay #23 had 23/23 trades with null strategy attribution | Persist strategy attribution and compute per-strategy PF from net P&L | v3.4.14 |
| Zero-trade strategies disappeared from the report | Empty strategy audit/performance artifacts | Always emit all 20 strategies with explicit UNPROVEN/NO_TRADES_RECORDED | v3.4.14 |
| Replay data preflight could feel stalled | Replay #24 UI sat at CHECKING STORED DATA | Non-blocking preflight + short timeout + explicit status states | v3.4.15 |
| Stored-data lookup was potentially N+1 and slow | Repeated ticker-level DB access path | Bulk ticker/day cache lookup | v3.4.15 |
| Export ZIP hydration could be write-heavy | Per ticker-day DB writes | Batch candle-cache persistence | v3.4.15 |
| Research Lab / Trading Automation cluttered the normal workflow | Visible top-level tabs | Keep behind `⋯ More` | v3.4.11 / v3.4.15 |
| Theme felt inconsistent | No explicit persisted theme control | Persisted Light/Dark toggle | v3.4.15 |

## Non-negotiable forensic rule

Historical runs must never receive invented strategy attribution. If the artifact does not preserve the strategy decision identity, the report must show an attribution gap rather than reverse-engineer or guess which strategy produced a trade.

## Replay #23 account-level facts

- Starting capital: ₹200,000.00
- Ending capital: ₹199,286.34
- Net P&L: -₹713.66
- Account return: -0.3568%
- Trades: 23
- Wins / losses: 9 / 14
- Costs: ₹920.00
- Gross P&L: +₹206.34
- Actual replayed date: 2026-09-21
- Selected dates: 2026-09-21 through 2026-09-25
- Strategy attribution coverage: 0%

## Interpretation

Replay #23 demonstrated a thin positive gross result that was more than absorbed by transaction costs. It also demonstrated that exit handling and attribution needed to be measured separately from strategy-entry quality. This is why subsequent versions changed the execution layer/reporting layer without changing the 20 strategy definitions.

| New finding — Replay download can kill the web worker on large/new runs | Replay #24 exposed 1,356 replay-cache ticker-days; prior downloader materialized the candle cache as DB `fetchall()` → Python list → JSONL string → in-memory ZIP → `getvalue()` | Stream candle rows with a named PostgreSQL cursor in small batches directly into a disk-backed ZIP; keep the large candle payload out of the Python object graph; retain the full candle artifact in the ZIP | v3.4.16 |

## Replay 24 — v3.4.17

Normal RAW replay bypassed strategy attachment (27/27 unattributed trades; zero strategy evaluations). Connected the evaluator, expanded independent candidates, enforced attribution, enabled configured RAW risk budgets, synchronized sizing capital, corrected bar-based trailing/short exits and stop ceiling, and added measured View Replay results with checkpoint persistence. See `REPLAY_24_DIAGNOSIS_AND_FIX.md` for evidence, limits and validation. Original strategies remain byte-identical. Replay 24 remains historical invalid strategy evidence; no profitable rerun is claimed.
