# Replay 24: diagnosis and v3.4.17 repairs

## What the supplied evidence establishes

| Metric | Replay 23 | Replay 24 |
|---|---:|---:|
| Closed trades | 23 | 27 |
| Wins / losses | 9 / 14 | 3 / 24 |
| Gross P&L | ₹206.34 | -₹1,609.77 |
| Recorded costs | ₹920.00 | ₹1,080.00 |
| Net P&L | -₹713.66 | -₹2,689.77 |
| Return on ₹200,000 | -0.3568% | -1.3449% |
| Strategy attribution | 0% | 0% |
| Average closed-trade entry value | ₹11,865.59 | ₹11,749.17 |

Replay 24 was stopped at 10:38 on September 21: only 4.5% progress across five selected dates, with 11 positions still open. Its closed-trade P&L is not a completed five-day return or a final marked-to-market account valuation. The difference from Replay 23 is descriptive, not a controlled comparison of strategies or exit policies.

## Why the result cannot judge the 20 strategies

`fetch_intraday_candidate_snapshot` now builds factual market data only. The normal replay builder still treated its return as a finished strategy candidate: it never called `attach_intraday_strategy_engine`. RAW removed the generic score gate, allowing these unscored, unattributed stock rows through to the trade planner. Consequently all 27 closed trades have blank strategy IDs/names, scores and regimes, and every strategy evaluation counter is zero. These are not merely missing display labels: the supplied normal-replay source bypassed the strategy evaluator.

The old strategy table exposed published reference profit factors and audit counts. It did not show the measured per-strategy trade count, net PF and net P&L together. Zero audit counters were accurate evidence of a broken invocation path, not proof that all strategies had no opportunities.

## Were the earlier improvements applied?

Some were present: the forensic download includes complete settings, integrity diagnostics, selected versus replayed dates, account return and candle data. The snapshot records the newer absolute protection settings. However:

- Attribution storage/reporting existed downstream, but there was no strategy decision upstream to record.
- Risk budget was configured enabled yet runtime statistics show disabled: initialization incorrectly required learning to be enabled. RAW explicitly disables learning.
- The bar conflict code tested the effective trailing stop but used the original stop as the exit price.
- The exit snapshot supplied the entire session's highs/lows, which contaminated position MFE/MAE and could arm trailing protection using movement before entry.
- The configured stop ceiling could still expand up to 2.5 times through opening-range sizing; 17 Replay 24 trades exited at hard stops.
- The stated account had ₹200,000 but sizing read ₹50,000 from Trading Automation. The settings snapshot proves both values.

Seven give-back exits totaled -₹273.89 net despite ₹6.11 gross. Seventeen hard-stop exits totaled -₹2,546.95 net. These describe the flawed run; they cannot establish how an authentic strategy portfolio would have performed. The export does not preserve a source commit hash, so its exact deployed source identity cannot be independently proved from the ZIP. Its observed failures are consistent with the supplied source.

## Changes in v3.4.17

1. Connect replay snapshots to the existing evaluator using only the historical window available at the signal time.
2. Expand independent strategy candidates; never execute the parent stock snapshot. Remove the eight-candidate display limit from independent signal execution. Shared capital, ticker occupancy, cooldown and trade-count constraints still govern actual entries; use isolated tournament ledgers for unconstrained per-strategy research.
3. Reject missing strategy identity and surface evaluator failures. Persist the strategy identity from entry through exit; preserve historical missing IDs as missing.
4. Keep risk budgeting active in RAW when configured. Synchronize recorded replay account capital and sizing; forward protection settings to the exit evaluator.
5. Enforce the configured stop ceiling without editing the strategy definitions. Original strategy target/stop requests continue through the existing central execution planner; this is a portfolio/execution test, not a promise of untouched standalone strategy exits.
6. Resolve long and short stop/target touches correctly. Fill existing trailing stops at their effective levels; adverse gaps fill at the worse bar open. Both-touched bars remain conservative stop-first.
7. Feed current-bar extrema to position-excursion tracking while retaining historical session context. Newly armed trails are not retrospectively assumed to fill earlier in the same candle. MFE/MAE remain candle-resolution bounds: order within the exit candle is unknown.
8. Apply adverse slippage in the correct direction for shorts; enforce fill-gap limits in either direction.
9. Persist full-run strategy measurements at existing progress checkpoints. View Replay shows evaluated, eligible, actual entries, closed trades, win rate, measured net PF and net P&L, alongside separately labeled published PF. Short strategy IDs have their own rows. Historical attribution gaps suppress unsupported automatic strategy conclusions.
10. Fail visibly on trade-persistence errors. Record execution build `v3.4.17-replay24-audit` in new run settings. Reloaded stage counts derive from persisted audit data.

## Why the ZIP grew

Replay 24 contains `market_candles.jsonl`: 32,759,915 bytes uncompressed, 5,871,601 bytes compressed. Replay 23's bundled forensic ZIP did not contain that dataset. This explains most of the new 6 MB forensic archive; it is not evidence of larger orders. The application ZIP also carries historical research CSVs and documentation. Those research/history files are retained; disposable bytecode is omitted from this release.

## Validation performed

- Five behavioral regression groups passed: long/short exits including gaps and both-touch bars; refusal of unattributed snapshots; actual strategy scan and future-candle isolation; concurrent/daily budget accounting and historical attribution gaps; maximum stop ceiling under a wide opening range.
- Real Replay 24 candle smoke test: 800 strategy-side evaluations, 14 eligible candidates across six strategy-side IDs. This checks invocation and attribution, not portfolio returns. No missing data was fabricated.
- Existing four download-memory-safety checks passed.
- Python compilation and browser JavaScript syntax checks passed. The measured-results renderer was exercised against attributed and historical-gap examples.
- `strategies.py` is byte-for-byte unchanged. SHA-256: `e444961282d651bb6a3981647a2ad2e71ce37026735c75c95713580b8c05b7e5`.

No connected production database, Render deployment or full new five-day replay was exercised here. The new run remains necessary. Existing missing reference inputs remain subject to each strategy's own unavailable-input handling; these checks do not certify every strategy's data sufficiency or profitability.

## Next replay

Deploy the complete updated source, preserving the existing database and environment. Run RAW, learning off, same dates/universe, ₹200,000, capital enforcement and risk budgeting enabled. Note that 25% maximum position value now refers to ₹200,000 (₹50,000 maximum), not the mismatched old ₹50,000 account (₹12,500 maximum). The risk budget and strategy stop distance can reduce actual size. For a fixed-notional comparison, explicitly use a 6.25% position cap on ₹200,000 and record that setting; otherwise the old/new tests also differ in sizing.

Before interpreting profit: verify nonzero evaluation counts, named strategies on every entry/closed trade, 100% closed-trade attribution, and a completed run with no open positions. Export the new forensic ZIP. Do not rewrite Replay 24 as if it had tested the repaired engine. These repairs remove identified faults; positive after-cost edge remains unproven.
