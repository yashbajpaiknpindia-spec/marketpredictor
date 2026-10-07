# MarketPredictor — Release History, Version Start Changes & Replay Findings

Generated for the **v3.4.15** source package.

This document is the single index for the engineering changes that led into each major/minor version, plus the replay findings that were used to drive subsequent fixes. The individual version reports remain included in the ZIP unchanged.

## How to use this pack

- The `V3*.md` / `V31*.md` reports are the detailed source documents for each release or audit.
- This file gives the chronological map so a future maintainer can understand **what changed, why it changed, and which replay/research finding caused it** without reading every report first.
- Replay findings are separated into **verified forensic evidence** and **UI/runtime observations**. No unverified strategy attribution is invented for historical runs where the artifact did not contain it.

## Version-by-version change history

### V3 Alpha — research architecture foundation
**Source reports:** `V3_ALPHA_FIRST_BUILD_README.md`, `V3_COMBINED_RESEARCH_STATUS.md`

Starting point for the V3 research architecture. The design separated data-quality checks, next-executable-open labels, MFE/MAE/path labels, configurable transaction costs, economic gating, candidate records and research outputs. The verified corpus was still 5-minute data; genuine 1-minute history was explicitly not fabricated. Live authorization stayed OFF.

### V3.1 — data-first / alpha-first research
**Source reports:** `V31_DATA_BACKFILL_README.md`, `V31_FINAL_ROUND_REPORT.md`

Added genuine-1m ingestion/resampling paths, authenticated DhanHQ/Upstox routes, data validation and stricter walk-forward research. Multiple OOS research families remained negative after costs/stress. The release therefore kept production authorization locked and treated stronger real 1-minute/execution-context data as a prerequisite rather than forcing a positive result.

### V3.2.1 — opportunity/barrier/economic parity
**Source reports:** `V3.2_README.md`, `V3.2.1_REQUIREMENTS.md`, `V3.2.1_POST_FIX_AUDIT.md`, `V3.2_SIMULATION_REPORT.md`

Correctness hardening across candidate discovery, LONG/SHORT side parity, barrier labels, conservative same-bar ambiguity, economics/costs, portfolio ranking and runtime parity. The post-fix audit reported **40/40 checks passed** and the simulation layer remained non-production-authorized because the verified historical corpus was still 5-minute-only.

### V3.2.2 — provider/fallback + export auditability
**Source reports:** `V3.2.2_DATA_PROVIDER_AUDIT.md`, `V3.2.2_EXPORT_AUDIT.md`, `V3.2.2_EXPORT_UI_STATUS.md`, `V3.2.2_FALLBACK_REQUIREMENTS.md`

Stopped undocumented index-token guessing. Provider routes were made explicit, fallback attempts/errors became auditable, and dataset/date completeness was surfaced instead of silently dropping reference data.

### V3.2.3 — persistent historical exports
**Source report:** `V3.2.3_EXPORT_PERSISTENCE_AUDIT.md`

Render restart evidence showed historical-export jobs and local ZIPs could disappear with the worker. Job metadata and the newest completed archive were persisted in PostgreSQL, compact ZIP mode was introduced, export progress became visible earlier, and several unrelated runtime NameErrors exposed by logs were fixed.

### V3.2.4 — low-memory export hardening
**Source report:** `V3.2.4_LOW_MEMORY_EXPORT_AUDIT.md`

A reported Render OOM was traced to transient 1-minute export allocations. Provider concurrency and scrips-per-call defaults were reduced, prefetch memory guards and a 128 MB reserve were added, heap release was made more aggressive, and PostgreSQL archive chunks were reduced. The objective was to prevent worker death without changing candle values or trading rules.

### V3.2.5 — deployment/stress validation
**Source reports:** `V3.2.5_DEPLOYMENT.md`, `V3.2.5_STRESS_RESULTS.json`

Added/validated deployment and low-memory behavior using the real Flask/export implementation under constrained memory. Offline stress confirmed archive CRCs, row counts, HTTP range behavior and bounded memory under the tested synthetic workload. Live Render/broker/PostgreSQL credentials were not available in the build environment, so live deployment behavior remained an operational check.

### V3.2.6 — frozen 20-strategy replacement
**Source report:** `V3.2.6_20_STRATEGY_REPLACEMENT.md`

Replaced the prior 10-strategy intraday catalogue with the current **20 frozen baseline strategies**. This is the strategy set that later Replay releases must preserve unless a future research release explicitly changes it.

### V3.3.0 — boot resilience
**Source report:** `V3.3.0_BOOT_FIX.md`

Fixed startup/boot behavior so the application could reach a stable serving state instead of failing during initialization.

### V3.3.1 — database status correctness
**Source report:** `V3.3.1_DATABASE_STATUS_FIX.md`

Separated the database connectivity indicator from schema/initialization state so a connected PostgreSQL server is not mislabeled as a network failure merely because schema setup is still in progress.

### V3.3.2 — database boot diagnostics
**Source report:** `V3.3.2_DATABASE_BOOT_DIAGNOSTICS.md`

Expanded startup diagnostics so DB failures could be distinguished by phase, making Render startup incidents diagnosable rather than opaque.

### V3.3.3 — database state fix
**Source report:** `V3.3.3_DATABASE_STATE_FIX.md`

Corrected persisted/observed database state transitions so application status and actual PostgreSQL availability remained aligned during startup and recovery.

### V3.4.0 — PostgreSQL pooling
**Source report:** `V3.4.0_DATABASE_POOL_FIX.md`

Introduced a thread-safe lazy PostgreSQL pool, separated connectivity probes from schema readiness, and removed the global API gate that incorrectly blocked all traffic during schema migration.

### V3.4.1 — pool leak fix
**Source report:** `V3.4.1_DATABASE_POOL_LEAK_FIX.md`

Fixed `_PooledConnection` lifecycle handling so `with get_db_connection()` returns borrowed connections to the pool. Local regression covered 100 acquire/release cycles with one underlying connection and an exception-path return check.

### V3.4.2 — RAW gate audit + controlled strategy replay
**Source reports:** `V3.4.2_GATE_AUDIT.md`, `V3.4.2_STRATEGY_REPLAY_AUDIT.md`

Made RAW Replay a controlled 20-strategy research path. Global VWAP/opening-breakout/learning/score/evidence/shortlist overrides were removed from RAW strategy eligibility; strategy-native rules stayed inside each strategy. The replay stage and per-strategy evaluated/eligible/selected counters were exposed for auditability.

### V3.4.3 — Replay/DB/gate UI correctness
**Source report:** `V3.4.3_REPLAY_DB_GATE_FIX.md`

Fixed Replay client initialization, signal-mode submission, stale VWAP client references and server-side legacy override handling. The stage panel and 20-strategy audit from v3.4.2 remained intact.

### V3.4.4 — Replay runtime/DB state repair
**Source report:** `V3.4.4_REPLAY_DB_STATE_FIX.md`

Added missing execution helpers and status-path imports, made the DB badge use an active SELECT 1 probe, surfaced worker errors in Replay status, and kept RAW strategy independence separate from downstream execution/risk/cost gates.

### V3.4.5 — scrip-error hardening + independent tournament
**Source report:** `V3.4.5_SCRIP_AND_TOURNAMENT.md`

Stopped repeated undocumented index-history requests and added a separate 20-Strategy Tournament where every strategy has its own virtual capital/ledger/risk budget. Tournament behavior is isolated from normal Replay selector behavior.

### V3.4.6 — Replay forensics
**Source report:** `V3.4.6_REPLAY_FORENSICS.md`

Introduced the detailed Replay forensic artifact so trades, strategy decisions, events, capital timeline, configuration and data provenance can be audited from a single run export.

### V3.4.6.1 — forensic completeness repair
**Source report:** `V3.4.6.1_REPLAY_FORENSICS_FIX.md`

Hardened forensic export completeness and consistency so the artifact can serve as a source-of-truth run record rather than a partial UI dump.

### V3.4.7 — intraday Replay UX + cache
**Source report:** `V3.4.7_INTRADAY_REPLAY_UX_CACHE.md`

Made Intraday Replay a focused workspace and improved historical cache reuse/status behavior, keeping the live intraday engine and Replay conceptually aligned.

### V3.4.8 — extended Replay export reuse
**Source report:** `V3.4.8_EXTENDED_REPLAY_EXPORT_REUSE.md`

Extended Replay windows (30/60/1y) were segmented safely and exported/aggregated without loading the entire long range into memory at once. Reusable historical exports became part of the extended-run path.

### V3.4.9 — Replay results/cache correctness
**Source report:** `V3.4.9_REPLAY_RESULTS_AND_CACHE_FIX.md`

Corrected Replay result persistence/cache behavior and ensured results were based on the actual run rather than stale/partial UI state.

### V3.4.10 — independent 20-strategy execution
**Source report:** `V3.4.10_INDEPENDENT_STRATEGY_EXECUTION.md`

Reaffirmed that the normal intraday engine is **independent across all 20 strategies**. The system may share centralized capital/risk/exits, but must not turn the 20 strategies into a hidden winner-selector.

### V3.4.11 — independent intraday UX + canonical ₹2L capital
**Source report:** `V3.4.11_INDEPENDENT_INTRADAY_UX.md`

Set canonical Indian paper/replay capital to **₹200,000**, upgraded only known legacy ₹5,000 factory rows once, hid Research Lab and Trading Automation from the main navigation, and focused Replay results on compact account-level capital/P&L metrics.

### V3.4.12 — Replay data preflight + progress
**Source report:** `V3.4.12_REPLAY_DATA_PREFLIGHT_UX.md`

Added cache-first data planning: Replay DB cache → persisted Export Data → provider only for missing ticker-days. Added stored-data-only mode, `/api/intraday-replay/data-plan`, visible data loading stages, and bounded segmented long-range Replay.

### V3.4.13 — Replay #23 fixes
**Source report:** `V3.4.13_REPLAY23_FIXES.md`

Replay #23 exposed several concrete defects: headline return was based on summed per-trade percentages; `.NS` ticker names were not matching bare NSE names inside Export ZIPs; stop profiles could widen risk; profit protection was target-distance dependent; trailing-stop intrabar conflict handling could miss an already-armed stop; tournament configuration could fail at runtime; forensic snapshots could be missing; and stopped runs did not clearly distinguish selected dates from dates actually replayed. These were fixed without modifying the 20 strategy definitions.

### V3.4.14 — strategy attribution + per-strategy PF audit
**Source report:** `V3.4.14_REPLAY_STRATEGY_ATTRIBUTION_FIX.md`

Replay #23 also revealed a deeper audit problem: the completed trades and entry events had no `strategy_id`, so per-strategy PF/P&L could not be trusted. v3.4.14 makes all 20 strategies explicit in results, computes PF from net rupee P&L, marks zero-trade strategies as unproven rather than profitable/losing, reports attribution coverage, and preserves isolated tournament ledgers.

### V3.4.15 — Replay UX/theme/cache speed
**Source report:** `V3.4.15_REPLAY_UX_THEME_CACHE_SPEED.md`

The current release adds an explicit Light/Dark mode, removes Research Lab and Trading Automation from the main tab bar, distinguishes stored-data reuse from provider download, avoids making the browser wait on the informational data-plan request, shortens preflight timeout, bulk-loads stored candle coverage, batches Export ZIP hydration writes, and makes Replay startup feel responsive when data is already cached.

---

# Replay findings register

## Verified forensic finding — Replay #23

Source artifact: `audit/replay_run_23_20260927_210409_forensics.zip`

- Selected dates: **21–25 September 2026** (5 dates)
- Actual replayed date: **21 September 2026 only**
- Status: **stopped**
- Starting capital: **₹200,000.00**
- Ending capital: **₹199,286.34**
- Account return: **-0.3568%**
- Completed trades: **23**
- Wins/losses: **9 / 14**
- Win rate: **39.1%**
- Gross P&L: **+₹206.34**
- Costs: **₹920.00**
- Net P&L: **-₹713.66**
- Max drawdown: **₹851.93** adverse
- Peak capital deployed: **₹192,564.90 (96.3%)**
- Capital-guard refusals: **155**
- Strategy attribution coverage in the stored artifact: **0% (23/23 completed trades unattributed)**
- `strategy_performance.json`: empty in the historical artifact
- `strategy_audit.json`: empty in the historical artifact
- `settings_snapshot`: null in the historical artifact

### Exit-pattern evidence from Replay #23

- 8 `STOP_LOSS_HIT` trades: **-₹1,105.36 net**
- 7 `TRAILING_STOP_GAVE_BACK_PROFIT`: **-₹193.85 net**
- 4 `TRAILING_PROFIT_PROTECT`: **+₹242.74 net**
- 3 `INTRADAY_QUICK_PROFIT_BOOK`: **+₹218.97 net**
- 1 `TARGET_HIT`: **+₹123.84 net**

The artifact also showed that several trades which eventually lost had first reached roughly **+0.4% to +0.6% or more** favorable movement. That observation was the empirical basis for the execution-only profit-protection overlay introduced in v3.4.13.

### Important counterfactual note

An earlier forensic analysis estimated that a simple +0.40% protection rule could materially improve several losing trades. That estimate is **counterfactual**, not a realized result, because Replay #23 did not preserve the exact threshold-crossing timestamp needed to reconstruct every intrabar exit deterministically. It is therefore recorded as a hypothesis/finding, not as proof of a profitable strategy.

## Verified attribution limitation

Replay #23 cannot be retroactively assigned to individual strategies from its stored artifact. Both completed trades and entry events contain null `strategy_id` / `strategy_name` values. Future runs must carry strategy attribution from strategy decision → entry event → completed trade.

## UI/runtime observations that drove later fixes

### Incorrect headline return display
Replay results could show a very large negative headline percentage even when the account-level capital loss was small. The root cause was summing per-trade percentage returns instead of calculating account return from starting/ending capital. v3.4.13 corrected this.

### Data reuse appeared slow/stuck
Replay #24's UI visibly sat at a `CHECKING STORED DATA` stage around 2% before any actual provider-download decision was visible. That made a cache hit feel like a stalled downloader. v3.4.15 changes the status language, preflight behavior and DB lookup strategy to address this.

### Hidden research/workbench navigation
Research Lab and Trading Automation were still visible as top-level tabs. v3.4.15 moves both behind the `⋯ More` menu so the normal intraday workflow is cleaner.

### Theme consistency
The Replay workspace did not have an explicit, persisted Light/Dark mode control. v3.4.15 adds one so the interface remains usable in either visual mode without relying on OS/browser behavior.

---

# Safety and research governance carried forward

1. The **20 strategy definitions are unchanged** by the Replay UX/cache releases.
2. Strategy signals remain independent; centralized execution/risk/exits are separate from strategy logic.
3. Chronological candle processing and next-candle execution semantics remain the canonical simulation path.
4. Costs, slippage, capital guard, sizing and execution gates remain part of the replay economics.
5. Historical-data reuse must not change the candles; it only avoids redundant fetching.
6. A strategy with zero recorded trades is **not** classified as profitable or loss-making.
7. Any unattributed completed trades create an `ATTRIBUTION_GAP` diagnostic and must not be silently assigned to a strategy.
8. Research results are not production authorization; the existing research reports still document the need for genuine 1-minute/execution-context evidence before stronger claims can be made.

# Current documentation inventory

The v3.4.15 package includes the complete version-report set that exists in the project workspace, including the V3 Alpha, V3.1, V3.2.x, V3.3.x and V3.4.0–V3.4.15 engineering/audit reports, plus the current Replay #23 forensic source artifact.

---

# v3.4.16 — Replay download OOM / worker-restart fix

**Source report:** `V3.4.16_REPLAY_DOWNLOAD_OOM_FIX.md`

Replay #24 introduced a new failure mode that was not caused by the replay simulation itself: tapping **Download** on a newer run could make the web service disappear and the UI would then show `DB: Offline` / `Web service is unreachable`. The supplied PostgreSQL excerpt shows repeated successful authentication/authorization and ordinary disconnections, but does **not** show a PostgreSQL `FATAL`, `PANIC`, crash, or startup/shutdown message. The stronger evidence therefore points to the single Render web worker being restarted during the large export request; the red DB indicator is a consequence of the web request becoming unreachable, not proof that PostgreSQL itself restarted.

The vulnerable path loaded the entire replay candle cache into memory and then created additional in-memory representations while constructing the ZIP. For a new replay with **1,356 cached ticker-days**, this could multiply memory use enough to terminate the single gunicorn worker.

### v3.4.16 changes

- Replay download no longer fetches the entire candle cache into `fetchall()` memory.
- Candle rows are read from PostgreSQL using a named/server-side cursor with small `fetchmany()` batches.
- `market_candles.jsonl` is written directly into the ZIP entry while the ZIP is backed by a temporary disk file.
- The complete candle artifact remains included in the forensic ZIP; this is a memory-safety change, not removal of data.
- The HTTP response uses `send_file()` against the finished disk-backed ZIP instead of `BytesIO().getvalue()`, avoiding another full archive copy in RAM.
- The JSON export is kept as the compact metadata/trade/event view; the complete market-candle payload is intentionally ZIP-only so requesting JSON cannot recreate the same memory spike.
- Strategy definitions, strategy attribution rules, replay execution rules, capital/risk/exit semantics and historical-data contents are unchanged.

### Regression invariant

For a given replay run, `market_candles.jsonl` in the ZIP must contain the same cache rows that the previous exporter intended to include. Only the transport/materialization method changes: DB rows → bounded batches → ZIP stream rather than DB rows → giant Python list → giant string → giant in-memory archive.
