# AI Market Scanner

A Flask + PostgreSQL stock scanner that serves **locked, precomputed market scans** from cache instead of doing slow full-market analysis inside `/api/scan`.

## What changed

### 1. Scalable background market-scanning pipeline

`/api/scan` is now a fast cache reader. It returns the latest PostgreSQL result for the requested `market` + `horizon` and does **not** run a full yfinance universe scan or call Anthropic during the user request. When old predictions are due, it may perform a small one-time price check for those tickers to update the track record.

The heavy work runs in the background with:

```bash
python app.py background-scan
```

No cron job is configured by default, so paid AI calls only happen when you intentionally start a scan. A scan can be started from the user-facing **Run First Scan** button when no current-period locked prediction exists, from the CLI, or from the protected manual trigger endpoint. Predictions are locked by period: daily scans lock to the current calendar date, and weekly scans lock to the current ISO week. A second scan in the same lock period returns the saved prediction instead of overwriting it, unless you explicitly pass `--force` from the CLI/manual endpoint.

Pipeline stages:

1. **Liquidity filter** — removes inactive/illiquid tickers using 20-day average volume and 20-day average traded value.
2. **Momentum filter** — scores week/month/quarter price momentum, volume expansion, price vs moving averages, RSI and MACD.
3. **Technical screening** — ranks by RSI, MACD, MA20/50/200 alignment, Bollinger Bands, support/resistance and 52-week context.
4. **Fundamental enrichment** — only the narrowed set receives expensive `yfinance.Ticker.info` calls for revenue growth, EPS/earnings growth, ROE, debt, cash flow, margins and valuation.
5. **News sentiment** — only deep candidates receive headline sentiment checks.
6. **AI ranking** — only the most promising 50–100 candidates are sent to Claude for final reasoning, confidence, entry, stop-loss and targets. If the API key is unavailable or AI fails, the app falls back to deterministic transparent scoring.
7. **Post-AI validation** — the app rejects hallucinated tickers, grounds entry/current prices to the scan data, recomputes risk/reward, and downgrades weak BUY calls to WATCH/AVOID when they fail minimum confidence or risk/reward rules. HOLD is no longer shown to users because users may not already own the stock.

### 2. Transparent professional analysis methods

The app uses public, documented investment frameworks:

- Technicals: RSI, MACD, moving averages, Bollinger Bands, volume confirmation, support/resistance, 52-week context.
- Fundamentals: revenue growth, EPS/earnings growth, ROE, debt-to-equity, operating/free cash flow, profit margin and valuation where available.
- News: recent headline sentiment and risk-catalyst detection.
- Risk management: suggested entry, stop-loss, target price, target %, risk/reward and confidence score.

The prompts and UI explicitly state that the app **does not claim to follow or replicate any certified financial adviser's proprietary methodology**.

## API behavior

### Prediction locking and automatic accuracy checks

- `/api/scan` does **not** create a new prediction. It returns the latest locked PostgreSQL cache and tells the frontend whether a first/current-period scan can be started.
- On each user scan request, the app first checks whether any old predictions for that same market + horizon are due and still unchecked.
- If due unchecked predictions exist **after the relevant exchange close plus safety delay**, it fetches OHLC price data for those tickers, records close/high/low, checks target/stop-loss/close outcome, marks them `checked=true`, and updates the track record. It will not verify a day prediction at the open or while the session is still active.
- If those predictions were already checked, the app skips the accuracy check and does not re-score them.
- The user-facing **Run First Scan** button calls `/api/start-first-scan`, which starts the heavy scan only when the current lock period has no prediction yet. It refuses duplicate clicks while a scan is running.
- `python app.py background-scan --markets US --horizons day` also creates a new prediction only when the current lock period has no prediction yet.
- To intentionally overwrite the current locked period, use:

```bash
python app.py background-scan --markets US --horizons day --force
```

### `POST /api/scan`

Body:

```json
{ "market": "US", "horizon": "day" }
```

Response comes from PostgreSQL cache. If old predictions are due, the response also includes an `accuracy_check` object showing whether anything was scored or skipped:

```json
{
  "cached": true,
  "stale": false,
  "picks": [],
  "funnel_counts": {
    "total_universe": 100,
    "price_snapshots": 95,
    "liquidity_pass": 70,
    "momentum_pass": 70,
    "technical_pass": 70,
    "deep_analysis": 70,
    "final_picks": 10
  },
  "methodology": { "stages": [] },
  "locked": true,
  "lock_status": "current_period_locked",
  "prediction_lock_key": "2026-07-02",
  "accuracy_check": {
    "updated": 0,
    "skipped": true,
    "reason": "no_due_unchecked_predictions_after_exchange_close"
  }
}
```

The frontend now keeps the **Run First Scan** button visible at all times. If no locked cache exists yet, `/api/scan` also returns `can_start_first_scan: true` and the button can initialize the first locked prediction. If a locked prediction already exists, clicking it safely returns the existing locked result instead of starting a new paid scan.

### `POST /api/start-first-scan`

Starts the first/current-period scan for a selected market/horizon only if no locked prediction already exists. This endpoint is safe against repeated user clicks: it marks the scan as `running` in PostgreSQL and refuses to start a second paid scan for the same market/horizon while one is already running.

Body:

```json
{ "market": "US", "horizon": "day" }
```

Response while running:

```json
{ "ok": true, "status": "running" }
```


### Smarter track record, verification, and learning

The Track Record tab now has a **Check due predictions after close** button. It only scores predictions whose exchange/horizon window has ended:

- India daily predictions: checked after the NSE close plus `VERIFY_AFTER_CLOSE_DELAY_MINUTES`.
- US daily predictions: checked after the US regular market close plus `VERIFY_AFTER_CLOSE_DELAY_MINUTES`.
- Weekly predictions: checked after five valid trading sessions.
- Holidays/weekends are skipped. Additional holidays can be supplied with `MARKET_HOLIDAYS_US` or `MARKET_HOLIDAYS_IN`.

Outcome logic is stricter:

- `BUY`: Target Hit, Stop Loss Hit, Partial Success, or Closed Below Entry.
- `WATCH`: Correct Watch, Neutral Watch, or Missed Opportunity. WATCH is not treated like a normal buy trade.
- `AVOID`: Correct Avoid, Neutral Avoid, or Avoid Missed Rally.

Every checked prediction also creates a deterministic lesson in `prediction_lessons`. Failed or missed predictions are categorized, for example:

- `low_volume_confirmation`
- `overextended_or_chasing`
- `weak_risk_reward`
- `no_news_catalyst`
- `negative_news_or_event_risk`
- `stop_loss_hit`

Future scans load recent lessons for that market/horizon, add penalties for repeated failure patterns, and pass the learning context into the final AI ranking prompt. This makes the system evolve without spending extra AI calls for the learning step.

### API cost screen

The app now includes an **API Costs** tab and `/api/costs`. It records exact Anthropic usage from `response.usage.input_tokens` and `response.usage.output_tokens` for each paid ranking call. Cached scans still cost ₹0. The screen also estimates the next first-scan cost before it runs.

Cost-related environment variables:

```bash
ANTHROPIC_INPUT_USD_PER_MILLION=2
ANTHROPIC_OUTPUT_USD_PER_MILLION=10
USD_INR_RATE=95
SCAN_ESTIMATED_INPUT_TOKENS_BASE=4500
SCAN_ESTIMATED_INPUT_TOKENS_PER_CANDIDATE=350
SCAN_ESTIMATED_OUTPUT_TOKENS=2500
```

Indian `.NS` stocks display in INR (`₹`). US stocks display in USD (`$`).

### `POST /api/scan-status`

Used by the frontend to poll a user-started first scan. It returns `complete` with the locked payload once the scan has been saved. You can still run the same scan manually from a shell if preferred:

```bash
python app.py background-scan --markets US --horizons day
```

### `POST /api/run-background-scan`

Manual trigger endpoint for a single market/horizon. If `BACKGROUND_SCAN_SECRET` is set, send it as `X-Scan-Secret`.

### `GET/POST /api/check-accuracy`

Scores due predictions against current prices. It is idempotent: predictions already marked `checked=true` are skipped. You can run it manually too:

```bash
python app.py check-accuracy
```

## Universe configuration

The app ships with a default active/liquid universe for US, India and Global markets.

For a larger production universe, either set environment variables:

```bash
MARKET_UNIVERSE_US=AAPL,MSFT,NVDA,...
MARKET_UNIVERSE_IN=RELIANCE.NS,TCS.NS,...
MARKET_UNIVERSE_GLOBAL=AAPL,TSM,ASML,...
```

or add files:

```text
data/universe_US.txt
data/universe_IN.txt
data/universe_Global.txt
```

One ticker per line or comma-separated both work.

## Deploy on Render

1. Push this repo to GitHub.
2. In Render, create from `render.yaml`.
3. Add `ANTHROPIC_API_KEY` to the web service.
4. Optional but recommended: set `BACKGROUND_SCAN_SECRET` to protect the manual scan endpoint.
5. Deploy.
6. Open the app. The **Run First Scan** button is always visible. Click it to create the first locked prediction when none exists; if a current locked prediction already exists, the backend safely returns the locked result instead of starting a duplicate paid scan. You can also click **Load Cached Scan** to read existing PostgreSQL cache. You can also run `python app.py background-scan --markets US --horizons day` manually from a shell/job whenever you want to refresh cached results.

## Local development

```bash
pip install -r requirements.txt
export DATABASE_URL=postgres://...
export ANTHROPIC_API_KEY=sk-ant-...
python app.py background-scan --markets US --horizons day
# Running it again the same day reuses the lock. Use --force only when you intentionally want to overwrite.
python app.py
# Visit http://localhost:5000
```

Without `DATABASE_URL`, the app will start but cache and prediction tracking are disabled.

## Project structure

```text
stock-scanner/
├── app.py              # Flask backend, background scanner, DB cache, accuracy checks
├── templates/
│   └── index.html      # Frontend UI
├── requirements.txt
├── render.yaml         # Render web service only; no cron services
└── README.md
```

## Notes

- yfinance data can be delayed, incomplete, or rate-limited. The app handles missing data by dropping unavailable symbols from the funnel.
- This is an educational analysis tool, not financial advice.
- Locked predictions improve auditability because users see the same result until a deliberate new-period scan is run.
- Past performance or a measured track record does not guarantee future results.

## Trading automation layer

This version adds a controlled buy/sell automation layer. It is intentionally safe by default:

- `paper` mode simulates BUY entries from locked BUY signals and simulates exits when target or stop is reached.
- `assisted` mode creates confirmation-required orders but does not place live broker orders.
- `live` mode is blocked unless `LIVE_TRADING_ENABLED=true` and `BROKER_ORDER_WEBHOOK_URL` are configured server-side.

New endpoints:

```bash
GET  /api/trading?market=IN&horizon=day
POST /api/trading/settings
POST /api/trading/run
```

Important environment variables:

```bash
TRADING_ENABLED=true
AUTO_TRADING_MODE=paper              # paper | assisted | live
LIVE_TRADING_ENABLED=false           # must be true for live broker routing
BROKER_NAME=groww
BROKER_ORDER_WEBHOOK_URL=            # your Groww/broker execution adapter webhook
BROKER_ORDER_WEBHOOK_SECRET=
TRADING_DEFAULT_CAPITAL_INR=5000
TRADING_MAX_POSITION_PCT=25
TRADING_RISK_PER_TRADE_PCT=2
TRADING_MAX_OPEN_POSITIONS=3
TRADING_MIN_CONFIDENCE=65
TRADING_MIN_RISK_REWARD=1.5
TRADING_WEEKLY_PROFIT_TARGET_PCT=10
TRADING_MAX_WEEKLY_LOSS_PCT=4
TRADING_COVER_API_COSTS_FROM_PROFIT=true
TRADING_STOP_AFTER_WEEKLY_TARGET=true
TRADING_STOP_AFTER_WEEKLY_LOSS=true
```

### Weekly Portfolio Goal Mode

This upgrade adds a capital-protection goal guard. The app can attempt a weekly portfolio goal using multiple BUY setups, but it stops opening new entries when the weekly target is reached or when the weekly loss guard is hit.

Default behaviour:

- Capital: ₹5,000
- Weekly profit target: 10%
- Max weekly loss: 4%
- API costs are subtracted from weekly profit before the target is considered reached.
- If realised P/L is ₹520 and API costs are ₹20, net progress is ₹500.
- The goal is considered achieved only when net profit after API costs reaches the target.
- If the weekly guard is active, the automation still checks exits for open positions but does not open new trades.

Formula used by the app:

```text
net_weekly_profit = realised_trading_pnl - paid_api_costs
weekly_target = capital_inr * weekly_profit_target_pct / 100
max_weekly_loss = capital_inr * max_weekly_loss_pct / 100
```

For ₹5,000 and a 10% weekly target:

```text
weekly_target = ₹500 net after API costs
max_weekly_loss at 4% = ₹200
```

The trading screen now shows weekly net after API, target progress, API cost covered, and whether the goal guard is open or paused.

### Groww automation setup notes

The app is broker-adapter ready. To connect Groww live execution, use `BROKER_NAME=groww` and point `BROKER_ORDER_WEBHOOK_URL` to a secure adapter that signs in to Groww's official Trading API/SDK and places orders. Keep the main app in `paper` or `assisted` mode until the Groww adapter, audit trail, and broker/API requirements are tested.

Live Indian market execution should be connected only through a SEBI/broker-compliant API/algo flow with required approvals, identifiers, static IP or broker-hosted cloud execution, and audit trail.

## Systemized AI + rule execution architecture

This version separates expensive AI analysis from live execution:

```text
Claude / paid AI = bounded analyst for locked scan ranking only
App rule engine = buy/sell decisions, stop-loss, target, weekly goal and max-loss guard
Groww/broker API = execution bridge only after you connect a compliant adapter
```

Claude is **not** called for every buy/sell. It is not used for stop-loss checks, target checks, position exits, weekly goal checks, cached scan loading, or paper trading exits. Those actions are deterministic and use the locked prediction already saved in PostgreSQL.

### Paid AI budget guard

The app now has hard paid-AI limits. If the limit is reached, the scanner automatically falls back to deterministic ranking instead of calling Claude.

Environment variables:

```bash
AI_MAX_PAID_CALLS_PER_DAY=2
AI_MAX_PAID_CALLS_PER_WEEK=8
AI_MAX_COST_INR_PER_DAY=75
AI_MAX_COST_INR_PER_WEEK=300
AI_ALLOW_FORCE_OVER_BUDGET=false
LEARNING_AI_ENABLED=false
```

Cost screen now returns and displays the AI budget state from `/api/costs`. The `Run First Scan` path includes budget details and will only use Claude if the budget guard allows it.

### API cost coverage from profit

The weekly goal guard can now include both:

```text
1. exact paid Claude/API costs logged in api_cost_log
2. configured broker/API monthly subscription allocation
```

Set the broker/API monthly cost in the Trading Automation screen or with:

```bash
TRADING_BROKER_API_MONTHLY_COST_INR=589
BROKER_API_MONTHLY_COST_INR=589
```

Weekly cost allocation uses approximately:

```text
weekly_broker_api_cost = monthly_broker_api_cost * 12 / 52
net_weekly_profit = realised_trading_pnl - paid_ai_costs - weekly_broker_api_cost
```

So if the weekly target is ₹500 and weekly API allocation is ₹136, the system needs roughly ₹636 gross realised profit before considering the ₹500 net target achieved.


### Opening confirmation engine

Trading Automation now blocks fresh BUY entries until the configured opening wait window has passed.

Default behavior:

```text
1. Market opens.
2. First 15 minutes are observation only.
3. After the wait, the app checks live 5-minute data.
4. Entry is allowed only if price is still near the locked entry, not below the risk zone, not already at target, and risk/reward still passes.
5. No Claude call is made for this check.
```

Configurable controls:

```bash
OPENING_CONFIRMATION_ENABLED=true
OPENING_CONFIRMATION_WAIT_MINUTES=15
OPENING_MAX_ENTRY_CHASE_PCT=1.25
OPENING_MAX_ENTRY_PULLBACK_PCT=1.0
OPENING_MIN_VOLUME_MULTIPLIER=0
```

The UI also exposes these controls in Trading Automation. `OPENING_MIN_VOLUME_MULTIPLIER=0` means the volume gate is disabled; set it above 1 only if your data feed has reliable intraday volume.

### Profit protection exit engine

Open positions are no longer closed only at target or stop-loss. The deterministic exit engine can close paper/assisted/live positions when:

```text
- target is hit
- stop-loss is hit
- trailing profit-protection is hit
- momentum fades after enough target progress
- weekly goal has been reached and profit should be locked
- the day/week horizon ends
```

Default behavior:

```bash
PROFIT_PROTECTION_ENABLED=true
PROFIT_PROTECT_PROGRESS_PCT=50
TRAILING_STOP_ACTIVATION_PCT=40
TRAILING_STOP_GIVEBACK_PCT=35
EXIT_AT_HORIZON_END=true
```

This means the app may book profit before the original target if the setup has already progressed meaningfully but starts reversing. This is rule-based and does not call Claude.

### Professional live-flow rule

Recommended live-market flow:

```text
1. Run First Scan once for the current market/horizon.
2. App locks the prediction in PostgreSQL.
3. Market opens.
4. Trading Automation uses only locked data + deterministic risk rules.
5. Buy/sell/target/stop-loss actions do not call Claude.
6. After close, Track Record verifies predictions and stores lessons.
7. Future scans consider stored lessons before/while ranking.
```

This keeps API cost controlled and prevents uncontrolled AI calls during live trading.

## Intraday Engine page

This version adds a completely separate **Intraday Engine** page. It is not the same as the AI scanner and it does not call Claude during live trading.

Intraday flow:

```text
1. Market opens.
2. The engine waits for the configured opening wait window, default 15 minutes.
3. It scans the selected market universe with 5-minute candles.
4. It looks for bullish setups using deterministic rules only.
5. It opens paper/assisted/live orders only if Trading Automation is enabled and all portfolio guards allow entries.
6. It exits through target, stop-loss, quick profit booking, trailing protection, or forced exit before close.
```

Default intraday entry rules:

```text
- price moved up from open
- price is above VWAP
- opening range breakout is present
- recent 5-minute trend is not falling
- volume multiplier passes the configured threshold
- price is not too extended/chased
- weekly target/loss guard is open
- max intraday trades/day not reached
- max open positions not reached
```

Default intraday exit rules:

```text
- target hit
- stop-loss hit
- quick profit book if profit is reached but momentum fades
- trailing profit-protection hit
- forced exit before market close
- weekly target profit lock
```

Environment controls:

```bash
INTRADAY_ENGINE_ENABLED=false
INTRADAY_DEFAULT_MARKET=IN
INTRADAY_OPENING_WAIT_MINUTES=15
INTRADAY_UNIVERSE_LIMIT=60
INTRADAY_MAX_CANDIDATES=8
INTRADAY_MIN_SCORE=70
INTRADAY_MIN_PRICE_CHANGE_PCT=0.35
INTRADAY_MAX_CHASE_PCT=3.0
INTRADAY_REQUIRE_VWAP=true
INTRADAY_REQUIRE_OPENING_BREAKOUT=true
INTRADAY_MIN_VOLUME_MULTIPLIER=1.2
INTRADAY_QUICK_TARGET_PCT=0.8
INTRADAY_MAX_STOP_LOSS_PCT=0.6
INTRADAY_MIN_RISK_REWARD=1.2
INTRADAY_MAX_TRADES_PER_DAY=3
INTRADAY_PROFIT_BOOK_PCT=0.45
INTRADAY_FORCE_EXIT_BEFORE_CLOSE_MINUTES=15
```

New endpoints:

```text
GET  /api/intraday
POST /api/intraday/settings
POST /api/intraday/run
```

Important: intraday trading is high risk. Test in paper mode first. Live execution should only be enabled through a compliant broker/API setup with audit trails and strict loss limits.

## Intraday Engine: automatic/manual execution modes

The Intraday Engine is separate from the AI day/week scanner. It uses deterministic 5-minute candle rules and does not call Claude for live entries or exits.

New controls on the Intraday Engine page:

- **Intraday mode**
  - `paper`: simulated trades only
  - `assisted`: creates confirmation-required orders
  - `live`: sends broker orders only when `LIVE_TRADING_ENABLED=true` and broker webhook/API env vars are configured; otherwise it is downgraded to assisted
- **Confirm first**: keeps human confirmation on for assisted/live-style workflows
- **Auto-run**: allows the server-side intraday worker to repeatedly run the engine
- **Auto interval seconds**: how often the worker should run, minimum 30 seconds
- **Run once now**: manual trigger for the engine
- **Start auto / Stop auto**: starts or stops the server-side automatic intraday worker

Auto mode checks exits and entries repeatedly while the web service process is alive. It still obeys:

- opening wait window
- max intraday trades per day
- max open positions
- weekly target guard
- max weekly loss guard
- position sizing and risk-per-trade caps
- target, stop-loss, profit-booking, trailing protection and forced exit rules

The worker is disabled by default. To auto-start it on server boot, set:

```env
INTRADAY_SERVER_AUTO_ON_START=true
INTRADAY_ENGINE_ENABLED=true
INTRADAY_AUTO_ENABLED=true
```

Live mode remains blocked unless:

```env
LIVE_TRADING_ENABLED=true
BROKER_ORDER_WEBHOOK_URL=your_secure_broker_adapter_url
BROKER_ORDER_WEBHOOK_SECRET=your_secret
```

No Claude/API cost is created by intraday scan, buy, sell, stop-loss, target or auto-run cycles.


### Dynamic Exit Policy

Trade horizon is treated as the maximum validity/evaluation window, not a forced holding period. The engine may exit earlier when any stronger sell rule triggers:

- target hit
- stop-loss hit
- trailing profit-protection hit
- momentum-fade profit booking
- weekly goal profit lock
- intraday forced exit before close
- final horizon-expiry fallback

This means a day or week setup does not blindly hold until the end of the horizon if profit should be protected earlier.

## Intraday auto check history and market-close behavior

The Intraday Engine now records every manual/auto engine cycle in `intraday_run_log` with:
- checked time
- trigger (`manual-button`, `auto`, etc.)
- market and session state
- number of stocks scanned
- eligible bullish candidates
- orders created
- message/reason why entries were allowed, blocked, or skipped

The Intraday page displays this in the **Intraday auto/check history** table.

Auto mode is designed to keep checking during the valid market window. It does not buy before market open or during the opening wait window. After the market has closed, it performs one final exit check and then stops the auto loop instead of endlessly scanning a closed market.

## Market data source transparency

The Intraday Engine now displays the source used for each engine check and candidate row.

Current built-in data source:

```env
MARKET_DATA_PROVIDER=yfinance
```

This means the app fetches candles/prices through Yahoo Finance via `yfinance`. The UI labels this as `best_effort_may_be_delayed` and `not official/exchange-grade`, so you can clearly distinguish paper-test data from broker-grade data.

If you set:

```env
MARKET_DATA_PROVIDER=groww
GROWW_MARKET_DATA_ENABLED=true
```

this build will still show the truth: Groww is configured, but direct Groww market-data adapter is not yet implemented, so yfinance fallback remains active until the Groww adapter is added.

## Research Lab / Smarter Strategy Engine

This build adds a Research Lab so the system can improve based on evidence instead of only forward live win rate.

### What it adds

- Point-in-time historical replay for the existing funnel style using only bars available before each simulated signal date.
- No Claude calls during backtests; the historical replay is deterministic and cost-controlled.
- Realistic execution assumptions: slippage, spread, and fee basis points.
- Risk-adjusted metrics: win rate, expectancy, average win/loss, profit factor, Sharpe, Sortino, and max drawdown.
- Regime classification: trending bull, choppy/sideways, and high-vol/risk-off regimes.
- Pattern memory: signal combinations such as RSI zone + volume confirmation + MACD + risk/reward are stored in `strategy_signal_stats` and used as future score adjustments.
- Backtest history tables: `backtest_runs`, `backtest_trades`, and `strategy_signal_stats`.

### New endpoints

```text
GET  /api/research
POST /api/backtest/run
```

Example:

```bash
curl -X POST https://your-app.onrender.com/api/backtest/run \
  -H "Content-Type: application/json" \
  -d '{"market":"IN","horizon":"day","universe_limit":40,"top_n":5,"rebalance_step_days":5}'
```

### New CLI command

```bash
python app.py backtest --market IN --horizon day --universe-limit 40 --top-n 5 --step-days 5
```

### Important limitation

The built-in free historical mode uses yfinance daily bars. To avoid lookahead bias, it does **not** pretend to know past fundamentals/news exactly as of the historical date. In this free mode, backtesting focuses on price, volume, momentum, technicals, ATR/risk, regime, and execution assumptions. For institutional-grade validation, connect a paid point-in-time fundamentals/news/options dataset later.

### New environment variables

```env
BACKTEST_ENABLED=true
BACKTEST_DEFAULT_LOOKBACK_YEARS=3
BACKTEST_UNIVERSE_LIMIT=40
BACKTEST_TOP_N=5
BACKTEST_REBALANCE_STEP_DAYS=5
BACKTEST_MIN_HISTORY_DAYS=90
BACKTEST_MAX_WORKERS=6
BACKTEST_MAX_RUNTIME_SECONDS=24
BACKTEST_WEB_MAX_UNIVERSE_LIMIT=25
EXECUTION_SLIPPAGE_BPS=8
EXECUTION_SPREAD_BPS=5
EXECUTION_FEE_BPS=6
PORTFOLIO_CORRELATION_ALERT=0.72
PORTFOLIO_MAX_CORRELATED_POSITIONS=2
VOL_TARGET_ATR_RISK_MULTIPLIER=1.4
```

### Policy

The app should not claim to be smarter than traders until it has enough out-of-sample evidence. The Research Lab is meant to prove or disprove the edge across market regimes before scaling to real capital.

## Per-stock backtest analytics and mistake-learning upgrade

This build adds a visible per-stock backtest breakdown in Research Lab so the system can answer: *which stocks historically worked, how often, and why did failures happen?*

### What now appears in Research Lab

After a backtest run or refresh, the Research Lab shows:

- Per-stock trade count
- Per-stock correct/win percentage
- Per-stock expectancy per trade
- Average win and average loss
- Best and worst simulated trade
- Target hits, stop-loss hits, and horizon exits
- Dominant regime for that stock
- Top repeated failure reason
- A short lesson explaining what future scans should avoid or require as confirmation

### How learning/evolving now works

The system has two learning sources:

1. **Live prediction lessons** from `prediction_lessons` after real/forward predictions are checked.
2. **Historical pattern memory** from `backtest_trades` and `strategy_signal_stats` after Research Lab backtests.

When a prediction or simulated historical trade fails, the app records why, such as:

- `stop_loss_hit`
- `low_volume_confirmation`
- `overextended_or_chasing`
- `weak_risk_reward`
- `below_short_term_trend`
- `weak_or_negative_macd`
- `setup_did_not_follow_through`

The system converts the failed setup into a combination pattern such as:

```text
rsi_high + vol_low + rr_weak + macd_pos
```

Future scans compare new candidates against these stored patterns. Repeated weak patterns receive score/confidence penalties, while historically favorable patterns can receive a positive adjustment. This makes the system learn from repeated mistakes rather than treating every scan as a fresh blank slate.

### New / updated environment variables

```env
BACKTEST_STOCK_STATS_LIMIT=80
LEARNING_PATTERN_MIN_TRADES=3
LEARNING_BAD_PATTERN_WIN_RATE_PCT=45
LEARNING_GOOD_PATTERN_WIN_RATE_PCT=58
```

### Important note

This is still evidence-based learning, not guaranteed profit. The system can reduce repeated mistakes and improve discipline, but it must be validated with paper trading and out-of-sample backtests before live money.

## Raw vs Learned Backtest Mode

This build fixes a real gap: previously, `run_historical_backtest()` never applied its own
learned pattern-memory (`strategy_signal_stats`) or per-stock history back into the backtest
simulation itself -- it only *recorded* lessons for use in live/forward scans. That meant
re-running a backtest with "the same settings" could never actually show whether learning
helped, because learning was never applied inside the backtest loop in the first place.

### What changed

- `run_historical_backtest(..., learning_mode='raw' | 'learned')` -- new parameter.
  - `raw`: replays the base strategy exactly as coded. No pattern-memory or per-stock
    history adjustments are applied. This is the honest control/baseline.
  - `learned`: before ranking/filtering each historical test day, candidates are re-scored
    using `strategy_memory_adjustment()` (the same pattern-memory logic used in live scans)
    and `strict_learning_review()` is given the accumulated per-stock backtest history, so
    Strict Learning Mode can block/downgrade a stock in the simulation the same way it would
    live.
- The backtest fingerprint now includes `learning_mode` and, for `learned` runs only, a
  `strategy_memory_signature` (a hash of the current `strategy_signal_stats` state). This
  means a `learned`-mode rerun of the exact same window, done AFTER new learning has
  accumulated, is correctly treated as a new, non-duplicate test -- instead of being silently
  served from the duplicate-protection cache.

### New endpoint

```text
POST /api/backtest/compare-learning
{"market":"IN","horizon":"day","universe_limit":25,"top_n":5,"rebalance_step_days":1}
```

Runs the identical window/settings twice (`raw`, then `learned`) and returns both result sets
side by side plus an honest verdict:

```text
learning_helped_and_crossed_breakeven
learning_helped_and_stayed_profitable
learning_helped_but_still_net_negative
learning_made_it_worse_on_this_window
no_meaningful_change
incomplete_data
```

Research Lab now has a **Backtest mode** selector (Raw / Learned) and a
**"Compare Raw vs Learned (same window)"** button that calls this endpoint and renders both
rows with win rate, expectancy, Sharpe, Sortino, profit factor, and max drawdown side by side.

### Important honesty note

This feature answers *"did learning measurably help, on the same test"* -- it deliberately
does not chase or claim a 100% win rate. No real trading strategy sustains a 100% win rate;
a system tuned to hit one on historical data is almost certainly overfit to noise in that
specific window and should be expected to fail live. The realistic target shown here is a
small, consistent, positive expectancy (profit factor meaningfully above 1.0) that holds up
across multiple regimes and enough trades (100+) to be statistically believable -- not a
perfect record.

## Selectable Backtest History Range + Split/Corporate-Action Correctness

Two related fixes: how far back you can test, and whether the historical data used is
actually correct for stocks that had corporate actions (splits, bonus issues -- common on
NSE/BSE).

### Selectable history range

Research Lab now has a **History to test** selector: Last 1 / 3 / 5 / 7 / 10 years, or a
custom start/end date range. This maps directly to the existing `start_date`/`end_date`
parameters `run_historical_backtest()` already supported, so no server-side default was
silently limiting you -- the UI just wasn't exposing the choice before.

Guidance shown in the UI: sample size (trades per pattern) and calendar length are separate
requirements. A pattern needs roughly 50-100+ trades before its win rate is trustworthy
(margin of error is ~±18% at 8 trades vs ~±5% at 100 trades), and separately the tested
window needs to span multiple market regimes (trending bull, choppy/sideways, real
correction/bear) or the result only tells you about one kind of market, not how the
strategy behaves in general.

### Historical data correctness: stock splits / bonus issues

Previously, backtests fetched price history with `actions=False`, and price mode defaults to
`raw` (no auto-adjustment for splits/dividends). This meant a real 2:1 stock split or bonus
issue -- which shows up as a legitimate ~50%+ overnight price change in raw mode -- could:

1. Get flagged as "suspicious" by the data-integrity guard (false alarm on good data), and/or
2. Get simulated as if it were a real ~50% loss or gain inside a trade's holding window
   (corrupting that trade's outcome and, downstream, `strategy_signal_stats` learning) with
   a price move that never actually happened to a holder of the stock.

This build fixes both:

- History fetches now use `actions=True`, so confirmed split events (from yfinance's own
  corporate-action data, not just a price-gap heuristic) are detected via
  `detect_split_dates()`.
- `analyze_backtest_data_integrity()` no longer penalizes a large gap that coincides with a
  confirmed split -- it's reported separately as an expected corporate action, not a data
  quality warning.
- The trade simulation loop now skips (does not simulate) any candidate whose entry-to-exit
  holding window would straddle a confirmed split date, so a real split can never be
  misread as a fake stop-loss hit or a fake giant win. These skips are counted and reported
  in the backtest metrics as `skipped_for_corporate_action`.

This does not require a paid data source -- it uses yfinance's existing (free) split-history
field, just previously unused (`actions=False`). It's a correctness fix within the current
free data setup, not a new dependency.

## Full Data Export

This build adds a complete export screen and endpoints so the platform can be audited or backed up without leaving out core records.

UI:

```text
Export Data tab → Refresh export manifest → Download full ZIP export / Download full JSON export
```

Endpoints:

```bash
GET /api/export/manifest
GET /api/export/all?format=zip
GET /api/export/all?format=json
```

The ZIP export contains:

```text
manifest.json
summaries.json
tables/predictions.json + .csv
tables/stock_scan_cache.json + .csv
tables/stock_scan_runs.json + .csv
tables/prediction_lessons.json + .csv
tables/api_cost_log.json + .csv
tables/trading_settings.json + .csv
tables/intraday_settings.json + .csv
tables/trade_orders.json + .csv
tables/trade_positions.json + .csv
tables/intraday_run_log.json + .csv
tables/backtest_runs.json + .csv
tables/backtest_trades.json + .csv
tables/strategy_signal_stats.json + .csv
```

Computed summaries include:

```text
prediction accuracy overall
prediction accuracy by market / horizon / signal
trade performance overall
trade performance by engine / mode / market
API cost totals
latest backtest runs
per-stock backtest accuracy
strategy memory / signal pattern stats
safe non-secret environment configuration
```

Security note: the export intentionally excludes secret environment variables such as `DATABASE_URL`, `ANTHROPIC_API_KEY`, Groww tokens, webhook secrets, and broker credentials. Downloaded exports may still contain sensitive trading/prediction records, so keep them private.

## Easier Research Lab Wording

This build also simplifies the Research Lab front end so non-technical users can understand what the backtest is saying.

Changes:

```text
Strategy memory from backtests → Learning rules found from past tests
Patterns/regimes → Plain-English market conditions and signal mixes
Regime → Market type
Pattern → What the system saw
Expectation / expectancy → Avg result
Adjustment → Future action
Sharpe → Risk quality
Sortino → Downside safety
Max drawdown → Worst fall
```

Raw pattern keys such as `rsi_bull+vol_low+rr_weak+macd_pos` are translated into readable lessons such as:

```text
RSI bullish + Low volume + Weak reward vs risk + MACD positive
Future action: Avoid / strong penalty
```

The page now clearly states that Research Lab is past simulation and learning only. Future stock picks still come from the main Scan / Prediction Engine and Intraday Engine.

## Strict Learning Architecture

This build wires the learning architecture into the app as separate paths instead of mixing all learning together:

```text
Research Lab daily backtest → improves Main Prediction Engine
Trade History → improves Trading Automation
Intraday History → improves Intraday Engine
```

Why this matters:

```text
Daily/weekly prediction uses daily candles and swing-style patterns.
Trading automation learns from actual/paper order results and position exits.
Intraday learns from 5-minute breakouts, VWAP, volume spike and quick exits.
```

Strict Learning Mode is enabled by default and follows the policy:

```text
Trade less.
Filter harder.
Trust only historically strong patterns.
Avoid repeated failure patterns.
Show no-trade/watch when edge is weak.
```

Rules added:

```text
1. Minimum score filter
2. Minimum reward/risk filter
3. Volume confirmation filter
4. Market regime filter
5. Pattern reliability filter
6. Bad-pattern penalty
7. Per-stock historical strength filter
8. Training vs validation split
9. Benchmark comparison
10. No-trade recommendation when edge is weak
```

New non-secret environment controls:

```env
STRICT_LEARNING_ENABLED=true
STRICT_MIN_SCORE=60
STRICT_BUY_MIN_SCORE=65
STRICT_BACKTEST_MIN_SCORE=45
STRICT_MIN_RISK_REWARD=1.4
STRICT_MIN_VOLUME_RATIO=0.8
STRICT_AVOID_RISK_OFF=true
STRICT_PATTERN_RELIABLE_MIN_TRADES=8
STRICT_PER_STOCK_MIN_TRADES=5
STRICT_PER_STOCK_MIN_WIN_RATE=48
STRICT_PER_STOCK_MIN_EXPECTANCY=0
STRICT_TRADE_HISTORY_MIN_TRADES=3
STRICT_INTRADAY_HISTORY_MIN_TRADES=3
STRICT_NO_TRADE_IF_NO_BUY=true
```

Research Lab now includes an explanation table called "How the system learns separately" and each backtest run stores:

```text
strict-learning rejected setup sample
training vs validation summary
simple benchmark comparison against loaded universe buy-and-hold
no forced Top N when setups are weak
```

Trading automation now checks closed/paper/live trade history before opening new positions. If a stock repeatedly failed in automation history, Strict Learning Mode can block the new entry even if the locked scan had previously marked it as BUY.

Intraday engine now checks intraday trade history separately. If a stock repeatedly fails quick breakout/VWAP setups, the intraday scanner adds a strict-learning block reason and keeps it out of eligible entries.

### Backtest symbol-availability fix

This build skips unavailable, renamed, delisted, or insufficient-history symbols during backtests instead of letting yfinance 404 messages disrupt the run. Known stale symbols are mapped before scanning, including:

- `TATAMOTORS.NS` → `TMCV.NS`
- `ADANITRANS.NS` → `ADANIENSOL.NS`

The Research Lab note now reports how many unavailable/old symbols were skipped. If you want full control, override the universe with `MARKET_UNIVERSE_IN`, `MARKET_UNIVERSE_US`, or `MARKET_UNIVERSE_GLOBAL`, or add `data/universe_IN.txt`.

### Quality-stock backtest filter + full run drilldown + macro/news risk layer

This build adds a cleaner Research Lab and safer forward-decision layer:

```text
Backtests now skip inactive, illiquid, unavailable, renamed, insufficient-history, low-volume, and low-turnover symbols.
Only quality/tradeable stocks are allowed to become learning examples.
```

Research Lab UI changes:

```text
Quality stocks to scan = how many symbols the backtest tries before filters.
Best stocks picked per test = how many strong setups can be simulated on each old test date.
Run old test every X days = how often old dates are replayed.
Tap any past test summary row to open the full history of that backtest run.
```

Forward scans/trading now check a market context layer:

```text
Broad market trend: bullish / bearish / sideways using market index trend.
Volatility/risk regime: VIX-style context when available.
Macro/news risk: war, trade deals, tariffs, rates, inflation, oil shocks, currency moves, regulation/legal risk.
```

Important: current macro/news risk is used for future scans, trading automation, and intraday checks. It is not applied to historical backtest dates because using today's headlines on 2023 trades would create lookahead bias.

New non-secret environment controls:

```env
QUALITY_STOCK_FILTER_ENABLED=true
QUALITY_MIN_PRICE=10
QUALITY_MAX_ZERO_VOLUME_DAYS=8
QUALITY_MIN_RECENT_BARS=60
QUALITY_MIN_AVG_VOLUME_MULTIPLIER=1.0
QUALITY_MIN_TURNOVER_MULTIPLIER=1.0

MACRO_RISK_ENABLED=true
MACRO_RISK_CAUTION_THRESHOLD=18
MACRO_RISK_BLOCK_THRESHOLD=35
MACRO_RISK_HEADLINE_LIMIT=12
MACRO_RISK_MANUAL_LEVEL=
MACRO_RISK_MANUAL_NOTE=
```

Manual macro override examples during unusual events:

```env
MACRO_RISK_MANUAL_LEVEL=caution
MACRO_RISK_MANUAL_NOTE=Election/tariff headline risk; demand stronger confirmation.
```

```env
MACRO_RISK_MANUAL_LEVEL=block
MACRO_RISK_MANUAL_NOTE=War/geopolitical shock; avoid fresh entries until market stabilizes.
```

## Balanced Strict Backtest Fix

The Research Lab strict mode was too aggressive and could produce `0 trades tested` even when the historical universe had usable quality stocks. This build separates research strictness from live execution strictness:

- Live/paper automation remains strict.
- Backtesting now blocks true no-trade setups, but can still test watch-only setups so the system has enough historical examples to learn from.
- Backtest runs now record rejection reason counts, such as low setup score, weak reward vs risk, weak volume confirmation, risky market type, quality/liquidity gate, or bad historical pattern.
- Opening a full backtest row now explains why trades were skipped instead of only showing `0`.

New controls:

```env
STRICT_BACKTEST_ALLOW_WATCH_ONLY=true
STRICT_BACKTEST_MIN_RISK_REWARD=1.0
STRICT_BACKTEST_MIN_VOLUME_RATIO=0.45
STRICT_BACKTEST_REJECT_RISK_OFF=false
STRICT_BACKTEST_MIN_SCORE=35
```

Recommended Render web UI settings after this fix:

```text
Universe limit: 25–50
Stocks to test each time: 3–5
Run historical test every X days: 2–5
Strict Learning Mode: ON
```

## Duplicate Learning Protection + Clear Stock Selection

This build adds duplicate-learning protection for Research Lab backtests.

Every backtest now gets a fingerprint based on:

- Market and horizon
- Start and end date
- Requested universe size
- Exact cleaned universe list
- Stocks picked per test day
- Step days
- Strict Learning Mode settings
- Quality-stock filter thresholds
- Execution-cost assumptions
- Data-provider/ticker-alias settings

If the exact same fingerprint already exists, the app reuses the existing result and does **not** update strategy memory again. This prevents repeated identical backtests from making the system overconfident by double-counting the same evidence.

New controls:

```env
BACKTEST_DUPLICATE_PROTECTION=true
BACKTEST_ALLOW_DUPLICATE_LEARNING=false
BACKTEST_FINGERPRINT_VERSION=v1-strict-quality
```

How the 50-stock example works:

1. **Quality stocks to scan = 50** means the app starts with the first 50 stocks from the selected market universe.
2. It first keeps only tradeable quality stocks: active symbols, enough clean history, usable volume, low zero-volume days, price above minimum, and sufficient turnover.
3. On each old test date, every remaining quality stock is scored using bullish setup, trend, RSI, MACD, volume confirmation, risk/reward, broad market type, learned pattern history, and stock-specific historical behavior.
4. **Stocks picked each test day** means the maximum number of best-ranked setups to simulate on that date. If only 2 setups pass filters, it tests 2. If none pass, it tests 0 and records why.

So quality universe selection is mostly about **tradeability and clean data**. Daily pick selection is about **bullish behavior, setup quality, volume, risk/reward, market context, and learned stock/pattern behavior**.

## Post-entry risk recheck engine

This build adds a post-entry risk recheck layer for intraday, day and week positions. It is rule-based and does not call Claude during execution. Its purpose is to protect capital and profits without panic-selling positions that may still work.

Policy:
- Target/stop-loss remain hard exit levels.
- Winners are allowed to continue when the setup is still valid.
- Weekly profit target is treated as a milestone, not a hard ceiling by default. If you explicitly enable “Use weekly target as hard ceiling,” new entries pause after the target.
- A losing position is not exited just because it is slightly red. It exits early only when multiple weakening/risk signals appear.
- Moderate risk tightens the stop instead of immediately exiting.
- Every exit/tightened stop writes a plain-English reason into `trade_positions.metadata_json.exit_engine`.

New controls:

```env
POST_ENTRY_RISK_RECHECK_ENABLED=true
POST_ENTRY_TIGHTEN_SCORE_THRESHOLD=45
POST_ENTRY_EXIT_SCORE_THRESHOLD=70
POST_ENTRY_PROFIT_EXIT_SCORE_THRESHOLD=85
POST_ENTRY_MIN_WEAK_SIGNALS=3
POST_ENTRY_TIGHTEN_BUFFER_PCT=0.45
POST_ENTRY_LOSS_EXIT_MIN_PNL_PCT=-0.25
POST_ENTRY_ALLOW_TIGHTEN_STOP=true
TRADING_STOP_AFTER_WEEKLY_TARGET=false
```

Signals checked after entry:
- intraday momentum falling
- price below VWAP
- daily closes weakening
- price below 20-day trend
- weak/negative MACD
- RSI weakening
- weak volume while the position is losing
- historically weak pattern memory
- bearish broad market trend
- macro/news risk such as war, tariffs, rates, oil shock or regulation risk

Possible actions:
- `HOLD` — setup is still valid.
- `TIGHTEN_STOP` — weakness exists but not enough for a full exit.
- `POST_ENTRY_CAPITAL_PROTECTION_EXIT` — losing position plus confirmed weakness/risk cluster.
- `POST_ENTRY_PROFIT_PROTECTION_EXIT` — profitable position but serious reversal/risk cluster.

## Export Fix + Research Lab Backup + Data Integrity Guard

This build fixes the `/api/export/manifest` crash caused by older databases where `trade_positions.engine` did not exist yet. Export summaries now detect available columns dynamically and infer the engine from metadata/horizon when needed, instead of failing.

Research Lab now has an **Export all research/data** button and a **ZIP backup** button. The main Export Data tab still downloads the full database export.

Export includes:
- predictions and correctness rates
- scan cache and scan runs
- prediction lessons
- API cost logs
- trading and intraday settings
- orders and positions
- intraday check/run history
- Research Lab backtest runs
- every backtest trade
- learning rules / strategy memory
- duplicate-learning fingerprints
- config_json and metrics_json, including data-integrity notes for new runs

New data integrity controls:

```env
BACKTEST_DATA_INTEGRITY_ENABLED=true
BACKTEST_PRICE_MODE=raw
BACKTEST_MAX_SUSPICIOUS_CANDLES=3
BACKTEST_MAX_DAILY_RANGE_PCT=35
BACKTEST_MAX_CLOSE_GAP_PCT=45
BACKTEST_MIN_DATA_QUALITY_SCORE=70
BACKTEST_BLOCK_LEARNING_ON_DATA_WARNING=true
```

`BACKTEST_PRICE_MODE=raw` uses raw OHLC candles from yfinance (`auto_adjust=false`) so displayed historical prices are closer to exchange-style candles. `BACKTEST_PRICE_MODE=adjusted` uses adjusted OHLC candles, which can differ from Google/NSE raw values but remain internally consistent after corporate actions.

The data integrity guard blocks suspicious historical candles before they can teach the learning engine, so wrong/broken data is less likely to corrupt future pattern memory.

## Raw vs Protected Backtest Learning + Intraday Simulation Capital

This build adds an exit-aware backtest replay that keeps two learning records for every historical trade:

- **Raw baseline**: what the trade would have done with only target, stop-loss and time/horizon exit.
- **Protected replay**: what the trade would have done if the post-entry capital/profit protection rules were active.

The dashboard can now show raw vs protected return for each historical trade. Overall backtest metrics use the selected return mode, while learning rules still use the raw entry baseline so avoided losses do not hide bad entries.

New controls:

```env
BACKTEST_EXIT_AWARE_ENABLED=true
BACKTEST_RETURN_MODE=protected
BACKTEST_PROTECTED_MIN_WEAK_SIGNALS=3
BACKTEST_PROTECTED_LOSS_EXIT_PCT=-0.25
BACKTEST_PROTECTED_PROFIT_FADE_MIN_PCT=0.20
```

`BACKTEST_RETURN_MODE=protected` means the main backtest cards show the protected/exit-aware result. `BACKTEST_RETURN_MODE=raw` shows the old target/stop/time result only. In both modes, the system keeps entry-risk learning separate from exit-quality learning.

The Intraday Engine page now also has **Paper simulation capital ₹**. This value controls intraday paper/assisted position sizing from the Intraday page, so users do not need to search the Trading Automation page just to set simulation capital.

## Protected Backtest Exit Price Model Fix

Protected exits now no longer only relabel the same horizon/close price. Daily/week backtests still use OHLC bars, not tick-by-tick data, so exact intraday exit timing is impossible without intraday candles. To make the protected replay useful while staying transparent, this build adds a protective-threshold model:

```env
BACKTEST_PROTECTED_EXIT_PRICE_MODEL=threshold
```

When a candle proves the capital-protection threshold was touched and multiple weakening/risk signals are present, protected replay exits at the configured protective threshold. If a later bar opens beyond the threshold, it exits at the worse open. Set `BACKTEST_PROTECTED_EXIT_PRICE_MODEL=close` to use the older conservative close-only replay.

This means Research Lab can now show a real difference between:
- raw baseline result, and
- protected capital/profit replay result,

while still storing raw entry learning so avoided losses do not hide risky setups.

## Paper auto scheduler + separated learning (latest update)

This build adds a paper-only scheduler that can check both engines automatically when configured markets are open:

- Daily/weekly paper simulation: uses locked day/week scan results and `engine=trading_automation`.
- Intraday paper simulation: uses the 5-minute intraday scanner and `engine=intraday`.
- India and US paper sizing are separated: India uses INR capital, US uses USD capital.
- P/L is recorded on `trade_positions` with the market, currency symbol, horizon and engine so learning/profit reporting is not mixed between daily/week and intraday.
- If the selected stock price is higher than available simulation capital or the allowed max position value, the trade is skipped and the skipped reason is shown in Entry checks.
- Paper auto never submits live orders. Live execution still requires explicit `LIVE_TRADING_ENABLED=true` plus broker integration.

Useful env vars:

```env
TRADING_DEFAULT_CAPITAL_INR=5000
TRADING_DEFAULT_CAPITAL_USD=1000
PAPER_AUTO_ENABLED=false
PAPER_AUTO_ON_START=false
PAPER_AUTO_MARKETS=IN,US
PAPER_AUTO_HORIZONS=day,week
PAPER_AUTO_INTRADAY_MARKETS=IN,US
PAPER_AUTO_INTERVAL_SECONDS=300
PAPER_AUTO_REQUIRE_MARKET_OPEN=true
PAPER_AUTO_FORCE_PAPER_MODE=true
```

Manual endpoints:

```text
GET  /api/paper-auto
POST /api/paper-auto/run
POST /api/paper-auto/start
POST /api/paper-auto/stop
```

