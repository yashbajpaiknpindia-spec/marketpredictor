import os
import sys
import json
import re
import time
import io
import csv
import zipfile
import datetime
import hashlib
import argparse
import logging
import threading
import concurrent.futures
import math
import urllib.request
import urllib.error
from decimal import Decimal
from zoneinfo import ZoneInfo
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
from flask import Flask, render_template, jsonify, request, Response
import yfinance as yf
logging.getLogger('yfinance').setLevel(logging.CRITICAL)
import anthropic
import psycopg2
import psycopg2.extras

app = Flask(__name__)

DATABASE_URL = os.environ.get('DATABASE_URL')
YF_TIMEOUT = int(os.environ.get('YF_TIMEOUT', '10'))

# Background scan sizing. The expensive stages (fundamentals, news, Claude AI)
# only run after the liquidity/momentum/technical funnel has narrowed the list.
BATCH_SIZE = int(os.environ.get('SCAN_BATCH_SIZE', '75'))
MOMENTUM_STAGE_LIMIT = int(os.environ.get('MOMENTUM_STAGE_LIMIT', '300'))
TECHNICAL_STAGE_LIMIT = int(os.environ.get('TECHNICAL_STAGE_LIMIT', '150'))
DEEP_ANALYSIS_LIMIT = int(os.environ.get('DEEP_ANALYSIS_LIMIT', '75'))
FINAL_PICK_LIMIT = int(os.environ.get('FINAL_PICK_LIMIT', '10'))
CACHE_TTL_MINUTES = int(os.environ.get('SCAN_CACHE_TTL_MINUTES', '180'))
MAX_BACKGROUND_WORKERS = int(os.environ.get('MAX_BACKGROUND_WORKERS', '8'))
FIRST_SCAN_RUNNING_TIMEOUT_MINUTES = int(os.environ.get('FIRST_SCAN_RUNNING_TIMEOUT_MINUTES', '120'))

# Prediction governance. Public users should see a locked, stable prediction for
# the current period. A manual background scan will not overwrite today's daily
# prediction or the current weekly prediction unless --force is used.
PREDICTION_LOCK_ENABLED = os.environ.get('PREDICTION_LOCK_ENABLED', 'true').lower() != 'false'
ACCURACY_CHECK_ON_SCAN = os.environ.get('ACCURACY_CHECK_ON_SCAN', 'true').lower() != 'false'
MIN_BUY_CONFIDENCE = int(os.environ.get('MIN_BUY_CONFIDENCE', '60'))
MIN_BUY_RISK_REWARD = float(os.environ.get('MIN_BUY_RISK_REWARD', '1.4'))
VERIFY_AFTER_CLOSE_DELAY_MINUTES = int(os.environ.get('VERIFY_AFTER_CLOSE_DELAY_MINUTES', '15'))
LESSON_LOOKBACK_DAYS = int(os.environ.get('LESSON_LOOKBACK_DAYS', '90'))
MAX_LEARNING_LESSONS = int(os.environ.get('MAX_LEARNING_LESSONS', '60'))

# Paid API cost controls. Defaults are deliberately environment-overridable so
# you can keep exact accounting aligned with your Anthropic account/pricing tier.
ANTHROPIC_INPUT_USD_PER_MILLION = float(os.environ.get('ANTHROPIC_INPUT_USD_PER_MILLION', '2'))
ANTHROPIC_OUTPUT_USD_PER_MILLION = float(os.environ.get('ANTHROPIC_OUTPUT_USD_PER_MILLION', '10'))
USD_INR_RATE = float(os.environ.get('USD_INR_RATE', '95'))
SCAN_ESTIMATED_INPUT_TOKENS_BASE = int(os.environ.get('SCAN_ESTIMATED_INPUT_TOKENS_BASE', '4500'))
SCAN_ESTIMATED_INPUT_TOKENS_PER_CANDIDATE = int(os.environ.get('SCAN_ESTIMATED_INPUT_TOKENS_PER_CANDIDATE', '350'))
SCAN_ESTIMATED_OUTPUT_TOKENS = int(os.environ.get('SCAN_ESTIMATED_OUTPUT_TOKENS', '2500'))

# Paid-AI budget guard. Claude should be used only for locked scan/ranking and
# optional batch learning, never for every buy/sell. If these limits are reached,
# scans fall back to deterministic public-rule ranking instead of spending again.
AI_MAX_PAID_CALLS_PER_DAY = int(os.environ.get('AI_MAX_PAID_CALLS_PER_DAY', '2'))
AI_MAX_PAID_CALLS_PER_WEEK = int(os.environ.get('AI_MAX_PAID_CALLS_PER_WEEK', '8'))
AI_MAX_COST_INR_PER_DAY = float(os.environ.get('AI_MAX_COST_INR_PER_DAY', '75'))
AI_MAX_COST_INR_PER_WEEK = float(os.environ.get('AI_MAX_COST_INR_PER_WEEK', '300'))
AI_ALLOW_FORCE_OVER_BUDGET = os.environ.get('AI_ALLOW_FORCE_OVER_BUDGET', 'false').lower() == 'true'
LEARNING_AI_ENABLED = os.environ.get('LEARNING_AI_ENABLED', 'false').lower() == 'true'

# Fixed broker/data API subscription cost allocation. Set this to your Groww/API
# subscription amount so weekly target math covers fixed costs from realised profit.
BROKER_API_MONTHLY_COST_INR = float(os.environ.get('BROKER_API_MONTHLY_COST_INR', '0'))

# Trading automation is intentionally paper/assisted by default. Live broker
# execution must be explicitly enabled with env vars and should only be used
# through a SEBI/broker-compliant API flow with audit trails.
TRADING_ENABLED = os.environ.get('TRADING_ENABLED', 'true').lower() != 'false'
AUTO_TRADING_MODE = os.environ.get('AUTO_TRADING_MODE', 'paper').lower()  # paper, assisted, live
LIVE_TRADING_ENABLED = os.environ.get('LIVE_TRADING_ENABLED', 'false').lower() == 'true'
BROKER_NAME = os.environ.get('BROKER_NAME', 'manual-webhook')
BROKER_ORDER_WEBHOOK_URL = os.environ.get('BROKER_ORDER_WEBHOOK_URL', '')
BROKER_ORDER_WEBHOOK_SECRET = os.environ.get('BROKER_ORDER_WEBHOOK_SECRET', '')

# Transparent market-data source display. This build fetches built-in prices/candles
# through yfinance/Yahoo Finance. Groww can be configured later as an official data
# adapter, but until that adapter is present the UI must show yfinance fallback.
MARKET_DATA_PROVIDER = os.environ.get('MARKET_DATA_PROVIDER', 'yfinance').strip().lower() or 'yfinance'
GROWW_MARKET_DATA_ENABLED = os.environ.get('GROWW_MARKET_DATA_ENABLED', 'false').lower() == 'true'
TRADING_DEFAULT_CAPITAL_INR = float(os.environ.get('TRADING_DEFAULT_CAPITAL_INR', '5000'))
TRADING_DEFAULT_CAPITAL_USD = float(os.environ.get('TRADING_DEFAULT_CAPITAL_USD', '1000'))
PAPER_AUTO_ENABLED = os.environ.get('PAPER_AUTO_ENABLED', 'false').lower() == 'true'
PAPER_AUTO_ON_START = os.environ.get('PAPER_AUTO_ON_START', 'false').lower() == 'true'
PAPER_AUTO_MARKETS = os.environ.get('PAPER_AUTO_MARKETS', 'IN,US')
PAPER_AUTO_HORIZONS = os.environ.get('PAPER_AUTO_HORIZONS', 'day,week')
PAPER_AUTO_INTRADAY_MARKETS = os.environ.get('PAPER_AUTO_INTRADAY_MARKETS', 'IN,US')
PAPER_AUTO_INTERVAL_SECONDS = int(os.environ.get('PAPER_AUTO_INTERVAL_SECONDS', '300'))
PAPER_AUTO_REQUIRE_MARKET_OPEN = os.environ.get('PAPER_AUTO_REQUIRE_MARKET_OPEN', 'true').lower() != 'false'
PAPER_AUTO_FORCE_PAPER_MODE = os.environ.get('PAPER_AUTO_FORCE_PAPER_MODE', 'true').lower() != 'false'
TRADING_MAX_POSITION_PCT = float(os.environ.get('TRADING_MAX_POSITION_PCT', '25'))
TRADING_RISK_PER_TRADE_PCT = float(os.environ.get('TRADING_RISK_PER_TRADE_PCT', '2'))
TRADING_DAILY_LOSS_LIMIT_PCT = float(os.environ.get('TRADING_DAILY_LOSS_LIMIT_PCT', '5'))
TRADING_MAX_OPEN_POSITIONS = int(os.environ.get('TRADING_MAX_OPEN_POSITIONS', '3'))
TRADING_MIN_CONFIDENCE = int(os.environ.get('TRADING_MIN_CONFIDENCE', '65'))
TRADING_MIN_RISK_REWARD = float(os.environ.get('TRADING_MIN_RISK_REWARD', '1.5'))
TRADING_ALLOW_US_WITH_INR_CAPITAL = os.environ.get('TRADING_ALLOW_US_WITH_INR_CAPITAL', 'false').lower() == 'true'
TRADING_IDEMPOTENCY_HOURS = int(os.environ.get('TRADING_IDEMPOTENCY_HOURS', '24'))
TRADING_WEEKLY_PROFIT_TARGET_PCT = float(os.environ.get('TRADING_WEEKLY_PROFIT_TARGET_PCT', '10'))
TRADING_MAX_WEEKLY_LOSS_PCT = float(os.environ.get('TRADING_MAX_WEEKLY_LOSS_PCT', '4'))
TRADING_COVER_API_COSTS_FROM_PROFIT = os.environ.get('TRADING_COVER_API_COSTS_FROM_PROFIT', 'true').lower() != 'false'
TRADING_STOP_AFTER_WEEKLY_TARGET = os.environ.get('TRADING_STOP_AFTER_WEEKLY_TARGET', 'false').lower() != 'false'
TRADING_STOP_AFTER_WEEKLY_LOSS = os.environ.get('TRADING_STOP_AFTER_WEEKLY_LOSS', 'true').lower() != 'false'
TRADING_BROKER_API_MONTHLY_COST_INR = float(os.environ.get('TRADING_BROKER_API_MONTHLY_COST_INR', str(BROKER_API_MONTHLY_COST_INR)))

# Deterministic live-entry and exit guards. These are rule-based and must not
# call Claude during the trading session. They confirm opening price action,
# avoid chasing, and protect profits before a setup fully reaches its target.
OPENING_CONFIRMATION_ENABLED = os.environ.get('OPENING_CONFIRMATION_ENABLED', 'true').lower() != 'false'
OPENING_CONFIRMATION_WAIT_MINUTES = int(os.environ.get('OPENING_CONFIRMATION_WAIT_MINUTES', '15'))
OPENING_MAX_ENTRY_CHASE_PCT = float(os.environ.get('OPENING_MAX_ENTRY_CHASE_PCT', '1.25'))
OPENING_MAX_ENTRY_PULLBACK_PCT = float(os.environ.get('OPENING_MAX_ENTRY_PULLBACK_PCT', '1.0'))
OPENING_MIN_VOLUME_MULTIPLIER = float(os.environ.get('OPENING_MIN_VOLUME_MULTIPLIER', '0.0'))  # 0 disables volume gate
PROFIT_PROTECTION_ENABLED = os.environ.get('PROFIT_PROTECTION_ENABLED', 'true').lower() != 'false'
PROFIT_PROTECT_PROGRESS_PCT = float(os.environ.get('PROFIT_PROTECT_PROGRESS_PCT', '50'))
TRAILING_STOP_ACTIVATION_PCT = float(os.environ.get('TRAILING_STOP_ACTIVATION_PCT', '70'))
TRAILING_STOP_GIVEBACK_PCT = float(os.environ.get('TRAILING_STOP_GIVEBACK_PCT', '45'))
EXIT_AT_HORIZON_END = os.environ.get('EXIT_AT_HORIZON_END', 'true').lower() != 'false'

# Post-entry risk recheck. This is the capital-protection layer that runs after
# entry. It must not panic-sell profitable positions; it exits only when multiple
# weakening/risk signals appear. Otherwise it can tighten the stop and record why.
POST_ENTRY_RISK_RECHECK_ENABLED = os.environ.get('POST_ENTRY_RISK_RECHECK_ENABLED', 'true').lower() != 'false'
POST_ENTRY_TIGHTEN_SCORE_THRESHOLD = float(os.environ.get('POST_ENTRY_TIGHTEN_SCORE_THRESHOLD', '45'))
POST_ENTRY_EXIT_SCORE_THRESHOLD = float(os.environ.get('POST_ENTRY_EXIT_SCORE_THRESHOLD', '70'))
POST_ENTRY_PROFIT_EXIT_SCORE_THRESHOLD = float(os.environ.get('POST_ENTRY_PROFIT_EXIT_SCORE_THRESHOLD', '85'))
POST_ENTRY_MIN_WEAK_SIGNALS = int(os.environ.get('POST_ENTRY_MIN_WEAK_SIGNALS', '3'))
POST_ENTRY_TIGHTEN_BUFFER_PCT = float(os.environ.get('POST_ENTRY_TIGHTEN_BUFFER_PCT', '0.45'))
POST_ENTRY_LOSS_EXIT_MIN_PNL_PCT = float(os.environ.get('POST_ENTRY_LOSS_EXIT_MIN_PNL_PCT', '-0.25'))
POST_ENTRY_ALLOW_TIGHTEN_STOP = os.environ.get('POST_ENTRY_ALLOW_TIGHTEN_STOP', 'true').lower() != 'false'


# Intraday opportunity engine. This is deliberately separate from the AI scanner:
# it watches 5-minute price action and executes only deterministic quick-trade
# rules. It must not call Claude during live market hours.
INTRADAY_ENGINE_ENABLED = os.environ.get('INTRADAY_ENGINE_ENABLED', 'false').lower() == 'true'
INTRADAY_DEFAULT_MARKET = os.environ.get('INTRADAY_DEFAULT_MARKET', 'IN')
INTRADAY_DEFAULT_MODE = os.environ.get('INTRADAY_DEFAULT_MODE', 'paper').lower()  # paper, assisted, live
INTRADAY_REQUIRE_CONFIRMATION = os.environ.get('INTRADAY_REQUIRE_CONFIRMATION', 'true').lower() != 'false'
INTRADAY_AUTO_ENABLED = os.environ.get('INTRADAY_AUTO_ENABLED', 'false').lower() == 'true'
INTRADAY_AUTO_INTERVAL_SECONDS = int(os.environ.get('INTRADAY_AUTO_INTERVAL_SECONDS', '300'))
INTRADAY_SERVER_AUTO_ON_START = os.environ.get('INTRADAY_SERVER_AUTO_ON_START', 'false').lower() == 'true'
INTRADAY_OPENING_WAIT_MINUTES = int(os.environ.get('INTRADAY_OPENING_WAIT_MINUTES', '15'))
INTRADAY_UNIVERSE_LIMIT = int(os.environ.get('INTRADAY_UNIVERSE_LIMIT', '60'))
INTRADAY_MAX_CANDIDATES = int(os.environ.get('INTRADAY_MAX_CANDIDATES', '8'))
INTRADAY_MIN_SCORE = int(os.environ.get('INTRADAY_MIN_SCORE', '70'))
INTRADAY_MIN_PRICE_CHANGE_PCT = float(os.environ.get('INTRADAY_MIN_PRICE_CHANGE_PCT', '0.35'))
# Low-capital cost-efficiency filter: avoid high-price qty-1 trades and penny/illiquid names.
# A stock's nominal price is not risk by itself, but lower-priced liquid stocks let
# the engine buy enough quantity for target profit to clear fixed brokerage/tax costs.
INTRADAY_PRICE_FILTER_ENABLED = os.environ.get('INTRADAY_PRICE_FILTER_ENABLED', 'true').lower() != 'false'
INTRADAY_MIN_STOCK_PRICE_INR = float(os.environ.get('INTRADAY_MIN_STOCK_PRICE_INR', '50'))
INTRADAY_MAX_STOCK_PRICE_INR = float(os.environ.get('INTRADAY_MAX_STOCK_PRICE_INR', '1500'))
INTRADAY_MIN_STOCK_PRICE_USD = float(os.environ.get('INTRADAY_MIN_STOCK_PRICE_USD', '2'))
INTRADAY_MAX_STOCK_PRICE_USD = float(os.environ.get('INTRADAY_MAX_STOCK_PRICE_USD', '250'))
INTRADAY_MIN_ORDER_QUANTITY = int(os.environ.get('INTRADAY_MIN_ORDER_QUANTITY', '5'))
INTRADAY_MIN_POSITION_VALUE_INR = float(os.environ.get('INTRADAY_MIN_POSITION_VALUE_INR', '10000'))
INTRADAY_MIN_POSITION_VALUE_USD = float(os.environ.get('INTRADAY_MIN_POSITION_VALUE_USD', '500'))
INTRADAY_MAX_CHASE_PCT = float(os.environ.get('INTRADAY_MAX_CHASE_PCT', '3.0'))
INTRADAY_REQUIRE_VWAP = os.environ.get('INTRADAY_REQUIRE_VWAP', 'true').lower() != 'false'
INTRADAY_REQUIRE_OPENING_BREAKOUT = os.environ.get('INTRADAY_REQUIRE_OPENING_BREAKOUT', 'true').lower() != 'false'
INTRADAY_MIN_VOLUME_MULTIPLIER = float(os.environ.get('INTRADAY_MIN_VOLUME_MULTIPLIER', '1.2'))
INTRADAY_QUICK_TARGET_PCT = float(os.environ.get('INTRADAY_QUICK_TARGET_PCT', '0.8'))
INTRADAY_MAX_STOP_LOSS_PCT = float(os.environ.get('INTRADAY_MAX_STOP_LOSS_PCT', '0.6'))
INTRADAY_MIN_RISK_REWARD = float(os.environ.get('INTRADAY_MIN_RISK_REWARD', '1.2'))
INTRADAY_MAX_TRADES_PER_DAY = int(os.environ.get('INTRADAY_MAX_TRADES_PER_DAY', '3'))
INTRADAY_PROFIT_BOOK_PCT = float(os.environ.get('INTRADAY_PROFIT_BOOK_PCT', '0.8'))
INTRADAY_FORCE_EXIT_BEFORE_CLOSE_MINUTES = int(os.environ.get('INTRADAY_FORCE_EXIT_BEFORE_CLOSE_MINUTES', '15'))
INTRADAY_MAX_WORKERS = int(os.environ.get('INTRADAY_MAX_WORKERS', '8'))
INTRADAY_DATA_INTERVAL = os.environ.get('INTRADAY_DATA_INTERVAL', '1m').strip() or '1m'
INTRADAY_MAX_DATA_AGE_MINUTES = int(os.environ.get('INTRADAY_MAX_DATA_AGE_MINUTES', '12'))
INTRADAY_BLOCK_STALE_DATA = os.environ.get('INTRADAY_BLOCK_STALE_DATA', 'true').lower() != 'false'
INTRADAY_VOLUME_CONFIRMATION_MODE = os.environ.get('INTRADAY_VOLUME_CONFIRMATION_MODE', 'soft').lower()  # soft or hard
INTRADAY_AUTO_CONTINUE_AFTER_CLOSE = os.environ.get('INTRADAY_AUTO_CONTINUE_AFTER_CLOSE', 'true').lower() != 'false'
# Dynamic intraday universe selection. The old engine scanned the first N symbols
# from the configured list. The upgraded engine first does a lightweight live
# pre-scan across a broader configured NSE/US universe, shortlists the most
# active symbols by move + volume + volatility + opening-range pressure, then
# runs the heavier detailed entry rules on that shortlist. This keeps Render
# load controlled without making the first 60 names permanently favored.
INTRADAY_DYNAMIC_UNIVERSE_ENABLED = os.environ.get('INTRADAY_DYNAMIC_UNIVERSE_ENABLED', 'true').lower() != 'false'
INTRADAY_PRE_SCAN_POOL_LIMIT = int(os.environ.get('INTRADAY_PRE_SCAN_POOL_LIMIT', '300'))
INTRADAY_PRE_SCAN_BATCH_SIZE = int(os.environ.get('INTRADAY_PRE_SCAN_BATCH_SIZE', '60'))
INTRADAY_PRE_SCAN_MIN_BARS = int(os.environ.get('INTRADAY_PRE_SCAN_MIN_BARS', '3'))

# Claude intraday control. This gives Claude bounded decision control in paper mode,
# while the deterministic engine still enforces non-negotiable hard risk gates:
# fresh data, cost coverage, stop-loss, max trades, max positions, and loss guards.
# Modes: off, review_only, approval_required, paper_full_control. Paper full control
# can choose among engine-approved plans and suggest target/stop tweaks, but it never
# bypasses hard gates and is downgraded outside paper simulation.
CLAUDE_INTRADAY_CONTROL_MODE = os.environ.get('CLAUDE_INTRADAY_CONTROL_MODE', 'off').strip().lower()
CLAUDE_INTRADAY_ALLOWED_MODES = {'off', 'review_only', 'approval_required', 'paper_full_control'}
if CLAUDE_INTRADAY_CONTROL_MODE not in CLAUDE_INTRADAY_ALLOWED_MODES:
    CLAUDE_INTRADAY_CONTROL_MODE = 'off'
CLAUDE_INTRADAY_MAX_REVIEWS_PER_RUN = int(os.environ.get('CLAUDE_INTRADAY_MAX_REVIEWS_PER_RUN', '5'))
CLAUDE_INTRADAY_MIN_CONFIDENCE = int(os.environ.get('CLAUDE_INTRADAY_MIN_CONFIDENCE', '70'))
CLAUDE_INTRADAY_MAX_TOKENS = int(os.environ.get('CLAUDE_INTRADAY_MAX_TOKENS', '2500'))
CLAUDE_INTRADAY_TARGET_INSTRUCTION = os.environ.get(
    'CLAUDE_INTRADAY_TARGET_INSTRUCTION',
    'Maximize net expectancy after all costs. Prefer no trade over a weak trade. Approve only trades with fresh data, clear momentum, valid stop, realistic target, and enough expected net profit after costs.'
).strip()

# Trading Automation selection mode. Locked-scan mode keeps the original behavior:
# it trades only from the saved First Scan prediction. Self-scan mode lets the
# automation build its own deterministic expert-style shortlist from the market
# universe when no scanner prediction is available. Auto mode tries the locked
# prediction first, then falls back to self-scan. Buy/sell execution remains
# deterministic; self-scan does not call Claude by default.
TRADING_SELECTION_MODE_DEFAULT = os.environ.get('TRADING_SELECTION_MODE', 'locked_scan').lower()
TRADING_SELF_SCAN_UNIVERSE_LIMIT = int(os.environ.get('TRADING_SELF_SCAN_UNIVERSE_LIMIT', '150'))
TRADING_SELF_SCAN_MIN_SCORE = float(os.environ.get('TRADING_SELF_SCAN_MIN_SCORE', '55'))


# Research / validation engine. These controls make the system prove strategies
# before scaling: point-in-time backtests, walk-forward splits, execution costs,
# regime classification, and portfolio risk analytics. Backtests use only historical
# bars available before each simulated decision and do not call Claude by default.
BACKTEST_ENABLED = os.environ.get('BACKTEST_ENABLED', 'true').lower() != 'false'
BACKTEST_DEFAULT_LOOKBACK_YEARS = int(os.environ.get('BACKTEST_DEFAULT_LOOKBACK_YEARS', '3'))
BACKTEST_UNIVERSE_LIMIT = int(os.environ.get('BACKTEST_UNIVERSE_LIMIT', '40'))
BACKTEST_TOP_N = int(os.environ.get('BACKTEST_TOP_N', '5'))
BACKTEST_REBALANCE_STEP_DAYS = int(os.environ.get('BACKTEST_REBALANCE_STEP_DAYS', '5'))
BACKTEST_MIN_HISTORY_DAYS = int(os.environ.get('BACKTEST_MIN_HISTORY_DAYS', '90'))
BACKTEST_MAX_WORKERS = int(os.environ.get('BACKTEST_MAX_WORKERS', '6'))
BACKTEST_MAX_RUNTIME_SECONDS = int(os.environ.get('BACKTEST_MAX_RUNTIME_SECONDS', '24'))
BACKTEST_WEB_MAX_UNIVERSE_LIMIT = int(os.environ.get('BACKTEST_WEB_MAX_UNIVERSE_LIMIT', '25'))
BACKTEST_STOCK_STATS_LIMIT = int(os.environ.get('BACKTEST_STOCK_STATS_LIMIT', '80'))
BACKTEST_DUPLICATE_PROTECTION = os.environ.get('BACKTEST_DUPLICATE_PROTECTION', 'true').lower() != 'false'
BACKTEST_ALLOW_DUPLICATE_LEARNING = os.environ.get('BACKTEST_ALLOW_DUPLICATE_LEARNING', 'false').lower() == 'true'
BACKTEST_FINGERPRINT_VERSION = os.environ.get('BACKTEST_FINGERPRINT_VERSION', 'v1-strict-quality')
LEARNING_PATTERN_MIN_TRADES = int(os.environ.get('LEARNING_PATTERN_MIN_TRADES', '3'))
LEARNING_BAD_PATTERN_WIN_RATE_PCT = float(os.environ.get('LEARNING_BAD_PATTERN_WIN_RATE_PCT', '45'))
LEARNING_GOOD_PATTERN_WIN_RATE_PCT = float(os.environ.get('LEARNING_GOOD_PATTERN_WIN_RATE_PCT', '58'))
BACKTEST_WALK_FORWARD_TRAIN_MONTHS = int(os.environ.get('BACKTEST_WALK_FORWARD_TRAIN_MONTHS', '9'))
BACKTEST_WALK_FORWARD_TEST_MONTHS = int(os.environ.get('BACKTEST_WALK_FORWARD_TEST_MONTHS', '3'))

# Strict Learning Mode. The goal is deliberately conservative: trade less, filter
# harder, trust only historically durable setups, and allow a no-trade outcome
# when the statistical edge is weak. Daily backtest memory improves the main
# prediction engine; closed trade history improves automation; intraday history
# improves the intraday engine separately.
STRICT_LEARNING_ENABLED = os.environ.get('STRICT_LEARNING_ENABLED', 'true').lower() != 'false'
STRICT_MIN_SCORE = float(os.environ.get('STRICT_MIN_SCORE', '60'))
STRICT_BUY_MIN_SCORE = float(os.environ.get('STRICT_BUY_MIN_SCORE', '65'))
STRICT_BACKTEST_MIN_SCORE = float(os.environ.get('STRICT_BACKTEST_MIN_SCORE', '35'))
STRICT_MIN_RISK_REWARD = float(os.environ.get('STRICT_MIN_RISK_REWARD', str(MIN_BUY_RISK_REWARD)))
STRICT_MIN_VOLUME_RATIO = float(os.environ.get('STRICT_MIN_VOLUME_RATIO', '0.8'))
STRICT_AVOID_RISK_OFF = os.environ.get('STRICT_AVOID_RISK_OFF', 'true').lower() != 'false'
STRICT_INTRADAY_BLOCK_RISK_OFF = os.environ.get('STRICT_INTRADAY_BLOCK_RISK_OFF', 'false').lower() == 'true'
STRICT_INTRADAY_BLOCK_MACRO_RISK = os.environ.get('STRICT_INTRADAY_BLOCK_MACRO_RISK', 'false').lower() == 'true'
STRICT_PATTERN_RELIABLE_MIN_TRADES = int(os.environ.get('STRICT_PATTERN_RELIABLE_MIN_TRADES', '8'))
STRICT_PER_STOCK_MIN_TRADES = int(os.environ.get('STRICT_PER_STOCK_MIN_TRADES', '5'))
STRICT_PER_STOCK_MIN_WIN_RATE = float(os.environ.get('STRICT_PER_STOCK_MIN_WIN_RATE', '48'))
STRICT_PER_STOCK_MIN_EXPECTANCY = float(os.environ.get('STRICT_PER_STOCK_MIN_EXPECTANCY', '0'))
STRICT_TRADE_HISTORY_MIN_TRADES = int(os.environ.get('STRICT_TRADE_HISTORY_MIN_TRADES', '3'))
STRICT_INTRADAY_HISTORY_MIN_TRADES = int(os.environ.get('STRICT_INTRADAY_HISTORY_MIN_TRADES', '3'))
STRICT_REQUIRE_VALIDATION_EDGE = os.environ.get('STRICT_REQUIRE_VALIDATION_EDGE', 'false').lower() == 'true'
STRICT_NO_TRADE_IF_NO_BUY = os.environ.get('STRICT_NO_TRADE_IF_NO_BUY', 'true').lower() != 'false'
STRICT_BAD_PATTERN_MIN_PENALTY = float(os.environ.get('STRICT_BAD_PATTERN_MIN_PENALTY', '-0.5'))
# Backtest strictness is intentionally a little softer than live execution.
# Research Lab must still test enough historical setups to learn. Live/paper
# automation remains stricter and can block trades.
STRICT_BACKTEST_ALLOW_WATCH_ONLY = os.environ.get('STRICT_BACKTEST_ALLOW_WATCH_ONLY', 'true').lower() != 'false'
STRICT_BACKTEST_MIN_RISK_REWARD = float(os.environ.get('STRICT_BACKTEST_MIN_RISK_REWARD', '1.0'))
STRICT_BACKTEST_MIN_VOLUME_RATIO = float(os.environ.get('STRICT_BACKTEST_MIN_VOLUME_RATIO', '0.45'))
STRICT_BACKTEST_REJECT_RISK_OFF = os.environ.get('STRICT_BACKTEST_REJECT_RISK_OFF', 'false').lower() == 'true'
STRICT_BACKTEST_RELAXED_LABEL = 'balanced_research_mode'

# Quality-stock gate. Backtests and scans should learn from active, liquid,
# tradeable stocks only. Illiquid/old/unavailable symbols are skipped and shown
# in Research Lab instead of polluting accuracy.
QUALITY_STOCK_FILTER_ENABLED = os.environ.get('QUALITY_STOCK_FILTER_ENABLED', 'true').lower() != 'false'
QUALITY_MIN_PRICE = float(os.environ.get('QUALITY_MIN_PRICE', '10'))
QUALITY_MAX_ZERO_VOLUME_DAYS = int(os.environ.get('QUALITY_MAX_ZERO_VOLUME_DAYS', '8'))
QUALITY_MIN_RECENT_BARS = int(os.environ.get('QUALITY_MIN_RECENT_BARS', '60'))
QUALITY_MIN_AVG_VOLUME_MULTIPLIER = float(os.environ.get('QUALITY_MIN_AVG_VOLUME_MULTIPLIER', '1.0'))
QUALITY_MIN_TURNOVER_MULTIPLIER = float(os.environ.get('QUALITY_MIN_TURNOVER_MULTIPLIER', '1.0'))

# Backtest data integrity guard. Historical learning should never be recorded
# blindly from suspicious or inconsistent candle data. Price mode controls whether
# yfinance returns raw OHLC (closer to exchange candles) or adjusted OHLC
# (corporate-action adjusted). The selected mode is stored in backtest metadata
# and exported for audit.
BACKTEST_DATA_INTEGRITY_ENABLED = os.environ.get('BACKTEST_DATA_INTEGRITY_ENABLED', 'true').lower() != 'false'
BACKTEST_PRICE_MODE = os.environ.get('BACKTEST_PRICE_MODE', 'raw').strip().lower()  # raw or adjusted
if BACKTEST_PRICE_MODE not in ('raw', 'adjusted'):
    BACKTEST_PRICE_MODE = 'raw'
BACKTEST_MAX_SUSPICIOUS_CANDLES = int(os.environ.get('BACKTEST_MAX_SUSPICIOUS_CANDLES', '3'))
BACKTEST_MAX_DAILY_RANGE_PCT = float(os.environ.get('BACKTEST_MAX_DAILY_RANGE_PCT', '35'))
BACKTEST_MAX_CLOSE_GAP_PCT = float(os.environ.get('BACKTEST_MAX_CLOSE_GAP_PCT', '45'))
BACKTEST_MIN_DATA_QUALITY_SCORE = float(os.environ.get('BACKTEST_MIN_DATA_QUALITY_SCORE', '70'))
BACKTEST_BLOCK_LEARNING_ON_DATA_WARNING = os.environ.get('BACKTEST_BLOCK_LEARNING_ON_DATA_WARNING', 'true').lower() != 'false'

# Raw vs Protected Backtest Learning. Raw baseline records what would have happened
# with only target/stop/horizon. Protected mode then replays the same trade with
# post-entry risk checks so the system learns both: bad entry patterns and whether
# the exit engine reduced loss or missed profit.
BACKTEST_EXIT_AWARE_ENABLED = os.environ.get('BACKTEST_EXIT_AWARE_ENABLED', 'true').lower() != 'false'
BACKTEST_RETURN_MODE = os.environ.get('BACKTEST_RETURN_MODE', 'protected').strip().lower()  # protected or raw
if BACKTEST_RETURN_MODE not in ('protected', 'raw'):
    BACKTEST_RETURN_MODE = 'protected'
BACKTEST_PROTECTED_MIN_WEAK_SIGNALS = int(os.environ.get('BACKTEST_PROTECTED_MIN_WEAK_SIGNALS', str(POST_ENTRY_MIN_WEAK_SIGNALS)))
BACKTEST_PROTECTED_LOSS_EXIT_PCT = float(os.environ.get('BACKTEST_PROTECTED_LOSS_EXIT_PCT', str(POST_ENTRY_LOSS_EXIT_MIN_PNL_PCT)))
BACKTEST_PROTECTED_PROFIT_FADE_MIN_PCT = float(os.environ.get('BACKTEST_PROTECTED_PROFIT_FADE_MIN_PCT', '0.20'))
# Daily historical bars do not contain tick-by-tick exit timing. In threshold mode,
# if a daily/weekly candle proves the protective loss threshold was touched and
# stacked weakness is present, the protected replay exits at the protective
# threshold price. If price gaps beyond the threshold, it exits at the bar open.
# Set to close for a very conservative close-only replay.
BACKTEST_PROTECTED_EXIT_PRICE_MODEL = os.environ.get('BACKTEST_PROTECTED_EXIT_PRICE_MODEL', 'threshold').strip().lower()
if BACKTEST_PROTECTED_EXIT_PRICE_MODEL not in ('threshold', 'close'):
    BACKTEST_PROTECTED_EXIT_PRICE_MODEL = 'threshold'

# Current macro/news event risk layer. Used for forward-looking scans, trading
# automation and intraday controls. Historical backtests do not apply today’s
# headlines to old dates, which avoids lookahead bias.
MACRO_RISK_ENABLED = os.environ.get('MACRO_RISK_ENABLED', 'true').lower() != 'false'
MACRO_RISK_CAUTION_THRESHOLD = float(os.environ.get('MACRO_RISK_CAUTION_THRESHOLD', '18'))
MACRO_RISK_BLOCK_THRESHOLD = float(os.environ.get('MACRO_RISK_BLOCK_THRESHOLD', '35'))
MACRO_RISK_HEADLINE_LIMIT = int(os.environ.get('MACRO_RISK_HEADLINE_LIMIT', '12'))
MACRO_RISK_MANUAL_LEVEL = os.environ.get('MACRO_RISK_MANUAL_LEVEL', '').strip().lower()  # low/caution/high/block
MACRO_RISK_MANUAL_NOTE = os.environ.get('MACRO_RISK_MANUAL_NOTE', '').strip()

EXECUTION_SLIPPAGE_BPS = float(os.environ.get('EXECUTION_SLIPPAGE_BPS', '8'))
EXECUTION_SPREAD_BPS = float(os.environ.get('EXECUTION_SPREAD_BPS', '5'))
EXECUTION_FEE_BPS = float(os.environ.get('EXECUTION_FEE_BPS', '6'))

# Live/paper cost model. Backtests already had cost assumptions, but live/paper
# positions must also be closed on NET P&L. These defaults are intentionally
# conservative for Indian equity intraday paper trading, because many tiny gross
# wins disappear after brokerage, STT, exchange charges, GST, stamp duty and
# slippage. Tune these env vars to match your actual broker contract note.
TRADE_COST_MODEL_ENABLED = os.environ.get('TRADE_COST_MODEL_ENABLED', 'true').lower() != 'false'
INDIA_INTRADAY_BROKERAGE_PER_ORDER_INR = float(os.environ.get('INDIA_INTRADAY_BROKERAGE_PER_ORDER_INR', '20'))
INDIA_INTRADAY_BROKERAGE_PCT = float(os.environ.get('INDIA_INTRADAY_BROKERAGE_PCT', '0.03'))
INDIA_INTRADAY_BROKERAGE_USE_LOWER_OF = os.environ.get('INDIA_INTRADAY_BROKERAGE_USE_LOWER_OF', 'false').lower() == 'true'
INDIA_INTRADAY_MIN_ROUND_TRIP_COST_INR = float(os.environ.get('INDIA_INTRADAY_MIN_ROUND_TRIP_COST_INR', '40'))
INDIA_INTRADAY_STT_SELL_PCT = float(os.environ.get('INDIA_INTRADAY_STT_SELL_PCT', '0.025'))
INDIA_INTRADAY_EXCHANGE_TXN_PCT = float(os.environ.get('INDIA_INTRADAY_EXCHANGE_TXN_PCT', '0.00307'))
INDIA_INTRADAY_SEBI_PCT = float(os.environ.get('INDIA_INTRADAY_SEBI_PCT', '0.0001'))
INDIA_INTRADAY_STAMP_DUTY_BUY_PCT = float(os.environ.get('INDIA_INTRADAY_STAMP_DUTY_BUY_PCT', '0.003'))
INDIA_INTRADAY_GST_PCT = float(os.environ.get('INDIA_INTRADAY_GST_PCT', '18'))
INTRADAY_COST_SLIPPAGE_BPS = float(os.environ.get('INTRADAY_COST_SLIPPAGE_BPS', str(EXECUTION_SLIPPAGE_BPS)))
INTRADAY_MIN_EXPECTED_NET_PROFIT_INR = float(os.environ.get('INTRADAY_MIN_EXPECTED_NET_PROFIT_INR', '50'))
INTRADAY_MIN_EXPECTED_NET_PROFIT_USD = float(os.environ.get('INTRADAY_MIN_EXPECTED_NET_PROFIT_USD', '1'))
INTRADAY_MIN_GROSS_PROFIT_TO_COST_RATIO = float(os.environ.get('INTRADAY_MIN_GROSS_PROFIT_TO_COST_RATIO', '1.5'))
INTRADAY_MIN_STOP_LOSS_PCT = float(os.environ.get('INTRADAY_MIN_STOP_LOSS_PCT', '0.25'))
INTRADAY_MIN_NET_PROFIT_TO_PROTECT_INR = float(os.environ.get('INTRADAY_MIN_NET_PROFIT_TO_PROTECT_INR', '25'))
INTRADAY_MIN_NET_PROFIT_TO_PROTECT_USD = float(os.environ.get('INTRADAY_MIN_NET_PROFIT_TO_PROTECT_USD', '0.5'))
US_INTRADAY_MIN_ROUND_TRIP_COST_USD = float(os.environ.get('US_INTRADAY_MIN_ROUND_TRIP_COST_USD', '1'))
US_INTRADAY_COMMISSION_PER_ORDER_USD = float(os.environ.get('US_INTRADAY_COMMISSION_PER_ORDER_USD', '0'))
US_INTRADAY_COST_BPS = float(os.environ.get('US_INTRADAY_COST_BPS', str(EXECUTION_FEE_BPS + EXECUTION_SPREAD_BPS)))
PORTFOLIO_CORRELATION_ALERT = float(os.environ.get('PORTFOLIO_CORRELATION_ALERT', '0.72'))
PORTFOLIO_MAX_CORRELATED_POSITIONS = int(os.environ.get('PORTFOLIO_MAX_CORRELATED_POSITIONS', '2'))
VOL_TARGET_ATR_RISK_MULTIPLIER = float(os.environ.get('VOL_TARGET_ATR_RISK_MULTIPLIER', '1.4'))

FIRST_SCAN_THREADS: Dict[str, threading.Thread] = {}
FIRST_SCAN_THREADS_LOCK = threading.Lock()
INTRADAY_RUN_LOCK = threading.Lock()
INTRADAY_AUTO_LOCK = threading.RLock()
INTRADAY_AUTO_STOP_EVENT = threading.Event()
INTRADAY_AUTO_THREAD: Optional[threading.Thread] = None
INTRADAY_AUTO_STATE: Dict[str, Any] = {
    'running': False,
    'started_at': None,
    'last_run_at': None,
    'last_checked_at': None,
    'last_message': None,
    'last_error': None,
    'last_orders': 0,
    'check_count': 0,
    'last_result': None,
    'recent_runs': [],
}

PAPER_AUTO_LOCK = threading.RLock()
PAPER_AUTO_STOP_EVENT = threading.Event()
PAPER_AUTO_THREAD: Optional[threading.Thread] = None
PAPER_AUTO_STATE: Dict[str, Any] = {
    'running': False,
    'started_at': None,
    'last_run_at': None,
    'last_checked_at': None,
    'last_message': None,
    'last_error': None,
    'last_orders': 0,
    'check_count': 0,
    'last_result': None,
    'recent_runs': [],
}

# Market-wide volatility index per market — free via yfinance, no API key.
VIX_TICKERS = {
    'US': '^VIX',
    'IN': '^INDIAVIX',
    'Global': '^VIX',
}

HORIZON_DAYS = {
    'day': 1,
    'week': 7,
    'intraday': 1,
}

EXCHANGE_SCHEDULES = {
    'US': {'tz': 'America/New_York', 'open': datetime.time(9, 30), 'close': datetime.time(16, 0), 'currency': 'USD', 'symbol': '$'},
    'IN': {'tz': 'Asia/Kolkata', 'open': datetime.time(9, 15), 'close': datetime.time(15, 30), 'currency': 'INR', 'symbol': '₹'},
    'Global': {'tz': 'America/New_York', 'open': datetime.time(9, 30), 'close': datetime.time(16, 0), 'currency': 'USD', 'symbol': '$'},
}

# Add more holidays through env, e.g. MARKET_HOLIDAYS_US=2026-07-03,2026-12-25
DEFAULT_MARKET_HOLIDAYS = {
    'US': {'2026-01-01', '2026-01-19', '2026-02-16', '2026-04-03', '2026-05-25', '2026-06-19', '2026-07-03', '2026-09-07', '2026-11-26', '2026-12-25'},
    'IN': set(),
    'Global': {'2026-01-01', '2026-01-19', '2026-02-16', '2026-04-03', '2026-05-25', '2026-06-19', '2026-07-03', '2026-09-07', '2026-11-26', '2026-12-25'},
}


def utc_now_naive() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)


def get_market_holidays(market: str) -> set:
    holidays = set(DEFAULT_MARKET_HOLIDAYS.get(market, set()))
    raw = os.environ.get(f'MARKET_HOLIDAYS_{market.upper()}', '')
    for item in raw.split(','):
        item = item.strip()
        if item:
            holidays.add(item)
    return holidays


def get_market_schedule(market: str) -> Dict[str, Any]:
    return EXCHANGE_SCHEDULES.get(market, EXCHANGE_SCHEDULES['US'])


def get_market_data_source_info(market: Optional[str] = None, purpose: str = 'market_scan') -> Dict[str, Any]:
    configured = (MARKET_DATA_PROVIDER or 'yfinance').lower()
    actual_provider = 'yfinance'
    source_label = 'Yahoo Finance via yfinance'
    source_type = 'free_unofficial_market_data'
    freshness = 'best_effort_may_be_delayed'
    official = False
    note = 'Built-in fetch currently uses yfinance/Yahoo Finance. Good for paper testing and daily/weekly research; use Groww/broker data for live execution.'
    if configured in ('groww', 'groww_api'):
        note = 'Groww market data is configured, but this build has no direct Groww data adapter yet; yfinance fallback is active.' if GROWW_MARKET_DATA_ENABLED else 'MARKET_DATA_PROVIDER=groww is not enabled through GROWW_MARKET_DATA_ENABLED; yfinance fallback is active.'
    elif configured not in ('yfinance', 'yahoo', 'yahoo_finance'):
        note = f'MARKET_DATA_PROVIDER={configured} is not implemented in this build; yfinance fallback is active.'
    return {
        'configured_provider': configured,
        'actual_provider': actual_provider,
        'provider': actual_provider,
        'source_name': source_label,
        'source_label': source_label,
        'source_type': source_type,
        'freshness': freshness,
        'official': official,
        'market': market or '',
        'purpose': purpose,
        'cost': '₹0 market-data API cost',
        'paid': False,
        'note': note,
        'fetched_at': utc_now_naive().isoformat(),
    }


def is_trading_day(market: str, day: datetime.date) -> bool:
    return day.weekday() < 5 and day.isoformat() not in get_market_holidays(market)


def next_trading_day(market: str, day: datetime.date) -> datetime.date:
    current = day
    for _ in range(14):
        if is_trading_day(market, current):
            return current
        current += datetime.timedelta(days=1)
    return current


def add_trading_sessions(market: str, start_day: datetime.date, sessions: int) -> datetime.date:
    current = next_trading_day(market, start_day)
    remaining = max(sessions - 1, 0)
    while remaining > 0:
        current += datetime.timedelta(days=1)
        if is_trading_day(market, current):
            remaining -= 1
    return current


def get_prediction_check_datetime(market: str, horizon: str, created_at_utc: Optional[datetime.datetime] = None) -> datetime.datetime:
    """Return the UTC-naive time when accuracy can be verified.

    Day predictions are checked only after the applicable exchange session closes,
    plus a small settlement delay. Weekly predictions wait for five valid trading
    sessions. This prevents checking at the open or while the horizon is active.
    """
    created_at_utc = created_at_utc or utc_now_naive()
    if created_at_utc.tzinfo is None:
        created_aware = created_at_utc.replace(tzinfo=datetime.timezone.utc)
    else:
        created_aware = created_at_utc.astimezone(datetime.timezone.utc)
    sched = get_market_schedule(market)
    tz = ZoneInfo(sched['tz'])
    local_dt = created_aware.astimezone(tz)
    local_day = local_dt.date()
    local_close_dt = datetime.datetime.combine(local_day, sched['close'], tzinfo=tz)

    if is_trading_day(market, local_day) and local_dt < local_close_dt:
        first_session_day = local_day
    else:
        first_session_day = next_trading_day(market, local_day + datetime.timedelta(days=1))

    sessions = 1 if horizon in ('day', 'intraday') else 5
    check_day = add_trading_sessions(market, first_session_day, sessions)
    check_local = datetime.datetime.combine(check_day, sched['close'], tzinfo=tz) + datetime.timedelta(minutes=VERIFY_AFTER_CLOSE_DELAY_MINUTES)
    return check_local.astimezone(datetime.timezone.utc).replace(tzinfo=None)


def get_currency_info(market: str, ticker: Optional[str] = None) -> Dict[str, str]:
    if market == 'IN' or (ticker and str(ticker).upper().endswith('.NS')):
        return {'currency': 'INR', 'symbol': '₹'}
    return {'currency': 'USD', 'symbol': '$'}


def calculate_claude_cost(input_tokens: int, output_tokens: int) -> Dict[str, float]:
    input_cost = (input_tokens / 1_000_000) * ANTHROPIC_INPUT_USD_PER_MILLION
    output_cost = (output_tokens / 1_000_000) * ANTHROPIC_OUTPUT_USD_PER_MILLION
    total_usd = input_cost + output_cost
    return {
        'input_tokens': int(input_tokens or 0),
        'output_tokens': int(output_tokens or 0),
        'cost_usd': round(total_usd, 6),
        'cost_inr': round(total_usd * USD_INR_RATE, 4),
    }


def get_ai_budget_windows(now: Optional[datetime.datetime] = None) -> Dict[str, datetime.datetime]:
    """Return UTC-naive day/week windows for paid-AI budget accounting."""
    now = now or utc_now_naive()
    if now.tzinfo is None:
        now_aware = now.replace(tzinfo=datetime.timezone.utc)
    else:
        now_aware = now.astimezone(datetime.timezone.utc)
    ist = now_aware.astimezone(ZoneInfo('Asia/Kolkata'))
    day_start_local = datetime.datetime.combine(ist.date(), datetime.time.min, tzinfo=ZoneInfo('Asia/Kolkata'))
    week_start_date = ist.date() - datetime.timedelta(days=ist.weekday())
    week_start_local = datetime.datetime.combine(week_start_date, datetime.time.min, tzinfo=ZoneInfo('Asia/Kolkata'))
    return {
        'day_start': day_start_local.astimezone(datetime.timezone.utc).replace(tzinfo=None),
        'week_start': week_start_local.astimezone(datetime.timezone.utc).replace(tzinfo=None),
        'now': now_aware.replace(tzinfo=None),
    }


def get_ai_budget_status(estimated_next_cost_inr: Optional[float] = None) -> Dict[str, Any]:
    """Check whether another paid Claude call is allowed.

    This is the hard cost-control layer: live trading execution must never depend
    on fresh Claude calls. Paid AI is restricted to locked scan/ranking and
    optional batch learning, and this function blocks it when limits are reached.
    """
    estimated_next_cost_inr = float(estimated_next_cost_inr or 0)
    windows = get_ai_budget_windows()
    day_calls = week_calls = 0
    day_cost = week_cost = 0.0
    conn = get_db_connection()
    if conn is not None:
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT COUNT(*) AS calls, COALESCE(SUM(cost_inr),0) AS cost
                    FROM api_cost_log
                    WHERE created_at >= %s
                """, (windows['day_start'],))
                row = cur.fetchone() or {}
                day_calls = int(row.get('calls') or 0)
                day_cost = float(row.get('cost') or 0)
                cur.execute("""
                    SELECT COUNT(*) AS calls, COALESCE(SUM(cost_inr),0) AS cost
                    FROM api_cost_log
                    WHERE created_at >= %s
                """, (windows['week_start'],))
                row = cur.fetchone() or {}
                week_calls = int(row.get('calls') or 0)
                week_cost = float(row.get('cost') or 0)
        except Exception as e:
            print(f"[WARN] Could not read AI budget status: {e}")
        finally:
            conn.close()

    reasons = []
    if AI_MAX_PAID_CALLS_PER_DAY >= 0 and day_calls >= AI_MAX_PAID_CALLS_PER_DAY:
        reasons.append('daily_ai_call_limit_reached')
    if AI_MAX_PAID_CALLS_PER_WEEK >= 0 and week_calls >= AI_MAX_PAID_CALLS_PER_WEEK:
        reasons.append('weekly_ai_call_limit_reached')
    if AI_MAX_COST_INR_PER_DAY >= 0 and day_cost + estimated_next_cost_inr > AI_MAX_COST_INR_PER_DAY:
        reasons.append('daily_ai_cost_limit_reached')
    if AI_MAX_COST_INR_PER_WEEK >= 0 and week_cost + estimated_next_cost_inr > AI_MAX_COST_INR_PER_WEEK:
        reasons.append('weekly_ai_cost_limit_reached')

    allowed = not reasons or AI_ALLOW_FORCE_OVER_BUDGET
    return {
        'allowed': bool(allowed),
        'blocked': not bool(allowed),
        'reasons': reasons,
        'force_over_budget_enabled': AI_ALLOW_FORCE_OVER_BUDGET,
        'today': {
            'calls': day_calls, 'cost_inr': round(day_cost, 2),
            'max_calls': AI_MAX_PAID_CALLS_PER_DAY, 'max_cost_inr': AI_MAX_COST_INR_PER_DAY,
            'remaining_calls': None if AI_MAX_PAID_CALLS_PER_DAY < 0 else max(AI_MAX_PAID_CALLS_PER_DAY - day_calls, 0),
            'remaining_cost_inr': None if AI_MAX_COST_INR_PER_DAY < 0 else round(max(AI_MAX_COST_INR_PER_DAY - day_cost, 0), 2),
        },
        'week': {
            'calls': week_calls, 'cost_inr': round(week_cost, 2),
            'max_calls': AI_MAX_PAID_CALLS_PER_WEEK, 'max_cost_inr': AI_MAX_COST_INR_PER_WEEK,
            'remaining_calls': None if AI_MAX_PAID_CALLS_PER_WEEK < 0 else max(AI_MAX_PAID_CALLS_PER_WEEK - week_calls, 0),
            'remaining_cost_inr': None if AI_MAX_COST_INR_PER_WEEK < 0 else round(max(AI_MAX_COST_INR_PER_WEEK - week_cost, 0), 2),
        },
        'estimated_next_cost_inr': round(estimated_next_cost_inr, 2),
        'policy': 'Paid AI is allowed only for locked scan/ranking and optional batch learning. Buy/sell/stop-loss/target execution is deterministic and does not call Claude.',
        'windows_utc': {
            'day_start': windows['day_start'].isoformat(),
            'week_start': windows['week_start'].isoformat(),
        }
    }


def get_execution_policy() -> Dict[str, Any]:
    return {
        'strategy_owner': 'App deterministic rule engine, with Claude used only as a bounded analyst/ranker.',
        'claude_used_for': ['locked first/current-period scan ranking', 'optional batch learning only if enabled'],
        'claude_never_used_for': ['live buy execution', 'live sell execution', 'stop-loss checks', 'target checks', 'weekly goal guard', 'cached scan display'],
        'buy_sell_engine': 'Rule based: saved locked prediction + confidence/RR filters + 15-minute opening confirmation + position sizing + weekly goal/max-loss/cost guard.',
        'cost_guard': 'Daily and weekly paid-AI call/cost budgets are enforced before any Claude ranking call.',
        'capital_guard': 'Weekly target is a profit milestone, not a ceiling by default; new entries continue only if strict edge/risk checks pass. Weekly max-loss still blocks new entries. Entry is also blocked until opening confirmation passes.',
        'opening_confirmation': 'No new BUY orders during the first configured opening-wait window. After that, price must stay near entry and above the risk zone before an order is created.',
        'profit_protection': 'Open positions are exited not only at target/stop. A post-entry risk recheck can hold, tighten stop, or exit when multiple weakening/risk signals appear; winners are not sold just because the weekly target is reached.',
        'horizon_policy': 'Trade horizon is a maximum validity/evaluation window only. The system can exit earlier for target, stop-loss, trailing profit-protection, confirmed momentum/risk failure, intraday forced-exit, or final horizon expiry.',
    }


def get_prediction_lock_key(horizon: str, moment: Optional[datetime.datetime] = None) -> str:
    """Return the lock bucket that should keep predictions stable.

    Daily scans lock to the local calendar date. Weekly scans lock to the ISO
    week, so a second weekly scan in the same week does not rewrite the picks.
    """
    moment = moment or datetime.datetime.now()
    if horizon == 'week':
        iso = moment.date().isocalendar()
        return f"{iso.year}-W{iso.week:02d}"
    return moment.date().isoformat()


def payload_matches_current_lock(payload: Optional[Dict[str, Any]], horizon: str) -> bool:
    if not payload:
        return False
    current_key = get_prediction_lock_key(horizon)
    if payload.get('prediction_lock_key') == current_key:
        return True

    # Backward compatibility for older cached payloads that do not yet contain
    # prediction_lock_key. Treat a completed scan from today/current week as
    # locked so older deployments do not accidentally overwrite it.
    raw_ts = payload.get('completed_at') or payload.get('timestamp')
    if not raw_ts:
        return False
    try:
        parsed = datetime.datetime.fromisoformat(str(raw_ts).replace('Z', '+00:00'))
    except Exception:
        return False
    return get_prediction_lock_key(horizon, parsed) == current_key


LIQUIDITY_RULES = {
    'US': {'min_avg_volume_20': 400_000, 'min_avg_dollar_volume_20': 20_000_000},
    'IN': {'min_avg_volume_20': 250_000, 'min_avg_dollar_volume_20': 100_000_000},
    'Global': {'min_avg_volume_20': 300_000, 'min_avg_dollar_volume_20': 15_000_000},
}


def quality_thresholds_for_market(market: str) -> Dict[str, float]:
    rules = LIQUIDITY_RULES.get(market, LIQUIDITY_RULES.get('US', {}))
    return {
        'min_avg_volume': float(rules.get('min_avg_volume_20', 0)) * QUALITY_MIN_AVG_VOLUME_MULTIPLIER,
        'min_avg_value': float(rules.get('min_avg_dollar_volume_20', 0)) * QUALITY_MIN_TURNOVER_MULTIPLIER,
        'min_price': QUALITY_MIN_PRICE,
        'max_zero_volume_days': QUALITY_MAX_ZERO_VOLUME_DAYS,
        'min_recent_bars': QUALITY_MIN_RECENT_BARS,
    }


def analyze_historical_stock_quality(ticker: str, hist, market: str) -> Dict[str, Any]:
    """Decide whether a symbol is active/liquid enough to be used in backtests.

    This is a data-quality and liquidity gate, not a trade signal. It prevents
    stale symbols, inactive shares, low-turnover stocks, and noisy/illiquid data
    from becoming training examples.
    """
    thresholds = quality_thresholds_for_market(market)
    result = {
        'ticker': ticker,
        'ok': True,
        'reason': 'Quality stock: active, liquid, and enough usable history.',
        'checks': [],
        'metrics': {},
        'thresholds': thresholds,
    }
    if not QUALITY_STOCK_FILTER_ENABLED:
        result['reason'] = 'Quality filter disabled by QUALITY_STOCK_FILTER_ENABLED=false.'
        return result
    try:
        frame = normalize_history_frame(hist)
        close = history_series(frame, 'Close')
        volume = history_series(frame, 'Volume')
        if close is None or close.empty:
            return {**result, 'ok': False, 'reason': 'No usable close-price history.'}
        if len(close) < max(BACKTEST_MIN_HISTORY_DAYS, QUALITY_MIN_RECENT_BARS):
            return {**result, 'ok': False, 'reason': f'Not enough clean daily history ({len(close)} bars).'}
        if volume is None or volume.empty:
            return {**result, 'ok': False, 'reason': 'No usable volume history, so liquidity cannot be trusted.'}
        n = min(len(close), len(volume))
        close = close.tail(n)
        volume = volume.tail(n)
        recent_n = min(len(close), max(20, QUALITY_MIN_RECENT_BARS))
        recent_close = close.tail(recent_n)
        recent_volume = volume.tail(recent_n)
        last_price = float(recent_close.iloc[-1]) if len(recent_close) else 0.0
        avg_volume = float(recent_volume.mean()) if len(recent_volume) else 0.0
        avg_value = float((recent_close.tail(len(recent_volume)).values * recent_volume.values).mean()) if len(recent_volume) else 0.0
        zero_volume_days = int((recent_volume <= 0).sum())
        positive_days = int((recent_volume > 0).sum())
        metrics = {
            'recent_bars': recent_n,
            'last_price': round(last_price, 2),
            'avg_volume': round(avg_volume, 0),
            'avg_turnover_value': round(avg_value, 2),
            'zero_volume_days': zero_volume_days,
            'positive_volume_days': positive_days,
        }
        checks = []
        def add_check(name, passed, detail):
            checks.append({'name': name, 'passed': bool(passed), 'detail': detail})
        add_check('Active recent trading', recent_n >= thresholds['min_recent_bars'] and positive_days >= max(15, recent_n - thresholds['max_zero_volume_days']), f'{positive_days}/{recent_n} recent days had volume')
        add_check('Minimum price', last_price >= thresholds['min_price'], f'last price {round(last_price, 2)}; needs {thresholds["min_price"]}+')
        add_check('Minimum average volume', avg_volume >= thresholds['min_avg_volume'], f'avg volume {round(avg_volume):,}; needs {round(thresholds["min_avg_volume"]):,}+')
        add_check('Minimum turnover/value traded', avg_value >= thresholds['min_avg_value'], f'avg value {round(avg_value):,}; needs {round(thresholds["min_avg_value"]):,}+')
        add_check('Low zero-volume days', zero_volume_days <= thresholds['max_zero_volume_days'], f'{zero_volume_days} zero-volume days; max {thresholds["max_zero_volume_days"]}')
        failed = [c for c in checks if not c['passed']]
        result['metrics'] = metrics
        result['checks'] = checks
        if failed:
            result['ok'] = False
            result['reason'] = '; '.join(c['detail'] for c in failed[:2])
        return result
    except BaseException as e:
        return {**result, 'ok': False, 'reason': f'Quality check failed: {str(e)[:160]}'}



def get_backtest_price_mode_info() -> Dict[str, Any]:
    mode = BACKTEST_PRICE_MODE if BACKTEST_PRICE_MODE in ('raw', 'adjusted') else 'raw'
    return {
        'mode': mode,
        'label': 'Raw OHLC candles' if mode == 'raw' else 'Adjusted OHLC candles',
        'yfinance_auto_adjust': False if mode == 'raw' else True,
        'plain_summary': (
            'Raw mode keeps Open/High/Low/Close closer to exchange-style historical candles. '
            'Adjusted mode rewrites older OHLC for splits/dividends/corporate actions, so values may differ from Google/NSE but remain internally consistent.'
        ),
        'display_note': 'Backtest uses real historical candles; trade decisions are simulated. Price mode is recorded for audit.'
    }


def data_integrity_thresholds() -> Dict[str, Any]:
    return {
        'enabled': BACKTEST_DATA_INTEGRITY_ENABLED,
        'price_mode': BACKTEST_PRICE_MODE,
        'max_suspicious_candles': BACKTEST_MAX_SUSPICIOUS_CANDLES,
        'max_daily_range_pct': BACKTEST_MAX_DAILY_RANGE_PCT,
        'max_close_gap_pct': BACKTEST_MAX_CLOSE_GAP_PCT,
        'min_data_quality_score': BACKTEST_MIN_DATA_QUALITY_SCORE,
        'block_learning_on_warning': BACKTEST_BLOCK_LEARNING_ON_DATA_WARNING,
    }


def detect_split_dates(hist) -> set:
    """Return the set of dates where yfinance recorded a real stock split/bonus action.

    Used so that raw-price mode (no auto-adjust) does not mistake a legitimate
    corporate action -- which can look like a 50%+ overnight price change for
    something like a 2:1 split or bonus issue, common on NSE/BSE -- for bad data
    or a real market crash.
    """
    try:
        if hist is None or getattr(hist, 'empty', True):
            return set()
        col = None
        for candidate in ('Stock Splits', 'Stock_Splits', 'stock splits'):
            if candidate in hist.columns:
                col = candidate
                break
        if col is None:
            return set()
        series = hist[col]
        return {idx.date() if hasattr(idx, 'date') else idx for idx, v in series.items() if v and float(v) != 0.0}
    except Exception:
        return set()


def analyze_backtest_data_integrity(ticker: str, hist, market: str, split_dates: Optional[set] = None) -> Dict[str, Any]:
    """Score whether a symbol's historical candles are trustworthy enough for learning.

    This is separate from liquidity. A stock can be liquid but still have suspicious
    historical candles due to provider gaps, corporate-action mismatches, zero prices,
    missing OHLC, or extreme one-day ranges. Bad integrity blocks learning/backtesting
    so wrong data does not teach the strategy wrong patterns.
    """
    result = {
        'ticker': ticker,
        'ok': True,
        'score': 100.0,
        'status': 'good',
        'price_mode': BACKTEST_PRICE_MODE,
        'source': get_market_data_source_info(market, 'historical_backtest'),
        'warnings': [],
        'metrics': {},
        'thresholds': data_integrity_thresholds(),
    }
    if not BACKTEST_DATA_INTEGRITY_ENABLED:
        result['status'] = 'not_used'
        result['warnings'].append('Data integrity guard disabled by BACKTEST_DATA_INTEGRITY_ENABLED=false.')
        return result
    try:
        frame = normalize_history_frame(hist)
        if frame is None or getattr(frame, 'empty', True):
            return {**result, 'ok': False, 'score': 0.0, 'status': 'bad', 'warnings': ['No historical frame available.']}
        close = history_series(frame, 'Close')
        open_s = history_series(frame, 'Open', fallback_col='Close')
        high = history_series(frame, 'High', fallback_col='Close')
        low = history_series(frame, 'Low', fallback_col='Close')
        volume = history_series(frame, 'Volume')
        if close is None or open_s is None or high is None or low is None:
            return {**result, 'ok': False, 'score': 0.0, 'status': 'bad', 'warnings': ['Missing usable OHLC columns.']}
        n = min(len(close), len(open_s), len(high), len(low))
        if n < max(60, BACKTEST_MIN_HISTORY_DAYS):
            return {**result, 'ok': False, 'score': 20.0, 'status': 'bad', 'warnings': [f'Only {n} usable OHLC bars; not enough for clean learning.']}
        close = close.tail(n)
        open_s = open_s.tail(n)
        high = high.tail(n)
        low = low.tail(n)
        bad_price_days = int(((open_s <= 0) | (high <= 0) | (low <= 0) | (close <= 0)).sum())
        range_pct = ((high - low).abs() / close.replace(0, np.nan).abs() * 100).replace([np.inf, -np.inf], np.nan).dropna()
        suspicious_range_days = int((range_pct > BACKTEST_MAX_DAILY_RANGE_PCT).sum()) if not range_pct.empty else 0
        gaps = close.pct_change().abs().dropna() * 100
        split_dates = split_dates or set()
        if split_dates and not gaps.empty:
            # A big overnight % change caused by a real split/bonus is expected in raw-price
            # mode -- it is not bad data, so it should not count against the integrity score.
            gap_dates = {idx.date() if hasattr(idx, 'date') else idx for idx in gaps.index}
            corporate_action_gap_dates = gap_dates & split_dates
            gaps = gaps[[
                (idx.date() if hasattr(idx, 'date') else idx) not in split_dates for idx in gaps.index
            ]]
        else:
            corporate_action_gap_dates = set()
        suspicious_gap_days = int((gaps > BACKTEST_MAX_CLOSE_GAP_PCT).sum()) if not gaps.empty else 0
        missing_volume_days = 0
        zero_volume_days = 0
        if volume is None or volume.empty:
            missing_volume_days = n
        else:
            v = volume.tail(min(len(volume), n))
            missing_volume_days = max(0, n - len(v))
            zero_volume_days = int((v <= 0).sum())
        warnings = []
        penalty = 0.0
        if bad_price_days:
            warnings.append(f'{bad_price_days} candle(s) have zero/negative OHLC values.')
            penalty += min(50, bad_price_days * 12)
        if suspicious_range_days > BACKTEST_MAX_SUSPICIOUS_CANDLES:
            warnings.append(f'{suspicious_range_days} candle(s) have unusually large daily range > {BACKTEST_MAX_DAILY_RANGE_PCT}%.')
            penalty += min(35, suspicious_range_days * 5)
        if suspicious_gap_days > BACKTEST_MAX_SUSPICIOUS_CANDLES:
            warnings.append(f'{suspicious_gap_days} close-to-close gap(s) exceed {BACKTEST_MAX_CLOSE_GAP_PCT}%. Possible corporate-action/data mismatch.')
            penalty += min(35, suspicious_gap_days * 5)
        if missing_volume_days > 0:
            warnings.append('Volume history missing or incomplete.')
            penalty += 20
        if zero_volume_days > QUALITY_MAX_ZERO_VOLUME_DAYS:
            warnings.append(f'{zero_volume_days} zero-volume day(s), above allowed {QUALITY_MAX_ZERO_VOLUME_DAYS}.')
            penalty += min(25, zero_volume_days * 2)
        score = max(0.0, round(100.0 - penalty, 2))
        ok = score >= BACKTEST_MIN_DATA_QUALITY_SCORE and not bad_price_days
        status = 'good' if ok and not warnings else ('warning' if ok else 'bad')
        if corporate_action_gap_dates:
            warnings.append(
                f'{len(corporate_action_gap_dates)} large price change(s) matched a confirmed stock split/bonus action '
                '(via yfinance corporate-action data) and were excluded from the suspicious-gap penalty.'
            )
        result.update({
            'ok': bool(ok),
            'score': score,
            'status': status,
            'warnings': warnings,
            'metrics': {
                'usable_bars': n,
                'bad_price_days': bad_price_days,
                'suspicious_range_days': suspicious_range_days,
                'suspicious_gap_days': suspicious_gap_days,
                'zero_volume_days': zero_volume_days,
                'missing_volume_days': missing_volume_days,
                'max_daily_range_pct_seen': round(float(range_pct.max()), 2) if not range_pct.empty else None,
                'max_close_gap_pct_seen': round(float(gaps.max()), 2) if not gaps.empty else None,
                'confirmed_split_dates': sorted([str(d) for d in (split_dates or set())]),
                'corporate_action_gaps_excluded': len(corporate_action_gap_dates),
            },
        })
        if not warnings:
            result['warnings'] = ['Historical candle data passed integrity guard.']
        return result
    except BaseException as e:
        return {**result, 'ok': False, 'score': 0.0, 'status': 'bad', 'warnings': [f'Data integrity check failed: {str(e)[:160]}']}

# Defaults are a liquid, active universe that works out-of-the-box on a small
# Render service. For production, place a fuller exchange universe in
# data/universe_US.txt, data/universe_IN.txt, data/universe_Global.txt or set
# MARKET_UNIVERSE_US / MARKET_UNIVERSE_IN / MARKET_UNIVERSE_GLOBAL as CSV.
DEFAULT_UNIVERSES = {
    'US': [
        'AAPL','MSFT','NVDA','GOOGL','GOOG','AMZN','META','TSLA','AMD','AVGO','CRM','ADBE','NFLX','ORCL','CSCO','INTC','QCOM','TXN','AMAT','MU','NOW','SHOP','SNOW','PLTR','UBER','ABNB','PANW','CRWD','DDOG','NET',
        'JPM','BAC','WFC','C','GS','MS','AXP','V','MA','PYPL','COIN','SOFI','HOOD','BLK','SCHW','USB','PNC','TFC','BK','AIG',
        'UNH','JNJ','LLY','PFE','MRK','ABBV','TMO','ABT','DHR','BMY','GILD','AMGN','CVS','CI','HCA','ISRG','VRTX','REGN','MDT','BSX',
        'XOM','CVX','COP','SLB','EOG','OXY','MPC','PSX','VLO','HAL','BKR','KMI','WMB','LNG','DVN',
        'WMT','COST','HD','LOW','TGT','NKE','MCD','SBUX','CMG','DIS','BKNG','MAR','RCL','CCL','GM','F','RIVN','LCID','NIO','LI','XPEV',
        'BA','CAT','DE','GE','HON','RTX','LMT','NOC','UPS','FDX','UNP','CSX','DAL','UAL','AAL','LUV','ETN','EMR','MMM',
        'PG','KO','PEP','PM','MO','MDLZ','CL','KMB','GIS','KHC','STZ','EL','KR',
        'LIN','APD','SHW','FCX','NEM','ALB','MOS','CF','NUE','STLD','AA','X'
    ],
    'IN': [
        'RELIANCE.NS','TCS.NS','INFY.NS','HDFCBANK.NS','ICICIBANK.NS','SBIN.NS','AXISBANK.NS','KOTAKBANK.NS','BAJFINANCE.NS','BAJAJFINSV.NS','HINDUNILVR.NS','ITC.NS','LT.NS','BHARTIARTL.NS','ASIANPAINT.NS',
        'MARUTI.NS','TMCV.NS','M&M.NS','EICHERMOT.NS','BAJAJ-AUTO.NS','HEROMOTOCO.NS','TVSMOTOR.NS','ASHOKLEY.NS',
        'SUNPHARMA.NS','CIPLA.NS','DRREDDY.NS','DIVISLAB.NS','APOLLOHOSP.NS','LUPIN.NS','AUROPHARMA.NS',
        'HCLTECH.NS','WIPRO.NS','TECHM.NS','LTIM.NS','PERSISTENT.NS','COFORGE.NS','MPHASIS.NS','OFSS.NS',
        'TITAN.NS','TRENT.NS','DMART.NS','NYKAA.NS','ZOMATO.NS','PAYTM.NS','POLYCAB.NS','DIXON.NS','VOLTAS.NS','HAVELLS.NS',
        'ADANIENT.NS','ADANIPORTS.NS','ADANIGREEN.NS','ADANIPOWER.NS','ADANITRANS.NS','AMBUJACEM.NS','ACC.NS','ULTRACEMCO.NS','GRASIM.NS','SHREECEM.NS',
        'ONGC.NS','NTPC.NS','POWERGRID.NS','COALINDIA.NS','TATAPOWER.NS','IOC.NS','BPCL.NS','GAIL.NS','JSWENERGY.NS','NHPC.NS',
        'JSWSTEEL.NS','TATASTEEL.NS','HINDALCO.NS','VEDL.NS','JINDALSTEL.NS','SAIL.NS','NMDC.NS','HINDZINC.NS',
        'IRFC.NS','RVNL.NS','IRCTC.NS','BHEL.NS','BEL.NS','HAL.NS','BDL.NS','MAZDOCK.NS','COCHINSHIP.NS',
        'HDFCLIFE.NS','SBILIFE.NS','ICICIPRULI.NS','ICICIGI.NS','SBICARD.NS','CHOLAFIN.NS','MUTHOOTFIN.NS','BAJAJHLDNG.NS'
    ],
    'Global': [
        'AAPL','MSFT','NVDA','GOOGL','AMZN','META','TSLA','AMD','AVGO','JPM','V','MA','UNH','XOM','LLY','WMT','COST','NFLX','ORCL','CRM',
        'TSM','ASML','SAP','NVO','BABA','JD','PDD','BIDU','NIO','LI','XPEV','TCEHY','SONY','TM','HMC','MUFG','SMFG',
        'HSBC','BP','SHEL','RIO','BHP','UL','AZN','GSK','NVS','RACE','MC.PA','OR.PA','AIR.PA','SU.PA','SAN.PA',
        'RELIANCE.NS','TCS.NS','INFY.NS','HDFCBANK.NS','ICICIBANK.NS','SBIN.NS','BHARTIARTL.NS','TMCV.NS','ZOMATO.NS','IRFC.NS',
        'MELI','NU','VALE','PBR','ERJ','SHOP','RY','TD','BNS','BMO','SE','GRAB','CPNG','BILI','NTES'
    ],
}


def _json_safe(obj: Any) -> Any:
    if isinstance(obj, np.generic):
        return obj.item()
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def get_db_connection():
    if not DATABASE_URL:
        return None
    return psycopg2.connect(DATABASE_URL)


def init_db():
    conn = get_db_connection()
    if conn is None:
        print("[WARN] DATABASE_URL not set - cache and prediction tracking disabled.")
        return
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS predictions (
                        id SERIAL PRIMARY KEY,
                        scan_timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
                        prediction_date DATE NOT NULL DEFAULT CURRENT_DATE,
                        market VARCHAR(20) NOT NULL,
                        horizon VARCHAR(10) NOT NULL,
                        ticker VARCHAR(20) NOT NULL,
                        company VARCHAR(200),
                        rank INT,
                        signal VARCHAR(10),
                        entry_price NUMERIC,
                        predicted_gain_pct NUMERIC,
                        target_price NUMERIC,
                        stop_price NUMERIC,
                        confidence INT,
                        reasoning TEXT,
                        check_date TIMESTAMP NOT NULL,
                        checked BOOLEAN NOT NULL DEFAULT FALSE,
                        actual_price NUMERIC,
                        actual_high NUMERIC,
                        actual_low NUMERIC,
                        actual_gain_pct NUMERIC,
                        outcome_correct BOOLEAN,
                        outcome_label VARCHAR(80),
                        outcome_status VARCHAR(40),
                        failure_reason TEXT,
                        lesson_summary TEXT,
                        features_json JSONB,
                        checked_at TIMESTAMP
                    );
                """)
                cur.execute("ALTER TABLE predictions ADD COLUMN IF NOT EXISTS prediction_date DATE NOT NULL DEFAULT CURRENT_DATE;")
                cur.execute("ALTER TABLE predictions ADD COLUMN IF NOT EXISTS actual_high NUMERIC;")
                cur.execute("ALTER TABLE predictions ADD COLUMN IF NOT EXISTS actual_low NUMERIC;")
                cur.execute("ALTER TABLE predictions ADD COLUMN IF NOT EXISTS outcome_label VARCHAR(80);")
                cur.execute("ALTER TABLE predictions ADD COLUMN IF NOT EXISTS outcome_status VARCHAR(40);")
                cur.execute("ALTER TABLE predictions ADD COLUMN IF NOT EXISTS failure_reason TEXT;")
                cur.execute("ALTER TABLE predictions ADD COLUMN IF NOT EXISTS lesson_summary TEXT;")
                cur.execute("ALTER TABLE predictions ADD COLUMN IF NOT EXISTS features_json JSONB;")
                cur.execute("UPDATE predictions SET signal='WATCH' WHERE signal='HOLD';")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_pred_pending ON predictions (checked, check_date);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_pred_ticker ON predictions (ticker);")
                cur.execute("""
                    DELETE FROM predictions p
                    USING predictions older
                    WHERE p.id > older.id
                      AND p.market = older.market
                      AND p.horizon = older.horizon
                      AND p.ticker = older.ticker
                      AND p.prediction_date = older.prediction_date;
                """)
                cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_pred_one_per_day ON predictions (market, horizon, ticker, prediction_date);")

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS stock_scan_cache (
                        id SERIAL PRIMARY KEY,
                        market VARCHAR(20) NOT NULL,
                        horizon VARCHAR(10) NOT NULL,
                        status VARCHAR(20) NOT NULL DEFAULT 'complete',
                        started_at TIMESTAMP NOT NULL DEFAULT NOW(),
                        completed_at TIMESTAMP,
                        expires_at TIMESTAMP,
                        total_universe INT NOT NULL DEFAULT 0,
                        total_liquid INT NOT NULL DEFAULT 0,
                        total_momentum INT NOT NULL DEFAULT 0,
                        total_technical INT NOT NULL DEFAULT 0,
                        total_deep_candidates INT NOT NULL DEFAULT 0,
                        model VARCHAR(100),
                        prediction_lock_key VARCHAR(32),
                        locked BOOLEAN NOT NULL DEFAULT TRUE,
                        results_json JSONB,
                        methodology_json JSONB,
                        error TEXT,
                        updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                        UNIQUE (market, horizon)
                    );
                """)
                cur.execute("ALTER TABLE stock_scan_cache ADD COLUMN IF NOT EXISTS prediction_lock_key VARCHAR(32);")
                cur.execute("ALTER TABLE stock_scan_cache ADD COLUMN IF NOT EXISTS locked BOOLEAN NOT NULL DEFAULT TRUE;")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_stock_scan_cache_lookup ON stock_scan_cache (market, horizon, updated_at DESC);")

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS stock_scan_runs (
                        id SERIAL PRIMARY KEY,
                        market VARCHAR(20) NOT NULL,
                        horizon VARCHAR(10) NOT NULL,
                        started_at TIMESTAMP NOT NULL DEFAULT NOW(),
                        completed_at TIMESTAMP,
                        status VARCHAR(20) NOT NULL,
                        total_universe INT NOT NULL DEFAULT 0,
                        total_liquid INT NOT NULL DEFAULT 0,
                        total_momentum INT NOT NULL DEFAULT 0,
                        total_technical INT NOT NULL DEFAULT 0,
                        total_deep_candidates INT NOT NULL DEFAULT 0,
                        error TEXT
                    );
                """)

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS prediction_lessons (
                        id SERIAL PRIMARY KEY,
                        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                        prediction_id INT,
                        market VARCHAR(20),
                        horizon VARCHAR(10),
                        ticker VARCHAR(20),
                        signal VARCHAR(20),
                        outcome_label VARCHAR(80),
                        outcome_status VARCHAR(40),
                        failure_reason TEXT,
                        lesson_summary TEXT,
                        actual_gain_pct NUMERIC,
                        features_json JSONB
                    );
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_prediction_lessons_lookup ON prediction_lessons (market, horizon, created_at DESC);")
                cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_prediction_lessons_once ON prediction_lessons (prediction_id) WHERE prediction_id IS NOT NULL;")

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS api_cost_log (
                        id SERIAL PRIMARY KEY,
                        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                        call_type VARCHAR(50) NOT NULL,
                        market VARCHAR(20),
                        horizon VARCHAR(10),
                        model VARCHAR(100),
                        input_tokens INT NOT NULL DEFAULT 0,
                        output_tokens INT NOT NULL DEFAULT 0,
                        cost_usd NUMERIC NOT NULL DEFAULT 0,
                        cost_inr NUMERIC NOT NULL DEFAULT 0,
                        notes TEXT,
                        metadata_json JSONB
                    );
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_api_cost_log_created ON api_cost_log (created_at DESC);")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS trading_settings (
                        id INT PRIMARY KEY DEFAULT 1,
                        enabled BOOLEAN NOT NULL DEFAULT FALSE,
                        mode VARCHAR(20) NOT NULL DEFAULT 'paper',
                        require_confirmation BOOLEAN NOT NULL DEFAULT TRUE,
                        broker VARCHAR(80) NOT NULL DEFAULT 'manual-webhook',
                        capital_inr NUMERIC NOT NULL DEFAULT 5000,
                        max_position_pct NUMERIC NOT NULL DEFAULT 25,
                        risk_per_trade_pct NUMERIC NOT NULL DEFAULT 2,
                        daily_loss_limit_pct NUMERIC NOT NULL DEFAULT 5,
                        max_open_positions INT NOT NULL DEFAULT 3,
                        min_confidence INT NOT NULL DEFAULT 65,
                        min_risk_reward NUMERIC NOT NULL DEFAULT 1.5,
                        allow_us_with_inr_capital BOOLEAN NOT NULL DEFAULT FALSE,
                        weekly_profit_target_pct NUMERIC NOT NULL DEFAULT 10,
                        max_weekly_loss_pct NUMERIC NOT NULL DEFAULT 4,
                        cover_api_costs_from_profit BOOLEAN NOT NULL DEFAULT TRUE,
                        stop_after_weekly_target BOOLEAN NOT NULL DEFAULT FALSE,
                        stop_after_weekly_loss BOOLEAN NOT NULL DEFAULT TRUE,
                        broker_api_monthly_cost_inr NUMERIC NOT NULL DEFAULT 0,
                        opening_confirmation_enabled BOOLEAN NOT NULL DEFAULT TRUE,
                        opening_wait_minutes INT NOT NULL DEFAULT 15,
                        opening_max_entry_chase_pct NUMERIC NOT NULL DEFAULT 1.25,
                        opening_max_entry_pullback_pct NUMERIC NOT NULL DEFAULT 1.0,
                        opening_min_volume_multiplier NUMERIC NOT NULL DEFAULT 0,
                        profit_protection_enabled BOOLEAN NOT NULL DEFAULT TRUE,
                        profit_protect_progress_pct NUMERIC NOT NULL DEFAULT 50,
                        trailing_stop_activation_pct NUMERIC NOT NULL DEFAULT 40,
                        trailing_stop_giveback_pct NUMERIC NOT NULL DEFAULT 35,
                        exit_at_horizon_end BOOLEAN NOT NULL DEFAULT TRUE,
                        selection_mode VARCHAR(30) NOT NULL DEFAULT 'locked_scan',
                        self_scan_universe_limit INT NOT NULL DEFAULT 150,
                        self_scan_min_score NUMERIC NOT NULL DEFAULT 55,
                        updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                    );
                """)
                cur.execute("ALTER TABLE trading_settings ADD COLUMN IF NOT EXISTS capital_usd NUMERIC NOT NULL DEFAULT 1000;")
                cur.execute("ALTER TABLE trading_settings ADD COLUMN IF NOT EXISTS weekly_profit_target_pct NUMERIC NOT NULL DEFAULT 10;")
                cur.execute("ALTER TABLE trading_settings ADD COLUMN IF NOT EXISTS max_weekly_loss_pct NUMERIC NOT NULL DEFAULT 4;")
                cur.execute("ALTER TABLE trading_settings ADD COLUMN IF NOT EXISTS cover_api_costs_from_profit BOOLEAN NOT NULL DEFAULT TRUE;")
                cur.execute("ALTER TABLE trading_settings ADD COLUMN IF NOT EXISTS stop_after_weekly_target BOOLEAN NOT NULL DEFAULT FALSE;")
                cur.execute("ALTER TABLE trading_settings ADD COLUMN IF NOT EXISTS stop_after_weekly_loss BOOLEAN NOT NULL DEFAULT TRUE;")
                cur.execute("ALTER TABLE trading_settings ADD COLUMN IF NOT EXISTS broker_api_monthly_cost_inr NUMERIC NOT NULL DEFAULT 0;")
                cur.execute("ALTER TABLE trading_settings ADD COLUMN IF NOT EXISTS opening_confirmation_enabled BOOLEAN NOT NULL DEFAULT TRUE;")
                cur.execute("ALTER TABLE trading_settings ADD COLUMN IF NOT EXISTS opening_wait_minutes INT NOT NULL DEFAULT 15;")
                cur.execute("ALTER TABLE trading_settings ADD COLUMN IF NOT EXISTS opening_max_entry_chase_pct NUMERIC NOT NULL DEFAULT 1.25;")
                cur.execute("ALTER TABLE trading_settings ADD COLUMN IF NOT EXISTS opening_max_entry_pullback_pct NUMERIC NOT NULL DEFAULT 1.0;")
                cur.execute("ALTER TABLE trading_settings ADD COLUMN IF NOT EXISTS opening_min_volume_multiplier NUMERIC NOT NULL DEFAULT 0;")
                cur.execute("ALTER TABLE trading_settings ADD COLUMN IF NOT EXISTS profit_protection_enabled BOOLEAN NOT NULL DEFAULT TRUE;")
                cur.execute("ALTER TABLE trading_settings ADD COLUMN IF NOT EXISTS profit_protect_progress_pct NUMERIC NOT NULL DEFAULT 50;")
                cur.execute("ALTER TABLE trading_settings ADD COLUMN IF NOT EXISTS trailing_stop_activation_pct NUMERIC NOT NULL DEFAULT 40;")
                cur.execute("ALTER TABLE trading_settings ADD COLUMN IF NOT EXISTS trailing_stop_giveback_pct NUMERIC NOT NULL DEFAULT 35;")
                cur.execute("ALTER TABLE trading_settings ADD COLUMN IF NOT EXISTS exit_at_horizon_end BOOLEAN NOT NULL DEFAULT TRUE;")
                cur.execute("ALTER TABLE trading_settings ADD COLUMN IF NOT EXISTS selection_mode VARCHAR(30) NOT NULL DEFAULT 'locked_scan';")
                cur.execute("ALTER TABLE trading_settings ADD COLUMN IF NOT EXISTS self_scan_universe_limit INT NOT NULL DEFAULT 150;")
                cur.execute("ALTER TABLE trading_settings ADD COLUMN IF NOT EXISTS self_scan_min_score NUMERIC NOT NULL DEFAULT 55;")
                if TRADING_SELECTION_MODE_DEFAULT in ('locked_scan', 'self_scan', 'auto'):
                    cur.execute("UPDATE trading_settings SET selection_mode=%s WHERE selection_mode IS NULL OR selection_mode='locked_scan';", (TRADING_SELECTION_MODE_DEFAULT,))
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS intraday_settings (
                        id INT PRIMARY KEY DEFAULT 1,
                        enabled BOOLEAN NOT NULL DEFAULT FALSE,
                        market VARCHAR(20) NOT NULL DEFAULT 'IN',
                        execution_mode VARCHAR(20) NOT NULL DEFAULT 'paper',
                        require_confirmation BOOLEAN NOT NULL DEFAULT TRUE,
                        auto_enabled BOOLEAN NOT NULL DEFAULT FALSE,
                        auto_interval_seconds INT NOT NULL DEFAULT 300,
                        opening_wait_minutes INT NOT NULL DEFAULT 15,
                        universe_limit INT NOT NULL DEFAULT 60,
                        max_candidates INT NOT NULL DEFAULT 8,
                        min_score INT NOT NULL DEFAULT 70,
                        price_filter_enabled BOOLEAN NOT NULL DEFAULT TRUE,
                        min_stock_price NUMERIC NOT NULL DEFAULT 50,
                        max_stock_price NUMERIC NOT NULL DEFAULT 1500,
                        min_order_quantity INT NOT NULL DEFAULT 5,
                        min_position_value NUMERIC NOT NULL DEFAULT 10000,
                        min_price_change_pct NUMERIC NOT NULL DEFAULT 0.35,
                        max_chase_pct NUMERIC NOT NULL DEFAULT 3.0,
                        require_vwap BOOLEAN NOT NULL DEFAULT TRUE,
                        require_opening_breakout BOOLEAN NOT NULL DEFAULT TRUE,
                        min_volume_multiplier NUMERIC NOT NULL DEFAULT 1.2,
                        quick_target_pct NUMERIC NOT NULL DEFAULT 0.8,
                        max_stop_loss_pct NUMERIC NOT NULL DEFAULT 0.6,
                        min_risk_reward NUMERIC NOT NULL DEFAULT 1.2,
                        max_trades_per_day INT NOT NULL DEFAULT 3,
                        profit_book_pct NUMERIC NOT NULL DEFAULT 0.45,
                        force_exit_before_close_minutes INT NOT NULL DEFAULT 15,
                        updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                    );
                """)
                cur.execute("ALTER TABLE intraday_settings ADD COLUMN IF NOT EXISTS execution_mode VARCHAR(20) NOT NULL DEFAULT 'paper';")
                cur.execute("ALTER TABLE intraday_settings ADD COLUMN IF NOT EXISTS require_confirmation BOOLEAN NOT NULL DEFAULT TRUE;")
                cur.execute("ALTER TABLE intraday_settings ADD COLUMN IF NOT EXISTS auto_enabled BOOLEAN NOT NULL DEFAULT FALSE;")
                cur.execute("ALTER TABLE intraday_settings ADD COLUMN IF NOT EXISTS auto_interval_seconds INT NOT NULL DEFAULT 300;")
                cur.execute("ALTER TABLE intraday_settings ADD COLUMN IF NOT EXISTS force_exit_before_close_minutes INT NOT NULL DEFAULT 15;")
                cur.execute("ALTER TABLE intraday_settings ADD COLUMN IF NOT EXISTS profit_book_pct NUMERIC NOT NULL DEFAULT 0.45;")
                cur.execute("ALTER TABLE intraday_settings ADD COLUMN IF NOT EXISTS price_filter_enabled BOOLEAN NOT NULL DEFAULT TRUE;")
                cur.execute("ALTER TABLE intraday_settings ADD COLUMN IF NOT EXISTS min_stock_price NUMERIC NOT NULL DEFAULT 50;")
                cur.execute("ALTER TABLE intraday_settings ADD COLUMN IF NOT EXISTS max_stock_price NUMERIC NOT NULL DEFAULT 1500;")
                cur.execute("ALTER TABLE intraday_settings ADD COLUMN IF NOT EXISTS min_order_quantity INT NOT NULL DEFAULT 5;")
                cur.execute("ALTER TABLE intraday_settings ADD COLUMN IF NOT EXISTS min_position_value NUMERIC NOT NULL DEFAULT 10000;")
                # Separate NSE/US price and size filters. The legacy min_stock_price/max_stock_price
                # columns remain for backwards compatibility, but the engine now reads the market-specific
                # values below so ₹1500 rules are never accidentally applied as $1500 rules.
                cur.execute(f"ALTER TABLE intraday_settings ADD COLUMN IF NOT EXISTS min_stock_price_inr NUMERIC NOT NULL DEFAULT {INTRADAY_MIN_STOCK_PRICE_INR};")
                cur.execute(f"ALTER TABLE intraday_settings ADD COLUMN IF NOT EXISTS max_stock_price_inr NUMERIC NOT NULL DEFAULT {INTRADAY_MAX_STOCK_PRICE_INR};")
                cur.execute(f"ALTER TABLE intraday_settings ADD COLUMN IF NOT EXISTS min_position_value_inr NUMERIC NOT NULL DEFAULT {INTRADAY_MIN_POSITION_VALUE_INR};")
                cur.execute(f"ALTER TABLE intraday_settings ADD COLUMN IF NOT EXISTS min_stock_price_usd NUMERIC NOT NULL DEFAULT {INTRADAY_MIN_STOCK_PRICE_USD};")
                cur.execute(f"ALTER TABLE intraday_settings ADD COLUMN IF NOT EXISTS max_stock_price_usd NUMERIC NOT NULL DEFAULT {INTRADAY_MAX_STOCK_PRICE_USD};")
                cur.execute(f"ALTER TABLE intraday_settings ADD COLUMN IF NOT EXISTS min_position_value_usd NUMERIC NOT NULL DEFAULT {INTRADAY_MIN_POSITION_VALUE_USD};")
                cur.execute(f"ALTER TABLE intraday_settings ADD COLUMN IF NOT EXISTS simulation_capital_inr NUMERIC NOT NULL DEFAULT {TRADING_DEFAULT_CAPITAL_INR};")
                cur.execute(f"ALTER TABLE intraday_settings ADD COLUMN IF NOT EXISTS simulation_capital_usd NUMERIC NOT NULL DEFAULT {TRADING_DEFAULT_CAPITAL_USD};")
                # Cost/expectancy gates are saved in DB so the frontend values survive refresh/redeploy.
                # These values decide whether a trade is worth taking after estimated brokerage, STT,
                # exchange charges, GST, stamp duty, slippage and minimum round-trip cost.
                cur.execute(f"ALTER TABLE intraday_settings ADD COLUMN IF NOT EXISTS min_expected_net_profit_inr NUMERIC NOT NULL DEFAULT {INTRADAY_MIN_EXPECTED_NET_PROFIT_INR};")
                cur.execute(f"ALTER TABLE intraday_settings ADD COLUMN IF NOT EXISTS min_expected_net_profit_usd NUMERIC NOT NULL DEFAULT {INTRADAY_MIN_EXPECTED_NET_PROFIT_USD};")
                cur.execute(f"ALTER TABLE intraday_settings ADD COLUMN IF NOT EXISTS min_gross_profit_to_cost_ratio NUMERIC NOT NULL DEFAULT {INTRADAY_MIN_GROSS_PROFIT_TO_COST_RATIO};")
                cur.execute(f"ALTER TABLE intraday_settings ADD COLUMN IF NOT EXISTS claude_control_mode VARCHAR(30) NOT NULL DEFAULT '{CLAUDE_INTRADAY_CONTROL_MODE}';")
                cur.execute(f"ALTER TABLE intraday_settings ADD COLUMN IF NOT EXISTS claude_max_reviews_per_run INT NOT NULL DEFAULT {CLAUDE_INTRADAY_MAX_REVIEWS_PER_RUN};")
                cur.execute(f"ALTER TABLE intraday_settings ADD COLUMN IF NOT EXISTS claude_min_confidence INT NOT NULL DEFAULT {CLAUDE_INTRADAY_MIN_CONFIDENCE};")
                cur.execute("ALTER TABLE intraday_settings ADD COLUMN IF NOT EXISTS claude_target_instruction TEXT;")
                cur.execute("""
                    INSERT INTO intraday_settings
                        (id, enabled, market, execution_mode, require_confirmation, auto_enabled, auto_interval_seconds,
                         opening_wait_minutes, universe_limit, max_candidates, min_score, price_filter_enabled,
                         min_stock_price, max_stock_price, min_order_quantity, min_position_value,
                         min_price_change_pct, max_chase_pct, require_vwap, require_opening_breakout,
                         min_volume_multiplier, quick_target_pct, max_stop_loss_pct, min_risk_reward,
                         max_trades_per_day, profit_book_pct, force_exit_before_close_minutes, simulation_capital_inr, simulation_capital_usd)
                    VALUES (1,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (id) DO NOTHING
                """, (
                    INTRADAY_ENGINE_ENABLED, INTRADAY_DEFAULT_MARKET,
                    INTRADAY_DEFAULT_MODE if INTRADAY_DEFAULT_MODE in ('paper', 'assisted', 'live') else 'paper',
                    INTRADAY_REQUIRE_CONFIRMATION, INTRADAY_AUTO_ENABLED, INTRADAY_AUTO_INTERVAL_SECONDS,
                    INTRADAY_OPENING_WAIT_MINUTES, INTRADAY_UNIVERSE_LIMIT, INTRADAY_MAX_CANDIDATES, INTRADAY_MIN_SCORE,
                    INTRADAY_PRICE_FILTER_ENABLED, INTRADAY_MIN_STOCK_PRICE_INR, INTRADAY_MAX_STOCK_PRICE_INR,
                    INTRADAY_MIN_ORDER_QUANTITY, INTRADAY_MIN_POSITION_VALUE_INR,
                    INTRADAY_MIN_PRICE_CHANGE_PCT, INTRADAY_MAX_CHASE_PCT, INTRADAY_REQUIRE_VWAP,
                    INTRADAY_REQUIRE_OPENING_BREAKOUT, INTRADAY_MIN_VOLUME_MULTIPLIER,
                    INTRADAY_QUICK_TARGET_PCT, INTRADAY_MAX_STOP_LOSS_PCT, INTRADAY_MIN_RISK_REWARD,
                    INTRADAY_MAX_TRADES_PER_DAY, INTRADAY_PROFIT_BOOK_PCT, INTRADAY_FORCE_EXIT_BEFORE_CLOSE_MINUTES,
                    TRADING_DEFAULT_CAPITAL_INR, TRADING_DEFAULT_CAPITAL_USD
                ))
                cur.execute("""
                    INSERT INTO trading_settings
                        (id, enabled, mode, require_confirmation, broker, capital_inr, capital_usd, max_position_pct, risk_per_trade_pct,
                         daily_loss_limit_pct, max_open_positions, min_confidence, min_risk_reward, allow_us_with_inr_capital,
                         weekly_profit_target_pct, max_weekly_loss_pct, cover_api_costs_from_profit,
                         stop_after_weekly_target, stop_after_weekly_loss, broker_api_monthly_cost_inr)
                    VALUES (1, FALSE, %s, TRUE, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                """, (
                    AUTO_TRADING_MODE if AUTO_TRADING_MODE in ('paper', 'assisted', 'live') else 'paper', BROKER_NAME,
                    TRADING_DEFAULT_CAPITAL_INR, TRADING_DEFAULT_CAPITAL_USD, TRADING_MAX_POSITION_PCT, TRADING_RISK_PER_TRADE_PCT,
                    TRADING_DAILY_LOSS_LIMIT_PCT, TRADING_MAX_OPEN_POSITIONS, TRADING_MIN_CONFIDENCE,
                    TRADING_MIN_RISK_REWARD, TRADING_ALLOW_US_WITH_INR_CAPITAL, TRADING_WEEKLY_PROFIT_TARGET_PCT,
                    TRADING_MAX_WEEKLY_LOSS_PCT, TRADING_COVER_API_COSTS_FROM_PROFIT, TRADING_STOP_AFTER_WEEKLY_TARGET,
                    TRADING_STOP_AFTER_WEEKLY_LOSS, TRADING_BROKER_API_MONTHLY_COST_INR
                ))
                # Newer policy: weekly target is a milestone by default, not a hard ceiling.
                # Existing DB rows created by older builds may have this set to TRUE; reset to FALSE
                # so the system can continue taking only strong, protected setups after hitting target.
                cur.execute("UPDATE trading_settings SET stop_after_weekly_target=FALSE WHERE stop_after_weekly_target=TRUE;")

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS trade_orders (
                        id SERIAL PRIMARY KEY,
                        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                        prediction_id INT,
                        source VARCHAR(40) NOT NULL DEFAULT 'scan',
                        mode VARCHAR(20) NOT NULL DEFAULT 'paper',
                        market VARCHAR(20) NOT NULL,
                        horizon VARCHAR(10),
                        ticker VARCHAR(20) NOT NULL,
                        company VARCHAR(200),
                        side VARCHAR(10) NOT NULL,
                        quantity INT NOT NULL DEFAULT 0,
                        order_type VARCHAR(20) NOT NULL DEFAULT 'LIMIT',
                        limit_price NUMERIC,
                        stop_price NUMERIC,
                        target_price NUMERIC,
                        estimated_value NUMERIC,
                        currency_symbol VARCHAR(5),
                        status VARCHAR(40) NOT NULL DEFAULT 'PROPOSED',
                        rationale TEXT,
                        risk_notes TEXT,
                        idempotency_key VARCHAR(160),
                        broker VARCHAR(80),
                        broker_order_id VARCHAR(120),
                        broker_response JSONB,
                        order_payload JSONB
                    );
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_trade_orders_created ON trade_orders (created_at DESC);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_trade_orders_status ON trade_orders (status, market, ticker);")
                cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_trade_orders_idempotency ON trade_orders (idempotency_key) WHERE idempotency_key IS NOT NULL;")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS trade_positions (
                        id SERIAL PRIMARY KEY,
                        opened_at TIMESTAMP NOT NULL DEFAULT NOW(),
                        closed_at TIMESTAMP,
                        mode VARCHAR(20) NOT NULL DEFAULT 'paper',
                        market VARCHAR(20) NOT NULL,
                        horizon VARCHAR(10),
                        ticker VARCHAR(20) NOT NULL,
                        company VARCHAR(200),
                        quantity INT NOT NULL,
                        entry_price NUMERIC NOT NULL,
                        stop_price NUMERIC,
                        target_price NUMERIC,
                        current_price NUMERIC,
                        status VARCHAR(20) NOT NULL DEFAULT 'OPEN',
                        exit_price NUMERIC,
                        exit_reason VARCHAR(80),
                        pnl_amount NUMERIC,
                        pnl_pct NUMERIC,
                        currency_symbol VARCHAR(5),
                        source_order_id INT,
                        metadata_json JSONB,
                        peak_price NUMERIC,
                        best_pnl_pct NUMERIC,
                        trailing_stop_price NUMERIC
                    );
                """)
                cur.execute("ALTER TABLE trade_positions ADD COLUMN IF NOT EXISTS horizon VARCHAR(10);")
                cur.execute("ALTER TABLE trade_positions ADD COLUMN IF NOT EXISTS metadata_json JSONB;")
                cur.execute("ALTER TABLE trade_positions ADD COLUMN IF NOT EXISTS engine VARCHAR(40);")
                cur.execute("UPDATE trade_positions SET engine=COALESCE(metadata_json->>'engine', metadata_json->>'source', CASE WHEN horizon='intraday' THEN 'intraday' ELSE 'trading_automation' END) WHERE engine IS NULL;")
                cur.execute("ALTER TABLE trade_positions ALTER COLUMN engine SET DEFAULT 'trading_automation';")
                cur.execute("ALTER TABLE trade_positions ADD COLUMN IF NOT EXISTS peak_price NUMERIC;")
                cur.execute("ALTER TABLE trade_positions ADD COLUMN IF NOT EXISTS best_pnl_pct NUMERIC;")
                cur.execute("ALTER TABLE trade_positions ADD COLUMN IF NOT EXISTS trailing_stop_price NUMERIC;")
                cur.execute("ALTER TABLE trade_positions ADD COLUMN IF NOT EXISTS gross_pnl_amount NUMERIC;")
                cur.execute("ALTER TABLE trade_positions ADD COLUMN IF NOT EXISTS cost_amount NUMERIC;")
                cur.execute("ALTER TABLE trade_positions ADD COLUMN IF NOT EXISTS net_pnl_amount NUMERIC;")
                cur.execute("ALTER TABLE trade_positions ADD COLUMN IF NOT EXISTS pnl_cost_json JSONB;")
                cur.execute("ALTER TABLE trade_positions ADD COLUMN IF NOT EXISTS expected_round_trip_cost NUMERIC;")
                cur.execute("ALTER TABLE trade_positions ADD COLUMN IF NOT EXISTS expected_net_profit_at_target NUMERIC;")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_trade_positions_open ON trade_positions (status, mode, ticker);")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS intraday_run_log (
                        id SERIAL PRIMARY KEY,
                        started_at TIMESTAMP NOT NULL,
                        finished_at TIMESTAMP NOT NULL DEFAULT NOW(),
                        trigger VARCHAR(40),
                        market VARCHAR(20),
                        mode VARCHAR(20),
                        session_state VARCHAR(60),
                        message TEXT,
                        scanned_count INT NOT NULL DEFAULT 0,
                        eligible_count INT NOT NULL DEFAULT 0,
                        watch_count INT NOT NULL DEFAULT 0,
                        order_count INT NOT NULL DEFAULT 0,
                        exit_count INT NOT NULL DEFAULT 0,
                        can_enter BOOLEAN NOT NULL DEFAULT FALSE,
                        data_provider VARCHAR(80),
                        data_source VARCHAR(160),
                        data_source_type VARCHAR(120),
                        claude_cost_inr NUMERIC NOT NULL DEFAULT 0,
                        result_json JSONB
                    );
                """)
                cur.execute("ALTER TABLE intraday_run_log ADD COLUMN IF NOT EXISTS data_provider VARCHAR(80);")
                cur.execute("ALTER TABLE intraday_run_log ADD COLUMN IF NOT EXISTS data_source VARCHAR(160);")
                cur.execute("ALTER TABLE intraday_run_log ADD COLUMN IF NOT EXISTS data_source_type VARCHAR(120);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_intraday_run_log_finished ON intraday_run_log (finished_at DESC);")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS trading_automation_run_log (
                        id SERIAL PRIMARY KEY,
                        started_at TIMESTAMP NOT NULL,
                        finished_at TIMESTAMP NOT NULL DEFAULT NOW(),
                        trigger VARCHAR(40),
                        market VARCHAR(20),
                        horizon VARCHAR(20),
                        mode VARCHAR(20),
                        message TEXT,
                        locked_scan_available BOOLEAN NOT NULL DEFAULT FALSE,
                        entry_check_count INT NOT NULL DEFAULT 0,
                        approved_count INT NOT NULL DEFAULT 0,
                        blocked_count INT NOT NULL DEFAULT 0,
                        order_count INT NOT NULL DEFAULT 0,
                        exit_count INT NOT NULL DEFAULT 0,
                        can_open_new_trades BOOLEAN NOT NULL DEFAULT FALSE,
                        session_state VARCHAR(60),
                        data_provider VARCHAR(80),
                        data_source VARCHAR(160),
                        data_source_type VARCHAR(120),
                        result_json JSONB
                    );
                """)
                cur.execute("ALTER TABLE trading_automation_run_log ADD COLUMN IF NOT EXISTS locked_scan_available BOOLEAN NOT NULL DEFAULT FALSE;")
                cur.execute("ALTER TABLE trading_automation_run_log ADD COLUMN IF NOT EXISTS approved_count INT NOT NULL DEFAULT 0;")
                cur.execute("ALTER TABLE trading_automation_run_log ADD COLUMN IF NOT EXISTS blocked_count INT NOT NULL DEFAULT 0;")
                cur.execute("ALTER TABLE trading_automation_run_log ADD COLUMN IF NOT EXISTS data_provider VARCHAR(80);")
                cur.execute("ALTER TABLE trading_automation_run_log ADD COLUMN IF NOT EXISTS data_source VARCHAR(160);")
                cur.execute("ALTER TABLE trading_automation_run_log ADD COLUMN IF NOT EXISTS data_source_type VARCHAR(120);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_trading_automation_run_log_finished ON trading_automation_run_log (finished_at DESC);")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS paper_auto_run_log (
                        id SERIAL PRIMARY KEY,
                        started_at TIMESTAMP NOT NULL,
                        finished_at TIMESTAMP NOT NULL DEFAULT NOW(),
                        trigger VARCHAR(40),
                        message TEXT,
                        daily_week_count INT NOT NULL DEFAULT 0,
                        intraday_count INT NOT NULL DEFAULT 0,
                        order_count INT NOT NULL DEFAULT 0,
                        exit_count INT NOT NULL DEFAULT 0,
                        skipped_count INT NOT NULL DEFAULT 0,
                        result_json JSONB
                    );
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_paper_auto_run_log_finished ON paper_auto_run_log (finished_at DESC);")


                cur.execute("""
                    CREATE TABLE IF NOT EXISTS backtest_runs (
                        id SERIAL PRIMARY KEY,
                        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                        market VARCHAR(20) NOT NULL,
                        horizon VARCHAR(20) NOT NULL,
                        start_date DATE,
                        end_date DATE,
                        universe_limit INT,
                        top_n INT,
                        rebalance_step_days INT,
                        total_trades INT NOT NULL DEFAULT 0,
                        win_rate_pct NUMERIC,
                        expectancy_pct NUMERIC,
                        avg_trade_pct NUMERIC,
                        avg_win_pct NUMERIC,
                        avg_loss_pct NUMERIC,
                        profit_factor NUMERIC,
                        sharpe NUMERIC,
                        sortino NUMERIC,
                        max_drawdown_pct NUMERIC,
                        best_regime VARCHAR(80),
                        worst_regime VARCHAR(80),
                        status VARCHAR(30) NOT NULL DEFAULT 'complete',
                        error TEXT,
                        metrics_json JSONB,
                        config_json JSONB
                    );
                """)
                cur.execute("ALTER TABLE backtest_runs ADD COLUMN IF NOT EXISTS fingerprint_hash VARCHAR(80);")
                cur.execute("ALTER TABLE backtest_runs ADD COLUMN IF NOT EXISTS duplicate_of_run_id INT;")
                cur.execute("ALTER TABLE backtest_runs ADD COLUMN IF NOT EXISTS learning_applied BOOLEAN NOT NULL DEFAULT TRUE;")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_backtest_runs_created ON backtest_runs (created_at DESC);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_backtest_runs_fingerprint ON backtest_runs (fingerprint_hash);")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS backtest_trades (
                        id SERIAL PRIMARY KEY,
                        run_id INT REFERENCES backtest_runs(id) ON DELETE CASCADE,
                        ticker VARCHAR(20),
                        market VARCHAR(20),
                        horizon VARCHAR(20),
                        signal_date DATE,
                        entry_date DATE,
                        exit_date DATE,
                        entry_price NUMERIC,
                        exit_price NUMERIC,
                        target_price NUMERIC,
                        stop_price NUMERIC,
                        raw_return_pct NUMERIC,
                        net_return_pct NUMERIC,
                        outcome VARCHAR(60),
                        regime VARCHAR(80),
                        score NUMERIC,
                        rsi NUMERIC,
                        volume_ratio NUMERIC,
                        atr_pct NUMERIC,
                        features_json JSONB
                    );
                """)
                cur.execute("ALTER TABLE backtest_trades ADD COLUMN IF NOT EXISTS failure_reason TEXT;")
                cur.execute("ALTER TABLE backtest_trades ADD COLUMN IF NOT EXISTS lesson_summary TEXT;")
                cur.execute("ALTER TABLE backtest_trades ADD COLUMN IF NOT EXISTS pattern_key VARCHAR(200);")
                cur.execute("ALTER TABLE backtest_trades ADD COLUMN IF NOT EXISTS pattern_verdict VARCHAR(40);")
                cur.execute("ALTER TABLE backtest_trades ADD COLUMN IF NOT EXISTS unprotected_exit_price NUMERIC;")
                cur.execute("ALTER TABLE backtest_trades ADD COLUMN IF NOT EXISTS unprotected_net_return_pct NUMERIC;")
                cur.execute("ALTER TABLE backtest_trades ADD COLUMN IF NOT EXISTS unprotected_outcome VARCHAR(80);")
                cur.execute("ALTER TABLE backtest_trades ADD COLUMN IF NOT EXISTS protected_exit_price NUMERIC;")
                cur.execute("ALTER TABLE backtest_trades ADD COLUMN IF NOT EXISTS protected_net_return_pct NUMERIC;")
                cur.execute("ALTER TABLE backtest_trades ADD COLUMN IF NOT EXISTS protected_outcome VARCHAR(80);")
                cur.execute("ALTER TABLE backtest_trades ADD COLUMN IF NOT EXISTS protected_action VARCHAR(80);")
                cur.execute("ALTER TABLE backtest_trades ADD COLUMN IF NOT EXISTS protected_reason TEXT;")
                cur.execute("ALTER TABLE backtest_trades ADD COLUMN IF NOT EXISTS avoided_loss_pct NUMERIC;")
                cur.execute("ALTER TABLE backtest_trades ADD COLUMN IF NOT EXISTS missed_profit_pct NUMERIC;")
                cur.execute("ALTER TABLE backtest_trades ADD COLUMN IF NOT EXISTS entry_learning_return_pct NUMERIC;")
                cur.execute("ALTER TABLE backtest_trades ADD COLUMN IF NOT EXISTS exit_learning_delta_pct NUMERIC;")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_backtest_trades_run ON backtest_trades (run_id, signal_date DESC);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_backtest_trades_ticker ON backtest_trades (market, horizon, ticker, signal_date DESC);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_backtest_trades_pattern ON backtest_trades (market, horizon, pattern_key);")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS strategy_signal_stats (
                        id SERIAL PRIMARY KEY,
                        updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                        market VARCHAR(20),
                        horizon VARCHAR(20),
                        regime VARCHAR(80),
                        pattern_key VARCHAR(200),
                        trades INT NOT NULL DEFAULT 0,
                        win_rate_pct NUMERIC,
                        expectancy_pct NUMERIC,
                        avg_return_pct NUMERIC,
                        confidence_adjustment NUMERIC NOT NULL DEFAULT 0,
                        notes TEXT,
                        UNIQUE (market, horizon, regime, pattern_key)
                    );
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_strategy_signal_stats_lookup ON strategy_signal_stats (market, horizon, regime, updated_at DESC);")
    finally:
        conn.close()


init_db()


def iter_chunks(items: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


# Some exchange symbols change after mergers/demergers/name changes. Keep the
# app from repeatedly asking yfinance for stale symbols that now return 404.
# Users can override the full universe with MARKET_UNIVERSE_IN/US/GLOBAL.
KNOWN_TICKER_ALIASES = {
    'TATAMOTORS.NS': 'TMCV.NS',      # Tata Motors post-demerger/current Yahoo/NSE symbol
    'ADANITRANS.NS': 'ADANIENSOL.NS',
}


def clean_tickers(tickers: Iterable[str]) -> List[str]:
    seen = set()
    cleaned = []
    for raw in tickers:
        t = str(raw).strip().upper()
        if not t or t.startswith('#'):
            continue
        # Replace old/stale symbols with the current data-provider symbol.
        # This prevents backtests from failing on known 404 symbols like TATAMOTORS.NS.
        t = KNOWN_TICKER_ALIASES.get(t, t)
        if not t or t in seen:
            continue
        seen.add(t)
        cleaned.append(t)
    return cleaned


def load_market_universe(market: str) -> List[str]:
    env_key = f"MARKET_UNIVERSE_{market.upper()}"
    if os.environ.get(env_key):
        return clean_tickers(os.environ[env_key].replace('\n', ',').split(','))

    file_path = os.path.join(os.path.dirname(__file__), 'data', f'universe_{market}.txt')
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            return clean_tickers(f.read().replace('\n', ',').split(','))

    return clean_tickers(DEFAULT_UNIVERSES.get(market, DEFAULT_UNIVERSES['US']))


def compute_rsi(prices, period=14):
    if len(prices) < period + 1:
        return None
    deltas = np.diff(prices)
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    avg_gain = float(np.mean(gains[-period:]))
    avg_loss = float(np.mean(losses[-period:]))
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 1)


def compute_ema(prices, span):
    if len(prices) == 0:
        return None
    k = 2 / (span + 1)
    ema = float(prices[0])
    for p in prices[1:]:
        ema = float(p) * k + ema * (1 - k)
    return ema


def compute_macd(prices):
    if len(prices) < 35:
        return None, None, None
    ema12 = compute_ema(prices[-60:], 12)
    ema26 = compute_ema(prices[-60:], 26)
    if ema12 is None or ema26 is None:
        return None, None, None
    macd = ema12 - ema26

    macd_series = []
    recent = prices[-60:]
    for i in range(26, len(recent) + 1):
        e12 = compute_ema(recent[:i], 12)
        e26 = compute_ema(recent[:i], 26)
        if e12 is not None and e26 is not None:
            macd_series.append(e12 - e26)
    signal = compute_ema(macd_series[-9:], 9) if len(macd_series) >= 9 else macd
    hist = macd - signal
    return round(macd, 4), round(signal, 4), round(hist, 4)


def pct_change(current: float, past: Optional[float]) -> Optional[float]:
    if not past or past == 0:
        return None
    return round(((current - float(past)) / float(past)) * 100, 2)


def build_stock_snapshot(ticker: str, hist) -> Optional[Dict[str, Any]]:
    try:
        if hist is None or hist.empty or 'Close' not in hist:
            return None
        hist = hist.dropna(subset=['Close'])
        if hist.empty or len(hist) < 30:
            return None

        closes = hist['Close'].astype(float).values
        highs = hist['High'].astype(float).values if 'High' in hist else closes
        lows = hist['Low'].astype(float).values if 'Low' in hist else closes
        volumes = hist['Volume'].fillna(0).astype(float).values if 'Volume' in hist else np.zeros(len(closes))

        current = round(float(closes[-1]), 2)
        if current <= 0:
            return None
        prev = float(closes[-2]) if len(closes) >= 2 else current
        day_chg = pct_change(current, prev)
        week_chg = pct_change(current, closes[-5]) if len(closes) >= 5 else None
        month_chg = pct_change(current, closes[-22]) if len(closes) >= 22 else None
        quarter_chg = pct_change(current, closes[-63]) if len(closes) >= 63 else None
        six_month_chg = pct_change(current, closes[-126]) if len(closes) >= 126 else None

        rsi = compute_rsi(closes)
        macd, macd_sig, macd_hist = compute_macd(closes)
        ma20 = round(float(np.mean(closes[-20:])), 2) if len(closes) >= 20 else None
        ma50 = round(float(np.mean(closes[-50:])), 2) if len(closes) >= 50 else None
        ma200 = round(float(np.mean(closes[-200:])), 2) if len(closes) >= 200 else None
        avg_vol_20 = int(np.mean(volumes[-20:])) if len(volumes) >= 20 else int(np.mean(volumes))
        avg_vol_50 = int(np.mean(volumes[-50:])) if len(volumes) >= 50 else avg_vol_20
        vol_ratio = round(float(volumes[-1]) / avg_vol_20, 2) if avg_vol_20 > 0 else 1.0
        avg_dollar_volume_20 = round(avg_vol_20 * current, 2)

        year_high = round(float(max(highs[-252:] if len(highs) >= 252 else highs)), 2)
        year_low = round(float(min(lows[-252:] if len(lows) >= 252 else lows)), 2)
        pct_from_high = pct_change(current, year_high)
        pct_from_low = pct_change(current, year_low)

        if len(closes) >= 20:
            std20 = float(np.std(closes[-20:]))
            bb_upper = round(ma20 + 2 * std20, 2)
            bb_lower = round(ma20 - 2 * std20, 2)
            bb_position = round((current - bb_lower) / (bb_upper - bb_lower) * 100, 1) if (bb_upper - bb_lower) != 0 else 50.0
        else:
            bb_upper = bb_lower = bb_position = None

        support_20 = round(float(min(lows[-20:])), 2)
        resistance_20 = round(float(max(highs[-20:])), 2)
        support_distance_pct = round(((current - support_20) / current) * 100, 2) if current else None
        resistance_distance_pct = round(((resistance_20 - current) / current) * 100, 2) if current else None

        return {
            'ticker': ticker,
            'company': ticker,
            'sector': 'N/A',
            'current_price': current,
            'day_change_pct': day_chg,
            'week_change_pct': week_chg,
            'month_change_pct': month_chg,
            'quarter_change_pct': quarter_chg,
            'six_month_change_pct': six_month_chg,
            'rsi_14': rsi,
            'macd': macd,
            'macd_signal': macd_sig,
            'macd_histogram': macd_hist,
            'macd_crossover': 'bullish' if (macd is not None and macd_sig is not None and macd > macd_sig) else 'bearish',
            'ma20': ma20,
            'ma50': ma50,
            'ma200': ma200,
            'price_vs_ma20_pct': round(((current - ma20) / ma20) * 100, 2) if ma20 else None,
            'price_vs_ma50_pct': round(((current - ma50) / ma50) * 100, 2) if ma50 else None,
            'price_vs_ma200_pct': round(((current - ma200) / ma200) * 100, 2) if ma200 else None,
            'bb_upper': bb_upper,
            'bb_lower': bb_lower,
            'bb_position_pct': bb_position,
            'volume_ratio_vs_20d_avg': vol_ratio,
            'avg_volume_20': avg_vol_20,
            'avg_volume_50': avg_vol_50,
            'avg_dollar_volume_20': avg_dollar_volume_20,
            '52w_high': year_high,
            '52w_low': year_low,
            'pct_from_52w_high': pct_from_high,
            'pct_from_52w_low': pct_from_low,
            'support_20': support_20,
            'resistance_20': resistance_20,
            'support_distance_pct': support_distance_pct,
            'resistance_distance_pct': resistance_distance_pct,
        }
    except Exception as e:
        print(f"[WARN] Snapshot failed for {ticker}: {e}")
        return None


def extract_ticker_frame(data, ticker: str, chunk_size: int):
    if data is None or data.empty:
        return None
    try:
        if hasattr(data.columns, 'nlevels') and data.columns.nlevels > 1:
            if ticker in data.columns.get_level_values(0):
                return data[ticker].dropna(how='all')
            return None
        return data.dropna(how='all') if chunk_size == 1 else None
    except Exception:
        return None


def download_universe_snapshots(tickers: List[str]) -> List[Dict[str, Any]]:
    snapshots = []
    for chunk in iter_chunks(tickers, BATCH_SIZE):
        try:
            data = yf.download(
                tickers=' '.join(chunk),
                period='1y',
                interval='1d',
                group_by='ticker',
                auto_adjust=False,
                threads=True,
                progress=False,
                timeout=YF_TIMEOUT,
            )
        except Exception as e:
            print(f"[WARN] Batch download failed for {len(chunk)} tickers: {e}")
            data = None

        for ticker in chunk:
            frame = extract_ticker_frame(data, ticker, len(chunk))
            snapshot = build_stock_snapshot(ticker, frame)
            if snapshot:
                snapshots.append(snapshot)
    return snapshots


def liquidity_filter(snapshots: List[Dict[str, Any]], market: str) -> List[Dict[str, Any]]:
    rules = LIQUIDITY_RULES.get(market, LIQUIDITY_RULES['US'])
    liquid = []
    for s in snapshots:
        if s.get('avg_volume_20', 0) >= rules['min_avg_volume_20'] and s.get('avg_dollar_volume_20', 0) >= rules['min_avg_dollar_volume_20']:
            s['quality_gate'] = {'passed': True, 'reason': 'Active/liquid enough for scan', 'rules': rules}
            liquid.append(s)
        else:
            s['quality_gate'] = {'passed': False, 'reason': 'Skipped: low average volume or low value traded', 'rules': rules}
    return liquid


def snapshot_passes_liquidity(snapshot: Dict[str, Any], market: str) -> Tuple[bool, str]:
    rules = LIQUIDITY_RULES.get(market, LIQUIDITY_RULES['US'])
    vol = float(snapshot.get('avg_volume_20') or 0)
    value = float(snapshot.get('avg_dollar_volume_20') or 0)
    if vol < float(rules.get('min_avg_volume_20') or 0):
        return False, f'low average volume {round(vol):,}; needs {rules.get("min_avg_volume_20"):,}+'
    if value < float(rules.get('min_avg_dollar_volume_20') or 0):
        return False, f'low average traded value {round(value):,}; needs {rules.get("min_avg_dollar_volume_20"):,}+'
    return True, 'liquidity passed'


def score_momentum(s: Dict[str, Any]) -> float:
    score = 0.0
    for key, weight in [('week_change_pct', 0.65), ('month_change_pct', 0.45), ('quarter_change_pct', 0.25)]:
        val = s.get(key)
        if val is not None:
            score += max(min(float(val), 25), -25) * weight
    if s.get('price_vs_ma20_pct') is not None:
        score += max(min(float(s['price_vs_ma20_pct']), 12), -12) * 0.8
    if s.get('price_vs_ma50_pct') is not None:
        score += max(min(float(s['price_vs_ma50_pct']), 18), -18) * 0.5
    if s.get('volume_ratio_vs_20d_avg') is not None:
        score += min(max(float(s['volume_ratio_vs_20d_avg']) - 1.0, 0), 3.0) * 8
    if s.get('macd_histogram') is not None and s['macd_histogram'] > 0:
        score += 8
    if s.get('rsi_14') is not None:
        rsi = float(s['rsi_14'])
        if 45 <= rsi <= 68:
            score += 12
        elif 35 <= rsi < 45:
            score += 5
        elif rsi > 76:
            score -= 10
    return round(score, 2)


def score_technical(s: Dict[str, Any]) -> float:
    score = 0.0
    current = s.get('current_price')
    ma20, ma50, ma200 = s.get('ma20'), s.get('ma50'), s.get('ma200')
    if current and ma20 and ma50 and current > ma20 > ma50:
        score += 20
    elif current and ma20 and current > ma20:
        score += 9
    if ma50 and ma200 and ma50 > ma200:
        score += 12
    if s.get('macd_histogram') is not None:
        score += 14 if s['macd_histogram'] > 0 else -6
    if s.get('rsi_14') is not None:
        rsi = float(s['rsi_14'])
        if 50 <= rsi <= 65:
            score += 16
        elif 40 <= rsi < 50:
            score += 8
        elif 30 <= rsi < 40:
            score += 5
        elif rsi > 72:
            score -= 9
    if s.get('bb_position_pct') is not None:
        bb = float(s['bb_position_pct'])
        if 25 <= bb <= 75:
            score += 8
        elif bb < 20:
            score += 5
        elif bb > 90:
            score -= 8
    if s.get('resistance_distance_pct') is not None and s.get('support_distance_pct') is not None:
        upside = max(float(s['resistance_distance_pct']), 0.01)
        downside = max(float(s['support_distance_pct']), 0.01)
        rr_proxy = upside / downside
        score += min(rr_proxy * 5, 15)
    if s.get('volume_ratio_vs_20d_avg') is not None and s['volume_ratio_vs_20d_avg'] >= 1.3:
        score += 8
    return round(score, 2)


def apply_momentum_stage(liquid: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    for s in liquid:
        s['momentum_score'] = score_momentum(s)
    return sorted(liquid, key=lambda x: x.get('momentum_score', 0), reverse=True)[:MOMENTUM_STAGE_LIMIT]


def apply_technical_stage(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    for s in candidates:
        s['technical_score'] = score_technical(s)
        s['preliminary_score'] = round((s.get('momentum_score', 0) * 0.45) + (s.get('technical_score', 0) * 0.55), 2)
    return sorted(candidates, key=lambda x: x.get('preliminary_score', 0), reverse=True)[:TECHNICAL_STAGE_LIMIT]


def get_recent_news(stock, limit=3):
    try:
        raw = stock.news or []
    except Exception:
        return []
    headlines = []
    for item in raw[:limit]:
        node = item.get('content', item) if isinstance(item, dict) else {}
        title = node.get('title')
        provider = node.get('provider')
        publisher = provider.get('displayName') if isinstance(provider, dict) else node.get('publisher')
        if title:
            headlines.append({'title': title, 'source': publisher or 'unknown'})
    return headlines


POSITIVE_NEWS_WORDS = {
    'beat', 'beats', 'upgrade', 'upgraded', 'raises', 'raised', 'growth', 'record', 'profit', 'profits',
    'approval', 'approved', 'partnership', 'contract', 'buyback', 'dividend', 'launch', 'surge', 'rally',
    'strong', 'outperform', 'wins', 'expands', 'expansion'
}
NEGATIVE_NEWS_WORDS = {
    'miss', 'misses', 'downgrade', 'downgraded', 'cuts', 'cut', 'lawsuit', 'probe', 'investigation',
    'fraud', 'recall', 'slump', 'falls', 'fall', 'weak', 'loss', 'losses', 'layoffs', 'warning',
    'regulatory', 'penalty', 'fine', 'bankruptcy', 'default'
}


def score_news_sentiment(headlines: List[Dict[str, str]]) -> Tuple[int, str]:
    if not headlines:
        return 0, 'No material recent headlines found; technical and fundamental setup carries more weight.'
    score = 0
    titles = []
    for h in headlines:
        title = h.get('title', '')
        titles.append(title)
        tokens = set(re.findall(r"[a-zA-Z]+", title.lower()))
        score += 6 * len(tokens & POSITIVE_NEWS_WORDS)
        score -= 8 * len(tokens & NEGATIVE_NEWS_WORDS)
    score = max(min(score, 20), -25)
    if score > 0:
        summary = 'Recent headlines lean positive and support the setup.'
    elif score < 0:
        summary = 'Recent headlines contain negative-risk language; confidence is reduced.'
    else:
        summary = 'Recent headlines look neutral; no obvious catalyst edge from news.'
    return score, summary


MACRO_RISK_KEYWORDS = {
    'war/geopolitical shock': {'war','wars','attack','attacks','missile','conflict','invasion','border','sanction','sanctions','geopolitical','hostage','terror','escalation'},
    'tariff/trade policy': {'tariff','tariffs','duties','duty','trade','trade-deal','deal','import','export','quota','protectionism'},
    'rates/inflation': {'inflation','rate','rates','fed','rbi','yield','yields','hawkish','recession','slowdown','cpi','policy'},
    'oil/energy shock': {'oil','crude','brent','opec','gas','energy','fuel'},
    'currency/rupee-dollar risk': {'dollar','rupee','usd','currency','forex','devaluation'},
    'regulation/legal risk': {'regulatory','probe','investigation','lawsuit','ban','penalty','fine','fraud'},
    'positive trade/easing catalyst': {'ceasefire','easing','stimulus','cut','cuts','deal','agreement','approval','peace','record','beat','growth'},
}


def macro_news_tickers(market: str) -> List[str]:
    if market == 'IN':
        return ['^NSEI', '^NSEBANK', 'INDA', 'INR=X', 'CL=F', 'GC=F']
    if market == 'US':
        return ['SPY', 'QQQ', '^GSPC', '^IXIC', '^VIX', 'CL=F', 'DX-Y.NYB']
    return ['SPY', 'QQQ', 'INDA', 'EEM', '^VIX', 'CL=F', 'DX-Y.NYB']


def get_macro_event_context(market: str) -> Dict[str, Any]:
    """Current macro/news risk used only for forward-looking decisions.

    This does not promise to catch every event. It uses free/yfinance headlines
    plus optional manual override env vars. For live capital, connect a proper
    news provider and keep manual risk overrides available during shocks.
    """
    if not MACRO_RISK_ENABLED:
        return {'enabled': False, 'risk_level': 'not_used', 'risk_score': 0, 'decision': 'not_used', 'factors': [], 'headlines': []}
    manual_map = {'low': 0, 'caution': MACRO_RISK_CAUTION_THRESHOLD, 'high': MACRO_RISK_BLOCK_THRESHOLD, 'block': MACRO_RISK_BLOCK_THRESHOLD + 10}
    headlines: List[Dict[str, Any]] = []
    factors: Dict[str, int] = {}
    risk_score = 0.0
    positive_offset = 0.0
    try:
        for t in macro_news_tickers(market):
            if len(headlines) >= MACRO_RISK_HEADLINE_LIMIT:
                break
            for h in get_recent_news(yf.Ticker(t), limit=4):
                title = h.get('title') or ''
                low = title.lower()
                tokens = set(re.findall(r"[a-zA-Z-]+", low))
                local_score = 0
                local_factors = []
                for label, words in MACRO_RISK_KEYWORDS.items():
                    hit = tokens & words
                    if not hit:
                        continue
                    local_factors.append(label)
                    factors[label] = factors.get(label, 0) + len(hit)
                    if label.startswith('positive'):
                        positive_offset += 4 * len(hit)
                    elif label in ('war/geopolitical shock', 'tariff/trade policy'):
                        local_score += 10 * len(hit)
                    elif label in ('rates/inflation', 'oil/energy shock'):
                        local_score += 7 * len(hit)
                    else:
                        local_score += 6 * len(hit)
                if local_score or local_factors:
                    headlines.append({**h, 'ticker': t, 'risk_score': local_score, 'factors': local_factors})
                    risk_score += local_score
        risk_score = max(0.0, min(80.0, risk_score - min(positive_offset, 18)))
    except Exception as e:
        return {'enabled': True, 'risk_level': 'unknown', 'risk_score': 0, 'decision': 'normal', 'factors': [], 'headlines': [], 'warning': str(e)[:160]}
    if MACRO_RISK_MANUAL_LEVEL in manual_map:
        risk_score = max(risk_score, float(manual_map[MACRO_RISK_MANUAL_LEVEL]))
        headlines.insert(0, {'title': MACRO_RISK_MANUAL_NOTE or f'Manual macro risk override: {MACRO_RISK_MANUAL_LEVEL}', 'source': 'manual override', 'ticker': market, 'risk_score': risk_score, 'factors': ['manual override']})
        factors['manual override'] = factors.get('manual override', 0) + 1
    if risk_score >= MACRO_RISK_BLOCK_THRESHOLD:
        risk_level = 'high_risk'
        decision = 'block_or_reduce_new_entries'
        note = 'Major event risk detected. System should avoid new trades unless setup is exceptional and risk controls are tight.'
    elif risk_score >= MACRO_RISK_CAUTION_THRESHOLD:
        risk_level = 'caution'
        decision = 'reduce_confidence_and_size'
        note = 'Macro/news risk exists. System should reduce confidence, demand stronger confirmation, and trade less.'
    else:
        risk_level = 'normal'
        decision = 'normal'
        note = 'No major macro/news risk detected from available free headlines.'
    return {
        'enabled': True,
        'risk_level': risk_level,
        'risk_score': round(risk_score, 2),
        'decision': decision,
        'note': note,
        'factors': sorted(factors.items(), key=lambda kv: kv[1], reverse=True)[:8],
        'headlines': headlines[:MACRO_RISK_HEADLINE_LIMIT],
        'thresholds': {'caution': MACRO_RISK_CAUTION_THRESHOLD, 'block': MACRO_RISK_BLOCK_THRESHOLD},
        'important_note': 'Current macro/news layer is used for forward scans/trading. Historical backtests intentionally do not use today’s news to avoid lookahead bias.',
    }


def merge_macro_into_market_context(base_context: Optional[Dict[str, Any]], market: str) -> Dict[str, Any]:
    ctx = dict(base_context or {})
    macro = get_macro_event_context(market)
    ctx['macro_event_risk'] = macro
    if macro.get('risk_level') == 'high_risk':
        ctx['regime'] = 'risk_off_or_high_vol'
        ctx['macro_block_new_trades'] = True
    elif macro.get('risk_level') == 'caution':
        ctx['macro_caution'] = True
    return ctx


def score_fundamentals(info: Dict[str, Any]) -> Tuple[float, Dict[str, Any]]:
    revenue_growth = info.get('revenueGrowth')
    earnings_growth = info.get('earningsQuarterlyGrowth')
    roe = info.get('returnOnEquity')
    debt_to_equity = info.get('debtToEquity')
    free_cashflow = info.get('freeCashflow')
    operating_cashflow = info.get('operatingCashflow')
    trailing_eps = info.get('trailingEps')
    profit_margins = info.get('profitMargins')
    current_ratio = info.get('currentRatio')

    score = 0.0
    if revenue_growth is not None:
        score += max(min(float(revenue_growth) * 100, 25), -20)
    if earnings_growth is not None:
        score += max(min(float(earnings_growth) * 60, 20), -20)
    if roe is not None:
        score += max(min(float(roe) * 80, 18), -10)
    if debt_to_equity is not None:
        score += 8 if float(debt_to_equity) < 80 else (-8 if float(debt_to_equity) > 200 else 0)
    if free_cashflow is not None:
        score += 8 if float(free_cashflow) > 0 else -8
    if operating_cashflow is not None:
        score += 6 if float(operating_cashflow) > 0 else -6
    if trailing_eps is not None:
        score += 6 if float(trailing_eps) > 0 else -6
    if profit_margins is not None:
        score += max(min(float(profit_margins) * 50, 10), -8)
    if current_ratio is not None:
        score += 4 if float(current_ratio) >= 1 else -4

    readings = {
        'revenue_growth_pct': round(float(revenue_growth) * 100, 1) if revenue_growth is not None else None,
        'earnings_growth_pct': round(float(earnings_growth) * 100, 1) if earnings_growth is not None else None,
        'roe_pct': round(float(roe) * 100, 1) if roe is not None else None,
        'debt_to_equity': round(float(debt_to_equity), 1) if debt_to_equity is not None else None,
        'free_cashflow_bn': round(float(free_cashflow) / 1e9, 2) if free_cashflow is not None else None,
        'operating_cashflow_bn': round(float(operating_cashflow) / 1e9, 2) if operating_cashflow is not None else None,
        'trailing_eps': round(float(trailing_eps), 2) if trailing_eps is not None else None,
        'profit_margin_pct': round(float(profit_margins) * 100, 1) if profit_margins is not None else None,
        'current_ratio': round(float(current_ratio), 2) if current_ratio is not None else None,
        'trailing_pe': round(float(info.get('trailingPE')), 1) if info.get('trailingPE') is not None else None,
        'forward_pe': round(float(info.get('forwardPE')), 1) if info.get('forwardPE') is not None else None,
        'market_cap_bn': round(float(info.get('marketCap')) / 1e9, 1) if info.get('marketCap') is not None else None,
    }
    return round(score, 2), readings


def enrich_candidate(ticker_candidate: Dict[str, Any]) -> Dict[str, Any]:
    ticker = ticker_candidate['ticker']
    enriched = dict(ticker_candidate)
    try:
        stock = yf.Ticker(ticker)
        info = stock.info or {}
    except Exception as e:
        print(f"[WARN] Fundamentals failed for {ticker}: {e}")
        info = {}

    company_name = info.get('longName') or info.get('shortName') or ticker
    sector = info.get('sector') or 'N/A'
    fundamental_score, fundamental_readings = score_fundamentals(info)
    news = get_recent_news(yf.Ticker(ticker), limit=3)
    news_score, news_reading = score_news_sentiment(news)

    enriched.update({
        'company': company_name,
        'sector': sector,
        'pe_ratio': fundamental_readings.get('trailing_pe'),
        'market_cap_bn': fundamental_readings.get('market_cap_bn'),
        'fundamental_score': fundamental_score,
        'fundamentals': fundamental_readings,
        'recent_news': news,
        'news_score': news_score,
        'news_reading': news_reading,
    })
    enriched['final_pre_ai_score'] = round(
        (enriched.get('momentum_score', 0) * 0.30) +
        (enriched.get('technical_score', 0) * 0.40) +
        (enriched.get('fundamental_score', 0) * 0.20) +
        (enriched.get('news_score', 0) * 0.10),
        2,
    )
    return enriched


def enrich_deep_candidates(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deep_seed = sorted(candidates, key=lambda x: x.get('preliminary_score', 0), reverse=True)[:DEEP_ANALYSIS_LIMIT]
    enriched = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_BACKGROUND_WORKERS) as executor:
        futures = [executor.submit(enrich_candidate, c) for c in deep_seed]
        for f in concurrent.futures.as_completed(futures):
            try:
                enriched.append(f.result())
            except Exception as e:
                print(f"[WARN] enrich_deep_candidates item failed: {e}")
    return sorted(enriched, key=lambda x: x.get('final_pre_ai_score', 0), reverse=True)


def market_trend_index_ticker(market: str) -> str:
    if market == 'IN':
        return '^NSEI'
    if market == 'US':
        return '^GSPC'
    return 'ACWI'


def get_market_trend_context(market: str) -> Dict[str, Any]:
    ticker = market_trend_index_ticker(market)
    try:
        h = yf.Ticker(ticker).history(period='1y', interval='1d', timeout=YF_TIMEOUT)
        if h is None or h.empty or 'Close' not in h or len(h) < 60:
            return {'index': ticker, 'direction': 'unknown', 'note': 'Not enough index trend data.'}
        close = h['Close'].dropna().astype(float)
        if len(close) < 60:
            return {'index': ticker, 'direction': 'unknown', 'note': 'Not enough clean index trend data.'}
        last = float(close.iloc[-1])
        ma20 = float(close.tail(20).mean())
        ma50 = float(close.tail(50).mean())
        ma200 = float(close.tail(min(200, len(close))).mean())
        ret20 = ((last / float(close.iloc[-20])) - 1) * 100 if len(close) >= 20 and float(close.iloc[-20]) else 0
        ret63 = ((last / float(close.iloc[-63])) - 1) * 100 if len(close) >= 63 and float(close.iloc[-63]) else 0
        if last > ma50 and ma20 > ma50 and ret20 > 1.0:
            direction = 'bullish'
        elif last < ma50 and ma20 < ma50 and ret20 < -1.0:
            direction = 'bearish'
        else:
            direction = 'sideways'
        return {
            'index': ticker,
            'direction': direction,
            'last': round(last, 2),
            'ma20': round(ma20, 2),
            'ma50': round(ma50, 2),
            'ma200': round(ma200, 2),
            'return_20d_pct': round(ret20, 2),
            'return_63d_pct': round(ret63, 2),
            'plain': f'Market trend is {direction}: index vs MA50 and recent 20-day return are checked before allowing new trades.'
        }
    except Exception as e:
        return {'index': ticker, 'direction': 'unknown', 'note': str(e)[:160]}


def get_market_context(market):
    vix_ticker = VIX_TICKERS.get(market, '^VIX')
    base: Dict[str, Any] = {}
    try:
        hist = yf.Ticker(vix_ticker).history(period='5d', timeout=YF_TIMEOUT)
        if hist is not None and not hist.empty:
            level = round(float(hist['Close'].iloc[-1]), 2)
            prev = round(float(hist['Close'].iloc[-2]), 2) if len(hist) > 1 else level
            chg_pct = round(((level - prev) / prev) * 100, 2) if prev else 0
            if level < 13:
                regime = 'low volatility / complacent'
            elif level < 20:
                regime = 'normal volatility'
            elif level < 30:
                regime = 'elevated volatility / cautious'
            else:
                regime = 'high volatility / risk-off'
            base = {'index': vix_ticker, 'level': level, 'change_pct': chg_pct, 'regime': regime}
    except Exception as e:
        print(f"[WARN] VIX fetch failed for {market}: {e}")
    base['market_trend'] = get_market_trend_context(market)
    trend = (base.get('market_trend') or {}).get('direction')
    if trend == 'bearish' and not str(base.get('regime') or '').lower().startswith('high'):
        base['trend_caution'] = True
        base['regime'] = base.get('regime') or 'bearish market / caution'
    return merge_macro_into_market_context(base, market)


def parse_rr(rr_text: str) -> Optional[float]:
    if not rr_text:
        return None
    try:
        return float(str(rr_text).split(':')[0])
    except Exception:
        return None


def make_tags(candidate: Dict[str, Any]) -> List[str]:
    tags = []
    if candidate.get('momentum_score', 0) > 15:
        tags.append('Momentum')
    if candidate.get('volume_ratio_vs_20d_avg', 1) >= 1.4:
        tags.append('Volume Surge')
    if candidate.get('macd_histogram') is not None and candidate['macd_histogram'] > 0:
        tags.append('MACD Signal')
    if candidate.get('price_vs_ma20_pct', 0) and candidate.get('price_vs_ma50_pct', 0) and candidate['price_vs_ma20_pct'] > 0 and candidate['price_vs_ma50_pct'] > 0:
        tags.append('Trend Following')
    if candidate.get('bb_position_pct') is not None and candidate['bb_position_pct'] < 25:
        tags.append('Oversold Bounce')
    if candidate.get('news_score', 0) > 0:
        tags.append('News Catalyst')
    if candidate.get('news_score', 0) < 0:
        tags.append('Negative News Risk')
    return tags[:3] or ['Technical Setup', 'Risk Managed']


def deterministic_rank_candidates(candidates: List[Dict[str, Any]], market: str, horizon: str) -> List[Dict[str, Any]]:
    picks = []
    horizon_mult = 1.0 if horizon == 'day' else 1.8
    for i, c in enumerate(sorted(candidates, key=lambda x: x.get('final_pre_ai_score', 0), reverse=True)[:FINAL_PICK_LIMIT], start=1):
        current = float(c['current_price'])
        score = float(c.get('final_pre_ai_score', 0))
        predicted_gain = round(max(min((score / 20) * horizon_mult, 8.0 if horizon == 'day' else 14.0), 1.2), 1)
        support = float(c.get('support_20') or current * 0.97)
        resistance = float(c.get('resistance_20') or current * 1.04)
        stop_pct = round(max(min(((current - support) / current) * 100, 5.5), 1.5), 1)
        target_pct = round(max(predicted_gain, ((resistance - current) / current) * 100), 1)
        target_price = round(current * (1 + target_pct / 100), 2)
        stop_price = round(current * (1 - stop_pct / 100), 2)
        rr = round(target_pct / stop_pct, 1) if stop_pct else 0
        confidence = int(max(min(50 + score * 0.45 + c.get('news_score', 0) * 0.25, 92), 35))
        signal = 'BUY' if confidence >= 58 and rr >= 1.2 else 'WATCH'
        picks.append({
            'rank': i,
            'ticker': c['ticker'],
            'company': c.get('company', c['ticker']),
            'sector': c.get('sector', 'N/A'),
            'current_price': current,
            'signal': signal,
            'predicted_gain': f"+{target_pct}%",
            'gain_number': target_pct,
            'entry_price': current,
            'target_exit_pct': f"+{target_pct}%",
            'stop_loss_pct': f"-{stop_pct}%",
            'target_price': target_price,
            'stop_price': stop_price,
            'risk_reward': f"{rr}:1",
            'confidence': confidence,
            'rsi_reading': f"{c.get('rsi_14')} — {'bullish momentum zone' if c.get('rsi_14') and 50 <= c.get('rsi_14') <= 65 else 'watch for confirmation'}",
            'macd_reading': f"{c.get('macd_crossover', 'neutral')} crossover; histogram {c.get('macd_histogram')}",
            'volume_reading': f"{c.get('volume_ratio_vs_20d_avg')}x 20-day average volume",
            'fundamental_reading': format_fundamental_reading(c.get('fundamentals', {})),
            'news_reading': c.get('news_reading', 'No material recent headlines found.'),
            'horizon_tags': make_tags(c),
            'reasoning': (
                f"Ranked through liquidity, momentum, technical, fundamental and news filters. "
                f"RSI {c.get('rsi_14')}, MACD histogram {c.get('macd_histogram')}, price vs MA20 {c.get('price_vs_ma20_pct')}%, "
                f"and 20-day volume ratio {c.get('volume_ratio_vs_20d_avg')}x support the setup; support is near {c.get('support_20')} and resistance near {c.get('resistance_20')}."
            ),
        })
    return picks


def format_fundamental_reading(f: Dict[str, Any]) -> str:
    bits = []
    if f.get('revenue_growth_pct') is not None:
        bits.append(f"revenue growth {f['revenue_growth_pct']}%")
    if f.get('roe_pct') is not None:
        bits.append(f"ROE {f['roe_pct']}%")
    if f.get('debt_to_equity') is not None:
        bits.append(f"debt/equity {f['debt_to_equity']}")
    if f.get('free_cashflow_bn') is not None:
        bits.append(f"FCF {f['free_cashflow_bn']}B")
    return ', '.join(bits[:4]) if bits else 'Fundamental data unavailable or limited; ranking relies more on technicals/news.'


def log_api_cost(call_type: str, market: Optional[str], horizon: Optional[str], model: str,
                 input_tokens: int = 0, output_tokens: int = 0, notes: str = '', metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    cost = calculate_claude_cost(input_tokens, output_tokens)
    conn = get_db_connection()
    if conn is not None:
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO api_cost_log
                            (call_type, market, horizon, model, input_tokens, output_tokens, cost_usd, cost_inr, notes, metadata_json)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                    """, (
                        call_type, market, horizon, model, int(input_tokens or 0), int(output_tokens or 0),
                        cost['cost_usd'], cost['cost_inr'], notes, json.dumps(metadata or {}, default=_json_safe)
                    ))
        except Exception as e:
            print(f"[WARN] Could not log API cost: {e}")
        finally:
            conn.close()
    return cost


def estimate_next_scan_cost(market: str, horizon: str) -> Dict[str, Any]:
    market, horizon = validate_market_horizon(market, horizon) if 'validate_market_horizon' in globals() else (market, horizon)
    universe_size = len(load_market_universe(market))
    estimated_deep = min(DEEP_ANALYSIS_LIMIT, TECHNICAL_STAGE_LIMIT, max(universe_size, 1))
    input_tokens = SCAN_ESTIMATED_INPUT_TOKENS_BASE + estimated_deep * SCAN_ESTIMATED_INPUT_TOKENS_PER_CANDIDATE
    output_tokens = SCAN_ESTIMATED_OUTPUT_TOKENS
    claude = calculate_claude_cost(input_tokens, output_tokens)
    budget = get_ai_budget_status(claude.get('cost_inr', 0))
    locked = get_locked_cached_scan(market, horizon) if DATABASE_URL else None
    yfinance_calls = math.ceil(max(universe_size, 1) / max(BATCH_SIZE, 1)) + estimated_deep * 2 + 1
    return {
        'market': market,
        'horizon': horizon,
        'locked_prediction_exists': bool(locked),
        'paid_cost_if_user_clicks_load_cached_scan_inr': 0,
        'paid_cost_if_locked_prediction_exists_inr': 0,
        'estimated_paid_cost_if_first_scan_runs': claude,
        'ai_budget': budget,
        'paid_ai_will_run_if_first_scan_needed': bool(not locked and budget.get('allowed')),
        'paid_ai_fallback_if_blocked': 'deterministic_ranker',
        'estimated_input_tokens': input_tokens,
        'estimated_output_tokens': output_tokens,
        'estimated_yfinance_fetches': yfinance_calls,
        'yfinance_cost': 0,
        'note': 'Load Cached Scan costs ₹0. Run First Scan costs ₹0 if a current locked prediction already exists. If a new lock is needed, Claude runs only when AI budget allows; otherwise deterministic ranking is used.',
    }


def get_learning_context(market: str, horizon: str, limit: int = MAX_LEARNING_LESSONS) -> Dict[str, Any]:
    conn = get_db_connection()
    if conn is None:
        return {'available': False, 'lessons': [], 'patterns': {}}
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT signal, outcome_label, outcome_status, failure_reason, lesson_summary, actual_gain_pct, features_json, created_at
                FROM prediction_lessons
                WHERE market=%s AND horizon=%s AND created_at >= NOW() - (%s || ' days')::interval
                ORDER BY created_at DESC
                LIMIT %s
            """, (market, horizon, LESSON_LOOKBACK_DAYS, limit))
            rows = cur.fetchall()
    except Exception as e:
        print(f"[WARN] Could not load learning context: {e}")
        rows = []
    finally:
        conn.close()

    patterns: Dict[str, int] = {}
    combo_stats: Dict[str, Dict[str, Any]] = {}
    lessons = []
    for r in rows:
        reason = r.get('failure_reason') or r.get('outcome_status') or 'general'
        features = r.get('features_json') or {}
        if isinstance(features, str):
            try:
                features = json.loads(features)
            except Exception:
                features = {}
        combo = backtest_pattern_key(features)
        bucket = combo_stats.setdefault(combo, {'total': 0, 'failed': 0, 'success': 0, 'reasons': {}})
        bucket['total'] += 1
        if r.get('outcome_status') in ('failed', 'missed_opportunity') or r.get('failure_reason'):
            bucket['failed'] += 1
            bucket['reasons'][reason] = bucket['reasons'].get(reason, 0) + 1
        elif r.get('outcome_status') == 'success':
            bucket['success'] += 1
        patterns[reason] = patterns.get(reason, 0) + 1
        lessons.append({
            'signal': r.get('signal'),
            'outcome_label': r.get('outcome_label'),
            'failure_reason': reason,
            'pattern_key': combo,
            'lesson_summary': r.get('lesson_summary'),
            'actual_gain_pct': float(r['actual_gain_pct']) if r.get('actual_gain_pct') is not None else None,
            'features': features,
        })
    pattern_memory = {}
    for combo, stat in combo_stats.items():
        total = stat.get('total') or 0
        if total >= LEARNING_PATTERN_MIN_TRADES:
            fail_rate = (stat.get('failed', 0) / total) * 100
            top_reason = max(stat.get('reasons', {}).items(), key=lambda kv: kv[1])[0] if stat.get('reasons') else None
            pattern_memory[combo] = {'total': total, 'failed': stat.get('failed', 0), 'success': stat.get('success', 0), 'failure_rate_pct': round(fail_rate, 1), 'top_reason': top_reason}
    return {'available': True, 'lessons': lessons[:20], 'patterns': patterns, 'pattern_memory': pattern_memory, 'lookback_days': LESSON_LOOKBACK_DAYS}


def learning_penalty_for_candidate(candidate: Dict[str, Any], learning_context: Dict[str, Any]) -> float:
    patterns = learning_context.get('patterns') or {}
    if not patterns:
        return 0.0
    penalty = 0.0
    vol = candidate.get('volume_ratio_vs_20d_avg') or 1.0
    rsi = candidate.get('rsi_14') or 50
    news_score = candidate.get('news_score', 0)
    rr_proxy = 0.0
    try:
        current = float(candidate.get('current_price') or 0)
        support = float(candidate.get('support_20') or current * 0.97)
        resistance = float(candidate.get('resistance_20') or current * 1.04)
        rr_proxy = ((resistance - current) / current) / max(((current - support) / current), 0.01)
    except Exception:
        rr_proxy = 1.0

    if patterns.get('low_volume_confirmation', 0) and vol < 0.8:
        penalty += min(6, patterns['low_volume_confirmation'] * 1.5)
    if patterns.get('overextended_or_chasing', 0) and rsi > 68:
        penalty += min(6, patterns['overextended_or_chasing'] * 1.5)
    if patterns.get('weak_risk_reward', 0) and rr_proxy < MIN_BUY_RISK_REWARD:
        penalty += min(5, patterns['weak_risk_reward'] * 1.2)
    if patterns.get('no_news_catalyst', 0) and news_score <= 0:
        penalty += min(4, patterns['no_news_catalyst'] * 1.0)
    if patterns.get('negative_news_or_event_risk', 0) and news_score < 0:
        penalty += min(6, patterns['negative_news_or_event_risk'] * 1.5)
    pattern_memory = learning_context.get('pattern_memory') or {}
    candidate_key = backtest_pattern_key({**candidate, 'rr_proxy': rr_proxy})
    candidate_pattern = pattern_memory.get(candidate_key)
    if candidate_pattern and candidate_pattern.get('failure_rate_pct', 0) >= 60:
        penalty += min(8, 2 + candidate_pattern.get('failed', 0) * 1.5)
        candidate['prediction_lesson_pattern_warning'] = f"Repeated failed pattern: {candidate_key}; top reason: {candidate_pattern.get('top_reason') or 'mixed'}"
    return round(penalty, 2)


def apply_learning_adjustments(candidates: List[Dict[str, Any]], learning_context: Dict[str, Any]) -> List[Dict[str, Any]]:
    for c in candidates:
        penalty = learning_penalty_for_candidate(c, learning_context)
        c['learning_penalty'] = penalty
        c['learning_patterns_considered'] = learning_context.get('patterns') or {}
        if penalty:
            c['final_pre_ai_score'] = float(c.get('final_pre_ai_score', 0)) - penalty
    return sorted(candidates, key=lambda x: x.get('final_pre_ai_score', 0), reverse=True)





def normalize_history_frame(hist):
    """Return a compact OHLCV dataframe with single-level columns when yfinance returns odd shapes."""
    try:
        if hist is None or getattr(hist, 'empty', True):
            return hist
        frame = hist.copy()
        # yfinance can occasionally return MultiIndex columns or duplicate columns.
        cols = getattr(frame, 'columns', None)
        if cols is not None and hasattr(cols, 'nlevels') and getattr(cols, 'nlevels', 1) > 1:
            # Prefer the OHLCV field level if present; otherwise flatten to the first matching label.
            wanted = {'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'}
            new_cols = []
            for col in cols:
                picked = None
                for part in col if isinstance(col, tuple) else (col,):
                    if str(part) in wanted:
                        picked = str(part)
                        break
                new_cols.append(picked or str(col[-1] if isinstance(col, tuple) else col))
            frame.columns = new_cols
        # Remove duplicate OHLCV columns if present; keep first.
        try:
            frame = frame.loc[:, ~frame.columns.duplicated()]
        except Exception:
            pass
        return frame
    except Exception:
        return hist


def history_series(hist, col: str, fallback_col: Optional[str] = None):
    """Safely extract a numeric Series from a yfinance history frame.

    This avoids Render/Gunicorn crashes caused by odd pandas objects, duplicate columns,
    or MultiIndex outputs during long backtests.
    """
    try:
        frame = normalize_history_frame(hist)
        if frame is None or getattr(frame, 'empty', True):
            return None
        if col in frame:
            series = frame[col]
        elif fallback_col and fallback_col in frame:
            series = frame[fallback_col]
        elif 'Close' in frame:
            series = frame['Close']
        else:
            return None
        # Duplicate columns can make frame[col] a DataFrame; use the first column.
        if hasattr(series, 'columns'):
            series = series.iloc[:, 0]
        series = series.dropna().astype(float)
        return series
    except BaseException:
        # BaseException intentionally catches worker-timeout SystemExit from nested pandas ops
        # while keeping the backtest from killing the whole web worker.
        return None

def classify_regime_from_history(hist) -> Dict[str, Any]:
    """Classify market regime from information available up to the current bar."""
    unknown = {'regime': 'unknown', 'trend': 'unknown', 'volatility': 'unknown', 'realized_vol_pct': None, 'adx_proxy': None}
    try:
        if hist is None or getattr(hist, 'empty', True) or len(hist) < 50:
            return unknown
        frame = normalize_history_frame(hist)
        closes = history_series(frame, 'Close')
        if closes is None or len(closes) < 50:
            return unknown
        highs = history_series(frame, 'High', fallback_col='Close')
        lows = history_series(frame, 'Low', fallback_col='Close')
        if highs is None or len(highs) == 0:
            highs = closes
        if lows is None or len(lows) == 0:
            lows = closes
        ma20 = float(closes.tail(20).mean())
        ma50 = float(closes.tail(50).mean())
        base_20 = float(closes.iloc[-20]) if len(closes) >= 20 else 0.0
        ret20 = ((float(closes.iloc[-1]) / base_20) - 1) * 100 if base_20 else 0.0
        returns = closes.pct_change().dropna().tail(20)
        realized_vol = float(returns.std() * (252 ** 0.5) * 100) if not returns.empty else 0.0
        high_tail = highs.tail(14)
        low_tail = lows.tail(14)
        recent_range = (float(high_tail.max()) - float(low_tail.min())) / max(float(closes.iloc[-1]), 0.01) * 100
        adx_proxy = abs(ret20) / max(float(recent_range), 0.01) * 25
        if ma20 > ma50 and ret20 > 2.5:
            trend = 'uptrend'
        elif ma20 < ma50 and ret20 < -2.5:
            trend = 'downtrend'
        else:
            trend = 'sideways'
        if realized_vol >= 35:
            volatility = 'high_vol'
        elif realized_vol <= 16:
            volatility = 'low_vol'
        else:
            volatility = 'normal_vol'
        if trend == 'uptrend' and volatility != 'high_vol':
            regime = 'trending_bull'
        elif trend == 'downtrend' or volatility == 'high_vol':
            regime = 'risk_off_or_high_vol'
        else:
            regime = 'choppy_sideways'
        return {'regime': regime, 'trend': trend, 'volatility': volatility, 'realized_vol_pct': round(realized_vol, 2), 'adx_proxy': round(float(adx_proxy), 2)}
    except BaseException as e:
        return {**unknown, 'error': str(e)[:180]}


def compute_atr_pct_from_history(hist, period: int = 14) -> Optional[float]:
    try:
        if hist is None or getattr(hist, 'empty', True) or len(hist) < period + 1:
            return None
        frame = normalize_history_frame(hist)
        high_s = history_series(frame, 'High', fallback_col='Close')
        low_s = history_series(frame, 'Low', fallback_col='Close')
        close_s = history_series(frame, 'Close')
        if high_s is None or low_s is None or close_s is None:
            return None
        n = min(len(high_s), len(low_s), len(close_s))
        if n < period + 1:
            return None
        high = high_s.tail(n).values
        low = low_s.tail(n).values
        close = close_s.tail(n).values
        trs = []
        for i in range(1, len(close)):
            trs.append(max(high[i] - low[i], abs(high[i] - close[i-1]), abs(low[i] - close[i-1])))
        atr = float(np.mean(trs[-period:]))
        current = float(close[-1])
        return round((atr / current) * 100, 2) if current else None
    except BaseException:
        return None


def backtest_pattern_key(features: Dict[str, Any]) -> str:
    parts = []
    rsi = features.get('rsi_14')
    vol = features.get('volume_ratio_vs_20d_avg')
    rr = features.get('rr_proxy')
    if rsi is not None:
        parts.append('rsi_high' if rsi >= 68 else 'rsi_bull' if rsi >= 50 else 'rsi_weak')
    if vol is not None:
        parts.append('vol_strong' if vol >= 1.3 else 'vol_low' if vol < 0.8 else 'vol_normal')
    if rr is not None:
        parts.append('rr_good' if rr >= 1.6 else 'rr_weak')
    parts.append('macd_pos' if (features.get('macd_histogram') or 0) > 0 else 'macd_neg')
    return '+'.join(parts[:5]) or 'general'




def pattern_verdict_from_stats(trades: int, win_rate: Optional[float], expectancy: Optional[float]) -> str:
    """Turn historical evidence into an actionable memory label."""
    if trades < LEARNING_PATTERN_MIN_TRADES or win_rate is None or expectancy is None:
        return 'insufficient_evidence'
    if win_rate <= LEARNING_BAD_PATTERN_WIN_RATE_PCT or expectancy < 0:
        return 'avoid_or_require_stronger_confirmation'
    if win_rate >= LEARNING_GOOD_PATTERN_WIN_RATE_PCT and expectancy > 0:
        return 'favorable_pattern'
    return 'neutral_pattern'


def explain_backtest_failure(trade: Dict[str, Any]) -> Dict[str, Any]:
    """Explain why a simulated trade failed using only stored historical features."""
    ret = float(trade.get('net_return_pct') or 0)
    outcome = trade.get('outcome') or 'unknown'
    features = trade.get('features') or {}
    reasons = []
    if ret > 0:
        return {
            'failure_reason': None,
            'pattern_verdict': 'worked',
            'lesson_summary': f"{trade.get('ticker')} pattern worked historically in {trade.get('regime') or 'unknown'} regime; similar setups can receive normal or positive weighting."
        }
    if outcome == 'stop_loss_hit':
        reasons.append('stop_loss_hit')
    if float(features.get('volume_ratio_vs_20d_avg') or 1) < 0.8:
        reasons.append('low_volume_confirmation')
    if float(features.get('rsi_14') or 50) >= 68:
        reasons.append('overextended_or_chasing')
    if float(features.get('rr_proxy') or 1.5) < MIN_BUY_RISK_REWARD:
        reasons.append('weak_risk_reward')
    if float(features.get('price_vs_ma20_pct') or 0) < 0:
        reasons.append('below_short_term_trend')
    if float(features.get('macd_histogram') or 0) <= 0:
        reasons.append('weak_or_negative_macd')
    if not reasons:
        reasons.append('setup_did_not_follow_through')
    reason = '+'.join(reasons[:4])
    ticker = trade.get('ticker')
    regime = trade.get('regime') or 'unknown'
    return {
        'failure_reason': reason,
        'pattern_verdict': 'failed',
        'lesson_summary': f"{ticker} lost {round(ret, 2)}% in backtest; likely issue: {reason.replace('_', ' ')} during {regime}. Future scans should reduce confidence or require stronger volume/RR confirmation for this pattern."
    }

def get_strategy_memory(market: str, horizon: str, regime: Optional[str] = None, limit: int = 40) -> Dict[str, Any]:
    conn = get_db_connection()
    if conn is None:
        return {'available': False, 'patterns': []}
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if regime:
                cur.execute("""
                    SELECT regime, pattern_key, trades, win_rate_pct, expectancy_pct, confidence_adjustment, notes
                    FROM strategy_signal_stats
                    WHERE market=%s AND horizon=%s AND (regime=%s OR regime='all')
                    ORDER BY trades DESC, updated_at DESC
                    LIMIT %s
                """, (market, horizon, regime, limit))
            else:
                cur.execute("""
                    SELECT regime, pattern_key, trades, win_rate_pct, expectancy_pct, confidence_adjustment, notes
                    FROM strategy_signal_stats
                    WHERE market=%s AND horizon=%s
                    ORDER BY trades DESC, updated_at DESC
                    LIMIT %s
                """, (market, horizon, limit))
            rows = cur.fetchall()
    except Exception as e:
        print(f"[WARN] Strategy memory load failed: {e}")
        rows = []
    finally:
        conn.close()
    return {'available': True, 'patterns': [dict(r) for r in rows]}


def strategy_memory_adjustment(candidate: Dict[str, Any], memory: Dict[str, Any], regime: Optional[str] = None) -> float:
    if not memory.get('patterns'):
        return 0.0
    key = backtest_pattern_key(candidate)
    adjustment = 0.0
    for row in memory.get('patterns', []):
        if row.get('pattern_key') == key and (not regime or row.get('regime') in (regime, 'all')):
            adjustment += float(row.get('confidence_adjustment') or 0)
    return round(adjustment, 2)


def apply_strategy_memory_adjustments(candidates: List[Dict[str, Any]], market: str, horizon: str, market_context: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    regime = market_context.get('regime') if isinstance(market_context, dict) else None
    memory = get_strategy_memory(market, horizon, regime=regime)
    for c in candidates:
        try:
            current = float(c.get('current_price') or 0)
            support = float(c.get('support_20') or current * 0.97)
            resistance = float(c.get('resistance_20') or current * 1.04)
            c['rr_proxy'] = round(((resistance-current)/max(current,0.01)) / max(((current-support)/max(current,0.01)), 0.01), 2)
        except Exception:
            c['rr_proxy'] = None
        adj = strategy_memory_adjustment(c, memory, regime=regime)
        c['strategy_memory_adjustment'] = adj
        c['strategy_pattern_key'] = backtest_pattern_key(c)
        if adj:
            c['final_pre_ai_score'] = float(c.get('final_pre_ai_score', 0)) + adj
    return sorted(candidates, key=lambda x: x.get('final_pre_ai_score', 0), reverse=True)




def _num(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None:
            return default
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, (int, float, np.generic)):
            return float(value)
        text = str(value).replace('%', '').replace('₹', '').replace('$', '').strip()
        if ':' in text:
            text = text.split(':')[0]
        return float(text)
    except Exception:
        return default


def get_stock_backtest_strength(market: str, horizon: str, tickers: Optional[List[str]] = None) -> Dict[str, Dict[str, Any]]:
    """Per-stock daily/weekly backtest memory used by the main prediction engine."""
    conn = get_db_connection()
    if conn is None:
        return {}
    params: List[Any] = [market, horizon]
    ticker_clause = ''
    if tickers:
        ticker_clause = ' AND ticker = ANY(%s)'
        params.append([str(t).upper() for t in tickers])
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"""
                SELECT ticker,
                       COUNT(*) AS trades,
                       AVG(CASE WHEN net_return_pct > 0 THEN 1 ELSE 0 END) * 100 AS win_rate_pct,
                       AVG(net_return_pct) AS expectancy_pct,
                       SUM(CASE WHEN outcome='target_hit' THEN 1 ELSE 0 END) AS target_hits,
                       SUM(CASE WHEN outcome='stop_loss_hit' THEN 1 ELSE 0 END) AS stop_hits,
                       MIN(signal_date) AS first_signal_date,
                       MAX(signal_date) AS last_signal_date
                FROM backtest_trades
                WHERE market=%s AND horizon=%s {ticker_clause}
                GROUP BY ticker
            """, params)
            return {str(r['ticker']).upper(): {k: (float(v) if hasattr(v, 'as_tuple') else v) for k, v in dict(r).items()} for r in cur.fetchall()}
    except Exception as e:
        print(f"[WARN] Could not load stock backtest strength: {e}")
        return {}
    finally:
        conn.close()


def get_closed_trade_history_strength(market: str, horizon: Optional[str] = None, engine: str = 'trading') -> Dict[str, Dict[str, Any]]:
    """Closed/paper/live trade memory used by trading automation or intraday."""
    conn = get_db_connection()
    if conn is None:
        return {}
    filters = ["status='CLOSED'", "market=%s", "pnl_pct IS NOT NULL"]
    params: List[Any] = [market]
    if engine == 'intraday':
        filters.append("COALESCE(engine, CASE WHEN horizon='intraday' THEN 'intraday' ELSE 'trading_automation' END)='intraday'")
    else:
        filters.append("COALESCE(engine, CASE WHEN horizon='intraday' THEN 'intraday' ELSE 'trading_automation' END) <> 'intraday'")
        if horizon:
            filters.append("horizon=%s")
            params.append(horizon)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"""
                SELECT ticker,
                       COUNT(*) AS trades,
                       AVG(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) * 100 AS win_rate_pct,
                       AVG(pnl_pct) AS expectancy_pct,
                       AVG(CASE WHEN exit_reason LIKE 'TARGET%%' OR exit_reason LIKE '%%PROFIT%%' THEN 1 ELSE 0 END) * 100 AS positive_exit_rate_pct,
                       MAX(closed_at) AS last_closed_at
                FROM trade_positions
                WHERE {' AND '.join(filters)}
                GROUP BY ticker
            """, params)
            return {str(r['ticker']).upper(): {k: (float(v) if hasattr(v, 'as_tuple') else v) for k, v in dict(r).items()} for r in cur.fetchall()}
    except Exception as e:
        print(f"[WARN] Could not load {engine} trade history strength: {e}")
        return {}
    finally:
        conn.close()


def strict_learning_architecture() -> Dict[str, Any]:
    return {
        'enabled': STRICT_LEARNING_ENABLED,
        'plain_summary': 'Daily backtests improve main predictions. Closed trade history improves trading automation. Intraday history improves the intraday engine. Weak setups are downgraded or skipped instead of forcing trades.',
        'paths': [
            {'source': 'Research Lab daily backtest', 'improves': 'Main prediction engine', 'uses': 'daily/weekly RSI, MACD, volume, reward/risk, regime, per-stock history'},
            {'source': 'Trade History', 'improves': 'Trading automation', 'uses': 'actual or paper entries, exits, target hits, stop hits, P/L, repeated stock failures'},
            {'source': 'Intraday History', 'improves': 'Intraday engine', 'uses': '5-minute breakout quality, VWAP hold/fail, volume spike, quick target/stop result, time-of-day behavior'},
        ],
        'strict_rules': [
            'Minimum score filter',
            'Minimum reward/risk filter',
            'Volume confirmation filter',
            'Market regime filter',
            'Pattern reliability filter',
            'Bad-pattern penalty',
            'Per-stock historical strength filter',
            'Training vs validation split',
            'Benchmark comparison',
            'No-trade recommendation when edge is weak',
        ],
        'thresholds': {
            'min_score': STRICT_MIN_SCORE,
            'buy_min_score': STRICT_BUY_MIN_SCORE,
            'backtest_min_score': STRICT_BACKTEST_MIN_SCORE,
            'min_risk_reward': STRICT_MIN_RISK_REWARD,
            'min_volume_ratio': STRICT_MIN_VOLUME_RATIO,
            'backtest_min_risk_reward': STRICT_BACKTEST_MIN_RISK_REWARD,
            'backtest_min_volume_ratio': STRICT_BACKTEST_MIN_VOLUME_RATIO,
            'backtest_allow_watch_only': STRICT_BACKTEST_ALLOW_WATCH_ONLY,
            'pattern_reliable_min_trades': STRICT_PATTERN_RELIABLE_MIN_TRADES,
            'per_stock_min_trades': STRICT_PER_STOCK_MIN_TRADES,
        }
    }


def strict_learning_review(candidate: Dict[str, Any], market: str, horizon: str, engine: str = 'prediction',
                           market_context: Optional[Dict[str, Any]] = None,
                           stock_memory: Optional[Dict[str, Dict[str, Any]]] = None,
                           trade_memory: Optional[Dict[str, Dict[str, Any]]] = None) -> Dict[str, Any]:
    """One conservative reviewer used by all engines with engine-specific memory.

    It does not predict prices. It decides whether the edge is strong enough to
    allow a BUY/entry, or whether the system should WATCH/AVOID/no-trade.
    """
    if not STRICT_LEARNING_ENABLED:
        return {'enabled': False, 'allowed': True, 'score_adjustment': 0.0, 'rules': []}
    ticker = str(candidate.get('ticker') or '').upper()
    score = _num(candidate.get('final_pre_ai_score'), _num(candidate.get('score'), _num(candidate.get('confidence'), 0.0))) or 0.0
    if engine == 'backtest':
        min_score_needed = STRICT_BACKTEST_MIN_SCORE
    elif engine == 'intraday':
        # Use the Intraday tab's saved Minimum score, not the global env default.
        # This fixes the UI mismatch where the screen showed 60 but strict learning
        # still blocked candidates with "needs 70+".
        min_score_needed = _num(candidate.get('min_score'), INTRADAY_MIN_SCORE) or INTRADAY_MIN_SCORE
    elif engine == 'trading_automation':
        min_score_needed = _num(candidate.get('min_score'), STRICT_MIN_SCORE) or STRICT_MIN_SCORE
    else:
        min_score_needed = STRICT_MIN_SCORE
    rr = _num(candidate.get('rr_proxy'), None)
    if rr is None:
        rr = _num(candidate.get('risk_reward'), None)
    if rr is None:
        try:
            entry = _num(candidate.get('entry_price'), _num(candidate.get('current_price'), _num(candidate.get('last'), 0))) or 0
            target = _num(candidate.get('target_price'), 0) or 0
            stop = _num(candidate.get('stop_price'), 0) or 0
            rr = ((target-entry)/max(entry,0.01)) / max(((entry-stop)/max(entry,0.01)), 0.01) if entry and target > entry and stop < entry else 0.0
        except Exception:
            rr = 0.0
    vol = _num(candidate.get('volume_ratio_vs_20d_avg'), _num(candidate.get('volume_multiplier'), 1.0))
    regime = None
    if isinstance(market_context, dict):
        regime = market_context.get('regime')
    if not regime:
        regime = candidate.get('regime') or (candidate.get('regime_info') or {}).get('regime')
    pattern_key = candidate.get('strategy_pattern_key') or candidate.get('pattern_key') or backtest_pattern_key({**candidate, 'rr_proxy': rr})
    raw_adj = _num(candidate.get('strategy_memory_adjustment'), 0.0) or 0.0
    adjustment = raw_adj
    hard_blocks: List[str] = []
    cautions: List[str] = []
    boosts: List[str] = []
    rules: List[Dict[str, Any]] = []

    def add_rule(name: str, passed: bool, action: str, detail: str):
        rules.append({'name': name, 'passed': bool(passed), 'action': action, 'detail': detail})
        if not passed and action in ('block', 'avoid'):
            hard_blocks.append(detail)
        elif not passed:
            cautions.append(detail)
        elif action == 'boost':
            boosts.append(detail)

    add_rule('Minimum setup score', score >= min_score_needed, 'block' if engine in ('trading_automation','intraday') else 'downgrade', f"score {round(score,1)}; needs {min_score_needed}+")
    if str(candidate.get('signal') or '').upper() == 'BUY':
        add_rule('Minimum BUY score', score >= STRICT_BUY_MIN_SCORE, 'downgrade', f"BUY score {round(score,1)}; needs {STRICT_BUY_MIN_SCORE}+")
    if engine == 'backtest':
        rr_threshold = STRICT_BACKTEST_MIN_RISK_REWARD
        vol_threshold = STRICT_BACKTEST_MIN_VOLUME_RATIO
    elif engine == 'intraday':
        rr_threshold = _num(candidate.get('min_risk_reward'), INTRADAY_MIN_RISK_REWARD) or INTRADAY_MIN_RISK_REWARD
        vol_threshold = _num(candidate.get('min_volume_multiplier'), INTRADAY_MIN_VOLUME_MULTIPLIER) or INTRADAY_MIN_VOLUME_MULTIPLIER
    else:
        rr_threshold = STRICT_MIN_RISK_REWARD
        vol_threshold = STRICT_MIN_VOLUME_RATIO
    add_rule('Minimum reward vs risk', rr >= rr_threshold, 'block' if engine in ('trading_automation','intraday') else 'downgrade', f"reward/risk {round(rr or 0,2)}; needs {rr_threshold}+")
    volume_unavailable = bool(candidate.get('volume_data_reliable') is False or candidate.get('volume_data_available') is False)
    if engine == 'intraday' and volume_unavailable:
        rules.append({'name': 'Volume confirmation', 'passed': True, 'action': 'neutral', 'detail': 'provider volume missing/zero; not blocking intraday setup on free data'})
    else:
        vol_action = 'block' if (engine == 'intraday' and INTRADAY_VOLUME_CONFIRMATION_MODE == 'hard') else 'downgrade'
        add_rule('Volume confirmation', (vol is None or vol >= vol_threshold), vol_action, f"volume {round(vol or 0,2)}x; needs {vol_threshold}x+")
    risk_off = bool(regime and ('risk_off' in str(regime) or 'high volatility' in str(regime).lower() or 'risk-off' in str(regime).lower()))
    if engine == 'intraday':
        # Macro/regime should warn or reduce score by default in intraday paper mode,
        # not silently block every otherwise-valid setup unless explicitly enabled.
        regime_action = 'block' if STRICT_INTRADAY_BLOCK_RISK_OFF else 'downgrade'
    else:
        regime_action = 'block' if (engine == 'trading_automation' or (engine == 'backtest' and STRICT_BACKTEST_REJECT_RISK_OFF)) else 'downgrade'
    add_rule('Market regime safety', not (STRICT_AVOID_RISK_OFF and risk_off), regime_action, f"market type is {regime or 'unknown'}")
    trend_direction = ((market_context or {}).get('market_trend') or {}).get('direction') if isinstance(market_context, dict) else None
    if trend_direction == 'bearish':
        adjustment -= 2.5
        add_rule('Bullish/bearish market direction', False, 'downgrade', 'broad market trend is bearish; require stronger confirmation or no trade')
    elif trend_direction == 'bullish':
        boosts.append('broad market trend is bullish')
        add_rule('Bullish/bearish market direction', True, 'boost', 'broad market trend is bullish')
    else:
        add_rule('Bullish/bearish market direction', True, 'neutral', f'broad market trend is {trend_direction or "unknown"}')

    macro = (market_context or {}).get('macro_event_risk') if isinstance(market_context, dict) else None
    if isinstance(macro, dict) and macro.get('enabled'):
        macro_level = macro.get('risk_level')
        macro_score = _num(macro.get('risk_score'), 0.0) or 0.0
        if macro_level == 'high_risk' or macro_score >= MACRO_RISK_BLOCK_THRESHOLD:
            if engine == 'intraday':
                action = 'block' if STRICT_INTRADAY_BLOCK_MACRO_RISK else 'downgrade'
            else:
                action = 'block' if engine == 'trading_automation' else 'downgrade'
            adjustment -= 6.0
            add_rule('Major news/macro risk', False, action, f"macro/news risk high ({round(macro_score,1)}): {macro.get('decision') or 'avoid new trades'}")
        elif macro_level == 'caution' or macro_score >= MACRO_RISK_CAUTION_THRESHOLD:
            adjustment -= 2.0
            add_rule('Major news/macro risk', False, 'downgrade', f"macro/news caution ({round(macro_score,1)}): require stronger confirmation")
        else:
            add_rule('Major news/macro risk', True, 'neutral', 'no major macro/news block detected')

    if raw_adj <= STRICT_BAD_PATTERN_MIN_PENALTY:
        adjustment += raw_adj  # make bad historical pattern count harder in strict mode
        add_rule('Bad-pattern penalty', False, 'downgrade', f"similar pattern has negative memory ({round(raw_adj,2)})")
    elif raw_adj > 0:
        boosts.append(f"similar pattern has positive memory (+{round(raw_adj,2)})")
        add_rule('Pattern reliability', True, 'boost', f"pattern memory adjustment +{round(raw_adj,2)}")
    else:
        add_rule('Pattern reliability', True, 'neutral', 'no strong pattern memory yet')

    stock_stat = (stock_memory or {}).get(ticker)
    if stock_stat and int(stock_stat.get('trades') or 0) >= STRICT_PER_STOCK_MIN_TRADES:
        sw = float(stock_stat.get('win_rate_pct') or 0)
        se = float(stock_stat.get('expectancy_pct') or 0)
        if sw < STRICT_PER_STOCK_MIN_WIN_RATE or se < STRICT_PER_STOCK_MIN_EXPECTANCY:
            adjustment -= 3.0
            add_rule('Per-stock historical strength', False, 'downgrade', f"{ticker} backtest history weak: {round(sw,1)}% win, avg {round(se,3)}%")
        else:
            adjustment += min(3.0, max(0.0, se * 2))
            add_rule('Per-stock historical strength', True, 'boost', f"{ticker} backtest history acceptable: {round(sw,1)}% win, avg {round(se,3)}%")
    else:
        add_rule('Per-stock historical strength', True, 'neutral', 'not enough stock-specific history yet')

    hist_stat = (trade_memory or {}).get(ticker)
    min_hist = STRICT_INTRADAY_HISTORY_MIN_TRADES if engine == 'intraday' else STRICT_TRADE_HISTORY_MIN_TRADES
    if hist_stat and int(hist_stat.get('trades') or 0) >= min_hist:
        hw = float(hist_stat.get('win_rate_pct') or 0)
        he = float(hist_stat.get('expectancy_pct') or 0)
        label = 'Intraday history' if engine == 'intraday' else 'Automation trade history'
        if hw < STRICT_PER_STOCK_MIN_WIN_RATE or he < 0:
            adjustment -= 4.0
            add_rule(label, False, 'block' if engine in ('trading_automation','intraday') else 'downgrade', f"actual/paper history weak: {round(hw,1)}% win, avg {round(he,3)}%")
        else:
            adjustment += min(3.0, max(0.0, he * 2))
            add_rule(label, True, 'boost', f"actual/paper history acceptable: {round(hw,1)}% win, avg {round(he,3)}%")

    allowed = not hard_blocks
    if engine == 'prediction':
        # Predictions can remain visible as WATCH/AVOID even when strict mode blocks BUY.
        allowed = True
    verdict = 'allow_buy'
    if hard_blocks:
        verdict = 'no_trade'
    elif cautions:
        verdict = 'watch_only'
    elif boosts:
        verdict = 'favorable'
    return {
        'enabled': True,
        'engine': engine,
        'ticker': ticker,
        'pattern_key': pattern_key,
        'allowed': allowed,
        'verdict': verdict,
        'hard_blocks': hard_blocks,
        'cautions': cautions,
        'boosts': boosts,
        'rules': rules,
        'score_before': round(score, 2),
        'score_adjustment': round(adjustment, 2),
        'score_after': round(score + adjustment, 2),
        'risk_reward': round(rr or 0, 2),
        'volume_confirmation': round(vol, 2) if vol is not None else None,
        'market_regime': regime,
    }


def apply_strict_learning_to_picks(picks: List[Dict[str, Any]], candidates: List[Dict[str, Any]], market: str, horizon: str,
                                   market_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not STRICT_LEARNING_ENABLED:
        return {'picks': picks, 'summary': {'enabled': False}}
    candidate_map = {str(c.get('ticker','')).upper(): c for c in candidates}
    stock_memory = get_stock_backtest_strength(market, horizon, list(candidate_map.keys()))
    buy_count_before = sum(1 for p in picks if p.get('signal') == 'BUY')
    downgraded = 0
    blocked_buy = 0
    for p in picks:
        t = str(p.get('ticker') or '').upper()
        c = dict(candidate_map.get(t) or {})
        # Merge candidate technical data with final pick prices/RR.
        merged = {**c, **p}
        review = strict_learning_review(merged, market, horizon, engine='prediction', market_context=market_context, stock_memory=stock_memory)
        p['strict_learning'] = review
        p['strict_learning_verdict'] = review.get('verdict')
        p['strict_learning_note'] = '; '.join((review.get('hard_blocks') or review.get('cautions') or review.get('boosts') or [])[:2])
        p['features_json'] = {**(p.get('features_json') or {}), 'strict_learning': review}
        if review.get('enabled'):
            # Strong future predictions should not remain BUY if strict mode sees weak edge.
            if p.get('signal') == 'BUY' and review.get('verdict') in ('no_trade', 'watch_only'):
                p['signal'] = 'WATCH' if review.get('verdict') == 'watch_only' else 'AVOID'
                p['confidence'] = max(0, min(int(p.get('confidence') or 50), 62 if p['signal'] == 'AVOID' else 68))
                p['reasoning'] = (p.get('reasoning') or '') + f" Strict Learning Mode downgraded it because {p.get('strict_learning_note') or 'edge is not strong enough'}."
                downgraded += 1
                if review.get('verdict') == 'no_trade':
                    blocked_buy += 1
    buy_count_after = sum(1 for p in picks if p.get('signal') == 'BUY')
    no_trade = STRICT_NO_TRADE_IF_NO_BUY and buy_count_after == 0
    return {
        'picks': picks,
        'summary': {
            'enabled': True,
            'buy_count_before': buy_count_before,
            'buy_count_after': buy_count_after,
            'downgraded': downgraded,
            'blocked_buy': blocked_buy,
            'no_trade_recommendation': no_trade,
            'message': 'Strict Learning Mode found no strong BUY today. Best action is WATCH / no trade.' if no_trade else 'Strict Learning Mode kept only stronger BUY setups.',
            'rules': strict_learning_architecture().get('strict_rules')
        }
    }


def compute_split_and_benchmark_metrics(trades: List[Dict[str, Any]], all_hist: Dict[str, Any], start_dt: datetime.date, end_dt: datetime.date) -> Dict[str, Any]:
    """Add training/validation and simple benchmark comparison to every backtest."""
    out: Dict[str, Any] = {}
    try:
        sorted_trades = sorted(trades, key=lambda t: str(t.get('signal_date') or ''))
        if sorted_trades:
            split_idx = max(1, int(len(sorted_trades) * 0.7))
            train = compute_backtest_metrics(sorted_trades[:split_idx])
            valid = compute_backtest_metrics(sorted_trades[split_idx:]) if len(sorted_trades[split_idx:]) else {}
            out['training_validation'] = {
                'policy': 'First 70% of simulated trades is training/readout; last 30% is validation/unseen-style check.',
                'training': {k: train.get(k) for k in ('total_trades','win_rate_pct','expectancy_pct','profit_factor','sharpe','max_drawdown_pct')},
                'validation': {k: valid.get(k) for k in ('total_trades','win_rate_pct','expectancy_pct','profit_factor','sharpe','max_drawdown_pct')},
                'validation_positive': bool((valid.get('total_trades') or 0) > 0 and (valid.get('expectancy_pct') or 0) > 0 and (valid.get('profit_factor') or 0) >= 1.0)
            }
    except Exception as e:
        out['training_validation'] = {'error': str(e)[:160]}
    try:
        universe_returns = []
        for ticker, h in all_hist.items():
            frame = normalize_history_frame(h)
            close = history_series(frame, 'Close')
            if close is None or len(close) < 2:
                continue
            period = [v for i, v in close.items() if i.date() >= start_dt and i.date() <= end_dt]
            if len(period) >= 2 and period[0]:
                universe_returns.append(((float(period[-1]) / float(period[0])) - 1) * 100)
        strat_sum = round(sum(float(t.get('net_return_pct') or 0) for t in trades), 3)
        bench = round(float(np.mean(universe_returns)), 3) if universe_returns else None
        out['benchmark_comparison'] = {
            'benchmark': 'Equal-weight buy-and-hold of loaded universe over same period',
            'benchmark_return_pct': bench,
            'strategy_sum_return_pct': strat_sum,
            'strategy_minus_benchmark_pct': round(strat_sum - bench, 3) if bench is not None else None,
            'note': 'Benchmark is a simple reference, not a guarantee. It helps check whether the strategy adds value versus just holding the tested universe.'
        }
    except Exception as e:
        out['benchmark_comparison'] = {'error': str(e)[:160]}
    return out

def historical_snapshot_at(ticker: str, hist, end_idx: int) -> Optional[Dict[str, Any]]:
    hist = normalize_history_frame(hist)
    frame = hist.iloc[:end_idx + 1].copy()
    snap = build_stock_snapshot(ticker, frame)
    if snap:
        snap['atr_pct'] = compute_atr_pct_from_history(frame)
        snap['regime_info'] = classify_regime_from_history(frame)
        try:
            current = float(snap.get('current_price') or 0)
            support = float(snap.get('support_20') or current * 0.97)
            resistance = float(snap.get('resistance_20') or current * 1.04)
            snap['rr_proxy'] = round(((resistance-current)/max(current,0.01)) / max(((current-support)/max(current,0.01)), 0.01), 2)
        except Exception:
            snap['rr_proxy'] = None
    return snap


def score_historical_candidate(snapshot: Dict[str, Any], market: str, horizon: str) -> Dict[str, Any]:
    snapshot['momentum_score'] = score_momentum(snapshot)
    snapshot['technical_score'] = score_technical(snapshot)
    snapshot['fundamental_score'] = 0
    snapshot['news_score'] = 0
    snapshot['final_pre_ai_score'] = round(snapshot['momentum_score'] * 0.45 + snapshot['technical_score'] * 0.55, 2)
    return snapshot


def backtest_exit_learning_policy() -> Dict[str, Any]:
    return {
        'enabled': BACKTEST_EXIT_AWARE_ENABLED,
        'return_mode': BACKTEST_RETURN_MODE,
        'plain_summary': (
            'Raw baseline shows what the original trade would have done with only target/stop/time exit. '
            'Protected result replays the same trade with post-entry capital/profit protection. '
            'Learning keeps both so an avoided loss still teaches why the entry became risky.'
        ),
        'rules': {
            'min_weak_signals': BACKTEST_PROTECTED_MIN_WEAK_SIGNALS,
            'loss_exit_pct': BACKTEST_PROTECTED_LOSS_EXIT_PCT,
            'profit_fade_min_pct': BACKTEST_PROTECTED_PROFIT_FADE_MIN_PCT,
            'exit_price_model': BACKTEST_PROTECTED_EXIT_PRICE_MODEL,
            'bar_level_note': (
                'Daily/week backtests use OHLC bars, not tick-by-tick execution. '
                'In threshold mode, if the candle proves the protective threshold was touched and stacked weakness exists, '
                'protected replay exits at the threshold price; if price gaps beyond it, it exits at the bar open. '
                'For exact timing, intraday historical candles or broker/exchange tick data are needed.'
            )
        }
    }


def backtest_bar_weakness_signals(hist, i: int, snapshot: Dict[str, Any], entry_price: float) -> Tuple[List[str], float]:
    """Detect bar-level weakening after entry without using future bars beyond the current bar.

    This is intentionally conservative: one weak candle should not panic-exit. It returns
    multiple reasons and a score so protected backtests only exit when risk evidence stacks up.
    """
    signals: List[str] = []
    try:
        close = float(hist['Close'].iloc[i]) if 'Close' in hist else entry_price
        open_p = float(hist['Open'].iloc[i]) if 'Open' in hist else close
        high = float(hist['High'].iloc[i]) if 'High' in hist else close
        low = float(hist['Low'].iloc[i]) if 'Low' in hist else close
        prev_close = float(hist['Close'].iloc[i-1]) if i > 0 and 'Close' in hist else close
        prev_high = float(hist['High'].iloc[i-1]) if i > 0 and 'High' in hist else high
        pnl_pct = ((close - entry_price) / entry_price) * 100 if entry_price else 0.0
        # Only use bars up to current index for rolling context.
        recent = hist.iloc[max(0, i-5):i+1]
        closes = recent['Close'].astype(float) if 'Close' in recent else None
        vols = recent['Volume'].astype(float) if 'Volume' in recent else None
        ma3 = float(closes.tail(3).mean()) if closes is not None and len(closes) >= 3 else close
        vol_now = float(vols.iloc[-1]) if vols is not None and len(vols) else 0.0
        vol_avg = float(vols.tail(5).mean()) if vols is not None and len(vols) >= 3 else vol_now
        if close < prev_close:
            signals.append('price closed below previous close')
        if close < ma3:
            signals.append('price fell below short-term average')
        if high < prev_high and close < open_p:
            signals.append('lower high with red candle')
        if vol_avg > 0 and vol_now < vol_avg * 0.65:
            signals.append('volume confirmation faded')
        if pnl_pct < BACKTEST_PROTECTED_LOSS_EXIT_PCT:
            signals.append('trade moved into capital-risk zone')
        if float(snapshot.get('volume_ratio_vs_20d_avg') or 1.0) < 0.8:
            signals.append('original setup had weak volume')
        if float(snapshot.get('rr_proxy') or 0) < 1.1:
            signals.append('original reward/risk was weak')
        regime = (snapshot.get('regime_info') or {}).get('regime') or ''
        if regime in ('risk_off_or_high_vol', 'bearish'):
            signals.append('market type was risky')
        pattern_verdict = str(snapshot.get('pattern_verdict') or snapshot.get('strict_learning_verdict') or '').lower()
        if 'avoid' in pattern_verdict or pattern_verdict == 'no_trade':
            signals.append('historical pattern memory was risky')
        # De-duplicate while preserving order.
        deduped = []
        for sig in signals:
            if sig not in deduped:
                deduped.append(sig)
        score = 0.0
        for sig in deduped:
            if 'capital-risk' in sig or 'risky' in sig or 'weak reward' in sig or 'pattern memory' in sig:
                score += 25
            elif 'volume' in sig:
                score += 18
            else:
                score += 14
        return deduped, min(score, 100.0)
    except Exception:
        return [], 0.0


def simulate_backtest_trade(ticker: str, hist, signal_idx: int, snapshot: Dict[str, Any], horizon: str) -> Optional[Dict[str, Any]]:
    try:
        hold_sessions = 1 if horizon == 'day' else 5
        if signal_idx + 1 >= len(hist):
            return None
        entry_idx = signal_idx + 1
        exit_limit_idx = min(signal_idx + hold_sessions, len(hist) - 1)
        if entry_idx > exit_limit_idx:
            return None
        entry_open = float(hist['Open'].iloc[entry_idx]) if 'Open' in hist else float(hist['Close'].iloc[entry_idx])
        if entry_open <= 0:
            return None
        atr_pct = snapshot.get('atr_pct') or 2.0
        score = float(snapshot.get('final_pre_ai_score') or 0)
        target_pct = max(0.8 if horizon == 'day' else 2.0, min((score / 28) * (1.0 if horizon == 'day' else 1.8), 6.0 if horizon == 'day' else 12.0))
        stop_pct = max(0.45 if horizon == 'day' else 1.2, min(float(atr_pct) * VOL_TARGET_ATR_RISK_MULTIPLIER, 4.0 if horizon == 'day' else 7.0))
        target_price = entry_open * (1 + target_pct / 100)
        stop_price = entry_open * (1 - stop_pct / 100)
        execution_cost_pct = (EXECUTION_SLIPPAGE_BPS + EXECUTION_SPREAD_BPS + EXECUTION_FEE_BPS) / 100.0

        def d(x):
            return x.date().isoformat() if hasattr(x, 'date') else str(x)[:10]

        def baseline_exit():
            outcome = 'horizon_exit'
            exit_price = float(hist['Close'].iloc[exit_limit_idx])
            actual_idx = exit_limit_idx
            for j in range(entry_idx, exit_limit_idx + 1):
                high = float(hist['High'].iloc[j]) if 'High' in hist else float(hist['Close'].iloc[j])
                low = float(hist['Low'].iloc[j]) if 'Low' in hist else float(hist['Close'].iloc[j])
                # Conservative ordering for daily bars when both target and stop are inside same candle.
                if low <= stop_price:
                    return stop_price, j, 'stop_loss_hit'
                if high >= target_price:
                    return target_price, j, 'target_hit'
            return exit_price, actual_idx, outcome

        unprotected_exit_price, unprotected_idx, unprotected_outcome = baseline_exit()
        unprotected_raw_return_pct = ((unprotected_exit_price - entry_open) / entry_open) * 100
        unprotected_net_return_pct = unprotected_raw_return_pct - execution_cost_pct

        protected_exit_price, protected_idx, protected_outcome = unprotected_exit_price, unprotected_idx, unprotected_outcome
        protected_action = 'RAW_BASELINE_ONLY'
        protected_reason = 'Exit-aware backtest disabled or raw mode selected.'
        if BACKTEST_EXIT_AWARE_ENABLED:
            protected_action = 'HOLD_BASELINE'
            protected_reason = 'No stacked weakening/capital-risk signals before baseline exit.'
            for j in range(entry_idx, exit_limit_idx + 1):
                high = float(hist['High'].iloc[j]) if 'High' in hist else float(hist['Close'].iloc[j])
                open_j = float(hist['Open'].iloc[j]) if 'Open' in hist else entry_open
                low = float(hist['Low'].iloc[j]) if 'Low' in hist else float(hist['Close'].iloc[j])
                close_j = float(hist['Close'].iloc[j]) if 'Close' in hist else entry_open
                if low <= stop_price:
                    protected_exit_price, protected_idx, protected_outcome = stop_price, j, 'stop_loss_hit'
                    protected_action = 'STOP_LOSS'
                    protected_reason = 'Stop-loss was touched before protective exit conditions could act.'
                    break
                if high >= target_price:
                    protected_exit_price, protected_idx, protected_outcome = target_price, j, 'target_hit'
                    protected_action = 'TARGET_HIT'
                    protected_reason = 'Target was reached while setup remained valid.'
                    break
                weak_signals, risk_score = backtest_bar_weakness_signals(hist, j, snapshot, entry_open)
                pnl_now = ((close_j - entry_open) / entry_open) * 100
                enough_weak = len(weak_signals) >= BACKTEST_PROTECTED_MIN_WEAK_SIGNALS
                # Protect capital only when the trade is already losing and weakness is confirmed.
                if pnl_now <= BACKTEST_PROTECTED_LOSS_EXIT_PCT and enough_weak and risk_score >= 55:
                    protective_threshold_price = entry_open * (1 + BACKTEST_PROTECTED_LOSS_EXIT_PCT / 100.0)
                    threshold_touched = low <= protective_threshold_price
                    if BACKTEST_PROTECTED_EXIT_PRICE_MODEL == 'threshold' and threshold_touched:
                        # If a later daily bar opens beyond the threshold, assume the protective order
                        # fills at the worse open. Otherwise use the configured protective threshold.
                        simulated_protective_price = open_j if (j > entry_idx and open_j < protective_threshold_price) else protective_threshold_price
                        model_note = f"protective threshold touched; simulated exit at {simulated_protective_price:.2f}"
                    else:
                        simulated_protective_price = close_j
                        model_note = 'close-only protective replay; no earlier threshold fill assumed'
                    protected_exit_price, protected_idx, protected_outcome = simulated_protective_price, j, 'protected_capital_exit'
                    protected_action = 'CAPITAL_PROTECTION_EXIT'
                    protected_reason = '; '.join(weak_signals[:4] + [model_note])
                    break
                # Protect profit only when there is profit to protect and weakness is confirmed.
                if pnl_now >= BACKTEST_PROTECTED_PROFIT_FADE_MIN_PCT and enough_weak and risk_score >= 70:
                    protected_exit_price, protected_idx, protected_outcome = close_j, j, 'protected_profit_exit'
                    protected_action = 'PROFIT_PROTECTION_EXIT'
                    protected_reason = '; '.join(weak_signals[:4])
                    break
        protected_raw_return_pct = ((protected_exit_price - entry_open) / entry_open) * 100
        protected_net_return_pct = protected_raw_return_pct - execution_cost_pct
        use_protected = BACKTEST_EXIT_AWARE_ENABLED and BACKTEST_RETURN_MODE == 'protected'
        final_exit_price = protected_exit_price if use_protected else unprotected_exit_price
        final_idx = protected_idx if use_protected else unprotected_idx
        final_outcome = protected_outcome if use_protected else unprotected_outcome
        final_raw_return_pct = protected_raw_return_pct if use_protected else unprotected_raw_return_pct
        final_net_return_pct = protected_net_return_pct if use_protected else unprotected_net_return_pct

        idx_signal = hist.index[signal_idx]
        idx_entry = hist.index[entry_idx]
        idx_exit = hist.index[final_idx]
        features = {k: snapshot.get(k) for k in ['rsi_14','macd_histogram','volume_ratio_vs_20d_avg','price_vs_ma20_pct','price_vs_ma50_pct','atr_pct','rr_proxy']}
        pattern_key = backtest_pattern_key({**features, 'macd_histogram': snapshot.get('macd_histogram')})
        avoided_loss_pct = max(0.0, protected_net_return_pct - unprotected_net_return_pct)
        # Missed profit is possible if protection exits earlier and baseline later reached a better result.
        missed_profit_pct = max(0.0, unprotected_net_return_pct - protected_net_return_pct)
        trade = {
            'ticker': ticker, 'signal_date': d(idx_signal), 'entry_date': d(idx_entry), 'exit_date': d(idx_exit),
            'entry_price': round(entry_open, 4), 'exit_price': round(final_exit_price, 4), 'target_price': round(target_price, 4), 'stop_price': round(stop_price, 4),
            'raw_return_pct': round(final_raw_return_pct, 4), 'net_return_pct': round(final_net_return_pct, 4), 'outcome': final_outcome,
            'regime': (snapshot.get('regime_info') or {}).get('regime', 'unknown'), 'score': round(float(snapshot.get('final_pre_ai_score') or 0), 2),
            'rsi': snapshot.get('rsi_14'), 'volume_ratio': snapshot.get('volume_ratio_vs_20d_avg'), 'atr_pct': snapshot.get('atr_pct'),
            'pattern_key': pattern_key, 'features': features,
            'unprotected_exit_price': round(unprotected_exit_price, 4),
            'unprotected_net_return_pct': round(unprotected_net_return_pct, 4),
            'unprotected_outcome': unprotected_outcome,
            'protected_exit_price': round(protected_exit_price, 4),
            'protected_net_return_pct': round(protected_net_return_pct, 4),
            'protected_outcome': protected_outcome,
            'protected_action': protected_action,
            'protected_reason': protected_reason,
            'avoided_loss_pct': round(avoided_loss_pct, 4),
            'missed_profit_pct': round(missed_profit_pct, 4),
            'entry_learning_return_pct': round(unprotected_net_return_pct, 4),
            'exit_learning_delta_pct': round(protected_net_return_pct - unprotected_net_return_pct, 4),
            'exit_aware_enabled': BACKTEST_EXIT_AWARE_ENABLED,
            'return_mode': BACKTEST_RETURN_MODE,
        }
        # Entry learning deliberately uses raw/unprotected outcome so avoided losses
        # still teach the system why the original setup was risky.
        lesson_trade = dict(trade)
        lesson_trade['net_return_pct'] = trade['entry_learning_return_pct']
        lesson_trade['outcome'] = trade['unprotected_outcome']
        lesson = explain_backtest_failure(lesson_trade)
        if avoided_loss_pct > 0 and trade.get('lesson_summary'):
            lesson['lesson_summary'] = (lesson.get('lesson_summary') or '') + f" Exit-aware replay reduced historical loss by {avoided_loss_pct:.2f} percentage points, but entry pattern still gets learned from the raw baseline."
        elif avoided_loss_pct > 0:
            lesson['lesson_summary'] = f"Exit-aware replay reduced historical loss by {avoided_loss_pct:.2f} percentage points; keep entry-risk learning separate from exit-quality learning."
        trade.update(lesson)
        return trade
    except Exception as e:
        print(f"[WARN] simulate_backtest_trade failed for {ticker}: {e}")
        return None


def compute_backtest_metrics(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    # returns = selected reporting return. In protected mode this uses the
    # exit-aware result. entry_returns always uses raw/unprotected baseline so
    # bad entry patterns still teach the learning engine even if protection saved loss.
    returns = [float(t.get('net_return_pct') or 0) for t in trades]
    entry_returns = [float(t.get('entry_learning_return_pct') if t.get('entry_learning_return_pct') is not None else (t.get('unprotected_net_return_pct') if t.get('unprotected_net_return_pct') is not None else t.get('net_return_pct') or 0)) for t in trades]
    if not returns:
        return {'total_trades': 0, 'win_rate_pct': None, 'expectancy_pct': None, 'sharpe': None, 'sortino': None, 'max_drawdown_pct': None}
    wins = [r for r in returns if r > 0]
    losses = [r for r in returns if r <= 0]
    avg = float(np.mean(returns))
    std = float(np.std(returns)) if len(returns) > 1 else 0.0
    downside = float(np.std([min(r,0) for r in returns])) if len(returns) > 1 else 0.0
    sharpe = (avg / std) * math.sqrt(252) if std > 0 else None
    sortino = (avg / downside) * math.sqrt(252) if downside > 0 else None
    equity = 100.0
    peak = 100.0
    max_dd = 0.0
    for r in returns:
        equity *= (1 + r / 100)
        peak = max(peak, equity)
        max_dd = min(max_dd, ((equity - peak) / peak) * 100)
    profit_factor = (sum(wins) / abs(sum(losses))) if losses and abs(sum(losses)) > 0 else (999.0 if wins else None)
    by_regime, by_pattern = {}, {}
    # Regime/pattern learning uses raw entry-return evidence to avoid hiding bad
    # setups that were rescued by protected exits. Overall performance cards still
    # use the selected reporting return above.
    for t, r in zip(trades, entry_returns):
        by_regime.setdefault(t.get('regime') or 'unknown', []).append(r)
        by_pattern.setdefault(t.get('pattern_key') or 'general', []).append(r)
    regime_stats = {k: {'trades': len(v), 'win_rate_pct': round(sum(1 for x in v if x > 0)/len(v)*100, 1), 'expectancy_pct': round(float(np.mean(v)), 3)} for k,v in by_regime.items() if v}
    pattern_stats = {k: {'trades': len(v), 'win_rate_pct': round(sum(1 for x in v if x > 0)/len(v)*100, 1), 'expectancy_pct': round(float(np.mean(v)), 3)} for k,v in by_pattern.items() if len(v) >= 3}
    best_regime = max(regime_stats.items(), key=lambda kv: kv[1]['expectancy_pct'])[0] if regime_stats else None
    worst_regime = min(regime_stats.items(), key=lambda kv: kv[1]['expectancy_pct'])[0] if regime_stats else None
    by_stock = {}
    for t, r in zip(trades, returns):
        ticker = t.get('ticker') or 'UNKNOWN'
        bucket = by_stock.setdefault(ticker, [])
        bucket.append((r, t))
    saved = [float(t.get('avoided_loss_pct') or 0) for t in trades]
    missed = [float(t.get('missed_profit_pct') or 0) for t in trades]
    protected_exits = [t for t in trades if str(t.get('protected_action') or '').upper() in ('CAPITAL_PROTECTION_EXIT','PROFIT_PROTECTION_EXIT')]
    exit_aware_summary = {
        'enabled': BACKTEST_EXIT_AWARE_ENABLED,
        'return_mode': BACKTEST_RETURN_MODE,
        'raw_entry_avg_pct': round(float(np.mean(entry_returns)), 3) if entry_returns else None,
        'protected_avg_pct': round(float(np.mean(returns)), 3) if returns else None,
        'protection_exits': len(protected_exits),
        'total_avoided_loss_pct': round(sum(saved), 3),
        'avg_avoided_loss_pct': round(float(np.mean([x for x in saved if x > 0])), 3) if any(x > 0 for x in saved) else 0,
        'total_missed_profit_pct': round(sum(missed), 3),
        'plain_summary': 'Overall result uses protected exits when enabled, but learning rules still use raw entry baseline so avoided losses do not hide weak patterns.'
    }
    stock_stats = []
    for ticker, values in by_stock.items():
        stock_returns = [v[0] for v in values]
        stock_wins = [x for x in stock_returns if x > 0]
        stock_losses = [x for x in stock_returns if x <= 0]
        outcomes = {}
        reasons = {}
        regimes = {}
        for _, trade in values:
            outcomes[trade.get('outcome') or 'unknown'] = outcomes.get(trade.get('outcome') or 'unknown', 0) + 1
            if trade.get('failure_reason'):
                reasons[trade.get('failure_reason')] = reasons.get(trade.get('failure_reason'), 0) + 1
            regimes[trade.get('regime') or 'unknown'] = regimes.get(trade.get('regime') or 'unknown', 0) + 1
        stock_stats.append({
            'ticker': ticker,
            'trades': len(stock_returns),
            'win_rate_pct': round((len(stock_wins) / len(stock_returns)) * 100, 1) if stock_returns else None,
            'expectancy_pct': round(float(np.mean(stock_returns)), 3) if stock_returns else None,
            'avg_win_pct': round(float(np.mean(stock_wins)), 3) if stock_wins else None,
            'avg_loss_pct': round(float(np.mean(stock_losses)), 3) if stock_losses else None,
            'best_trade_pct': round(max(stock_returns), 3) if stock_returns else None,
            'worst_trade_pct': round(min(stock_returns), 3) if stock_returns else None,
            'target_hits': outcomes.get('target_hit', 0),
            'stop_hits': outcomes.get('stop_loss_hit', 0),
            'horizon_exits': outcomes.get('horizon_exit', 0),
            'top_failure_reason': max(reasons.items(), key=lambda kv: kv[1])[0] if reasons else None,
            'dominant_regime': max(regimes.items(), key=lambda kv: kv[1])[0] if regimes else None,
        })
    stock_stats.sort(key=lambda x: (x.get('trades') or 0, x.get('expectancy_pct') or -999), reverse=True)

    return {
        'total_trades': len(returns), 'win_rate_pct': round((len(wins)/len(returns))*100, 1), 'expectancy_pct': round(avg, 3), 'avg_trade_pct': round(avg, 3),
        'avg_win_pct': round(float(np.mean(wins)), 3) if wins else None, 'avg_loss_pct': round(float(np.mean(losses)), 3) if losses else None,
        'avg_win_loss_ratio': round((float(np.mean(wins)) / abs(float(np.mean(losses)))), 2) if wins and losses and abs(float(np.mean(losses))) > 0 else None,
        'profit_factor': round(float(profit_factor), 3) if profit_factor is not None else None,
        'sharpe': round(float(sharpe), 3) if sharpe is not None else None, 'sortino': round(float(sortino), 3) if sortino is not None else None,
        'max_drawdown_pct': round(max_dd, 3), 'final_equity_index': round(equity, 2),
        'regime_stats': regime_stats, 'pattern_stats': pattern_stats, 'stock_stats': stock_stats[:BACKTEST_STOCK_STATS_LIMIT], 'best_regime': best_regime, 'worst_regime': worst_regime,
        'execution_assumptions': {'slippage_bps': EXECUTION_SLIPPAGE_BPS, 'spread_bps': EXECUTION_SPREAD_BPS, 'fee_bps': EXECUTION_FEE_BPS},
        'exit_aware_learning': exit_aware_summary,
    }


def update_strategy_signal_stats(cur, market: str, horizon: str, metrics: Dict[str, Any]):
    for regime, stat in (metrics.get('regime_stats') or {}).items():
        adj = max(min((float(stat.get('expectancy_pct') or 0) * 1.8), 5.0), -7.0)
        cur.execute("""
            INSERT INTO strategy_signal_stats
                (market, horizon, regime, pattern_key, trades, win_rate_pct, expectancy_pct, avg_return_pct, confidence_adjustment, notes, updated_at)
            VALUES (%s,%s,%s,'all',%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (market, horizon, regime, pattern_key) DO UPDATE SET
                trades=EXCLUDED.trades, win_rate_pct=EXCLUDED.win_rate_pct, expectancy_pct=EXCLUDED.expectancy_pct,
                avg_return_pct=EXCLUDED.avg_return_pct, confidence_adjustment=EXCLUDED.confidence_adjustment,
                notes=EXCLUDED.notes, updated_at=NOW()
        """, (market, horizon, regime, stat.get('trades'), stat.get('win_rate_pct'), stat.get('expectancy_pct'), stat.get('expectancy_pct'), adj, 'Updated from historical backtest regime expectancy.'))
    for pattern, stat in (metrics.get('pattern_stats') or {}).items():
        if int(stat.get('trades') or 0) < 3:
            continue
        adj = max(min((float(stat.get('expectancy_pct') or 0) * 1.5), 4.0), -6.0)
        cur.execute("""
            INSERT INTO strategy_signal_stats
                (market, horizon, regime, pattern_key, trades, win_rate_pct, expectancy_pct, avg_return_pct, confidence_adjustment, notes, updated_at)
            VALUES (%s,%s,'all',%s,%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (market, horizon, regime, pattern_key) DO UPDATE SET
                trades=EXCLUDED.trades, win_rate_pct=EXCLUDED.win_rate_pct, expectancy_pct=EXCLUDED.expectancy_pct,
                avg_return_pct=EXCLUDED.avg_return_pct, confidence_adjustment=EXCLUDED.confidence_adjustment,
                notes=EXCLUDED.notes, updated_at=NOW()
        """, (market, horizon, pattern, stat.get('trades'), stat.get('win_rate_pct'), stat.get('expectancy_pct'), stat.get('expectancy_pct'), adj, 'Updated from historical backtest pattern expectancy.'))


def persist_backtest_result(market: str, horizon: str, start_dt: datetime.date, end_dt: datetime.date, universe_limit: int, top_n: int, step: int,
                            trades: List[Dict[str, Any]], metrics: Dict[str, Any], started_at: datetime.datetime,
                            fingerprint_hash: Optional[str] = None, fingerprint_payload: Optional[Dict[str, Any]] = None,
                            duplicate_of_run_id: Optional[int] = None, learning_applied: bool = True) -> Optional[int]:
    conn = get_db_connection()
    if conn is None:
        return None
    try:
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    INSERT INTO backtest_runs
                        (created_at, market, horizon, start_date, end_date, universe_limit, top_n, rebalance_step_days,
                         total_trades, win_rate_pct, expectancy_pct, avg_trade_pct, avg_win_pct, avg_loss_pct,
                         profit_factor, sharpe, sortino, max_drawdown_pct, best_regime, worst_regime,
                         status, metrics_json, config_json, fingerprint_hash, duplicate_of_run_id, learning_applied)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'complete',%s::jsonb,%s::jsonb,%s,%s,%s)
                    RETURNING id
                """, (
                    started_at, market, horizon, start_dt, end_dt, universe_limit, top_n, step,
                    metrics.get('total_trades') or 0, metrics.get('win_rate_pct'), metrics.get('expectancy_pct'),
                    metrics.get('avg_trade_pct'), metrics.get('avg_win_pct'), metrics.get('avg_loss_pct'),
                    metrics.get('profit_factor'), metrics.get('sharpe'), metrics.get('sortino'),
                    metrics.get('max_drawdown_pct'), metrics.get('best_regime'), metrics.get('worst_regime'),
                    json.dumps(metrics, default=_json_safe),
                    json.dumps({
                        'execution_assumptions': metrics.get('execution_assumptions'),
                        'source': metrics.get('source'),
                        'fingerprint_hash': fingerprint_hash,
                        'fingerprint_payload': fingerprint_payload,
                        'duplicate_of_run_id': duplicate_of_run_id,
                        'learning_applied': learning_applied,
                    }, default=_json_safe),
                    fingerprint_hash, duplicate_of_run_id, learning_applied
                ))
                run_id = cur.fetchone()['id']
                for t in trades[:5000]:
                    cur.execute("""
                        INSERT INTO backtest_trades
                            (run_id, ticker, market, horizon, signal_date, entry_date, exit_date, entry_price, exit_price,
                             target_price, stop_price, raw_return_pct, net_return_pct, outcome, regime, score, rsi,
                             volume_ratio, atr_pct, failure_reason, lesson_summary, pattern_key, pattern_verdict,
                             unprotected_exit_price, unprotected_net_return_pct, unprotected_outcome,
                             protected_exit_price, protected_net_return_pct, protected_outcome, protected_action, protected_reason,
                             avoided_loss_pct, missed_profit_pct, entry_learning_return_pct, exit_learning_delta_pct, features_json)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                    """, (
                        run_id, t.get('ticker'), market, horizon, t.get('signal_date'), t.get('entry_date'), t.get('exit_date'),
                        t.get('entry_price'), t.get('exit_price'), t.get('target_price'), t.get('stop_price'),
                        t.get('raw_return_pct'), t.get('net_return_pct'), t.get('outcome'), t.get('regime'),
                        t.get('score'), t.get('rsi'), t.get('volume_ratio'), t.get('atr_pct'),
                        t.get('failure_reason'), t.get('lesson_summary'), t.get('pattern_key'), t.get('pattern_verdict'),
                        t.get('unprotected_exit_price'), t.get('unprotected_net_return_pct'), t.get('unprotected_outcome'),
                        t.get('protected_exit_price'), t.get('protected_net_return_pct'), t.get('protected_outcome'),
                        t.get('protected_action'), t.get('protected_reason'), t.get('avoided_loss_pct'), t.get('missed_profit_pct'),
                        t.get('entry_learning_return_pct'), t.get('exit_learning_delta_pct'),
                        json.dumps({**(t.get('features') or {}), 'pattern_key': t.get('pattern_key'), 'failure_reason': t.get('failure_reason'), 'lesson_summary': t.get('lesson_summary'), 'exit_aware_learning': {'enabled': t.get('exit_aware_enabled'), 'return_mode': t.get('return_mode'), 'unprotected_outcome': t.get('unprotected_outcome'), 'unprotected_net_return_pct': t.get('unprotected_net_return_pct'), 'protected_outcome': t.get('protected_outcome'), 'protected_net_return_pct': t.get('protected_net_return_pct'), 'protected_action': t.get('protected_action'), 'protected_reason': t.get('protected_reason'), 'avoided_loss_pct': t.get('avoided_loss_pct'), 'missed_profit_pct': t.get('missed_profit_pct')}}, default=_json_safe)
                    ))
                if learning_applied:
                    update_strategy_signal_stats(cur, market, horizon, metrics)
                return run_id
    except Exception as e:
        print(f"[WARN] persist_backtest_result failed: {e}")
        return None
    finally:
        conn.close()


def strict_rejection_label_from_review(review: Dict[str, Any]) -> str:
    """Turn strict rule failures into short dashboard labels."""
    try:
        failed = [r for r in (review.get('rules') or []) if not r.get('passed')]
        names = ' | '.join(str(r.get('name') or '') for r in failed).lower()
        details = ' | '.join(str(r.get('detail') or '') for r in failed).lower()
        blob = names + ' ' + details
        if 'score' in blob:
            return 'low setup score'
        if 'reward' in blob or 'risk' in blob:
            return 'weak reward vs risk'
        if 'volume' in blob:
            return 'weak volume confirmation'
        if 'regime' in blob or 'risk_off' in blob or 'high volatility' in blob:
            return 'risky market type'
        if 'bearish' in blob:
            return 'bearish broad market'
        if 'macro' in blob or 'news' in blob:
            return 'major news/macro risk'
        if 'pattern' in blob:
            return 'bad historical pattern'
        if 'stock' in blob:
            return 'weak stock-specific history'
    except Exception:
        pass
    return 'strict filter'


def bump_count(bucket: Dict[str, int], label: str, by: int = 1) -> None:
    bucket[label] = int(bucket.get(label) or 0) + by


def strategy_memory_signature(market: str, horizon: str) -> str:
    """Hash of the current learned pattern/regime memory state.

    Included in the fingerprint for 'learned' mode runs so that re-running the
    exact same window/settings AFTER new learning has accumulated is treated as
    a fresh, comparable test instead of being silently deduped against an older
    result computed with a different (or empty) memory state.
    """
    memory = get_strategy_memory(market, horizon, regime=None, limit=500)
    rows = memory.get('patterns') or []
    normalized = sorted([
        {
            'regime': r.get('regime'),
            'pattern_key': r.get('pattern_key'),
            'trades': r.get('trades'),
            'confidence_adjustment': float(r.get('confidence_adjustment') or 0),
        }
        for r in rows
    ], key=lambda x: (str(x['regime']), str(x['pattern_key'])))
    raw = json.dumps(normalized, sort_keys=True, default=_json_safe, separators=(',', ':'))
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()[:16]


def backtest_fingerprint_payload(market: str, horizon: str, start_dt: datetime.date, end_dt: datetime.date,
                                 universe_limit: int, top_n: int, step: int, universe: List[str],
                                 learning_mode: str = 'raw') -> Dict[str, Any]:
    """Build the exact evidence signature used for duplicate-learning protection."""
    payload = {
        'version': BACKTEST_FINGERPRINT_VERSION,
        'market': market,
        'horizon': horizon,
        'start_date': start_dt.isoformat(),
        'end_date': end_dt.isoformat(),
        'universe_limit': int(universe_limit),
        'top_n': int(top_n),
        'rebalance_step_days': int(step),
        'universe': list(universe),
        'learning_mode': learning_mode,
        # Only stamped for 'learned' mode: makes a rerun after new learning has
        # accumulated a genuinely different, non-duplicate fingerprint.
        'strategy_memory_signature': strategy_memory_signature(market, horizon) if learning_mode == 'learned' else None,
        'quality_filter': {
            'enabled': QUALITY_STOCK_FILTER_ENABLED,
            'thresholds': quality_thresholds_for_market(market),
            'min_price': QUALITY_MIN_PRICE,
            'max_zero_volume_days': QUALITY_MAX_ZERO_VOLUME_DAYS,
            'min_recent_bars': QUALITY_MIN_RECENT_BARS,
            'min_avg_volume_multiplier': QUALITY_MIN_AVG_VOLUME_MULTIPLIER,
            'min_turnover_multiplier': QUALITY_MIN_TURNOVER_MULTIPLIER,
        },
        'strict_learning': {
            'enabled': STRICT_LEARNING_ENABLED,
            'backtest_min_score': STRICT_BACKTEST_MIN_SCORE,
            'backtest_allow_watch_only': STRICT_BACKTEST_ALLOW_WATCH_ONLY,
            'backtest_min_risk_reward': STRICT_BACKTEST_MIN_RISK_REWARD,
            'backtest_min_volume_ratio': STRICT_BACKTEST_MIN_VOLUME_RATIO,
            'backtest_reject_risk_off': STRICT_BACKTEST_REJECT_RISK_OFF,
            'pattern_reliable_min_trades': STRICT_PATTERN_RELIABLE_MIN_TRADES,
            'per_stock_min_trades': STRICT_PER_STOCK_MIN_TRADES,
        },
        'execution_costs': {
            'slippage_bps': EXECUTION_SLIPPAGE_BPS,
            'spread_bps': EXECUTION_SPREAD_BPS,
            'fee_bps': EXECUTION_FEE_BPS,
        },
        'data_provider': MARKET_DATA_PROVIDER,
        'price_mode': get_backtest_price_mode_info(),
        'data_integrity_guard': data_integrity_thresholds(),
        'exit_aware_learning': backtest_exit_learning_policy(),
        'ticker_aliases': KNOWN_TICKER_ALIASES,
    }
    return payload


def backtest_fingerprint_hash(payload: Dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, default=_json_safe, separators=(',', ':'))
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()


def find_duplicate_backtest_run(fingerprint_hash: str) -> Optional[Dict[str, Any]]:
    if not fingerprint_hash or not BACKTEST_DUPLICATE_PROTECTION:
        return None
    conn = get_db_connection()
    if conn is None:
        return None
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, created_at, market, horizon, start_date, end_date, universe_limit, top_n,
                       rebalance_step_days, total_trades, win_rate_pct, expectancy_pct, max_drawdown_pct,
                       fingerprint_hash, learning_applied, metrics_json
                FROM backtest_runs
                WHERE fingerprint_hash=%s AND status='complete'
                ORDER BY created_at DESC
                LIMIT 1
            """, (fingerprint_hash,))
            row = cur.fetchone()
            return dict(row) if row else None
    except Exception as e:
        print(f"[WARN] duplicate backtest lookup failed: {e}")
        return None
    finally:
        conn.close()


def load_backtest_run_payload(run_id: int) -> Dict[str, Any]:
    """Internal version of the detail endpoint, used for duplicate-cache returns."""
    conn = get_db_connection()
    if conn is None:
        return {'ok': False, 'error': 'DATABASE_URL not set.'}
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, created_at, market, horizon, start_date, end_date, universe_limit, top_n,
                       rebalance_step_days, total_trades, win_rate_pct, expectancy_pct, avg_trade_pct,
                       avg_win_pct, avg_loss_pct, profit_factor, sharpe, sortino, max_drawdown_pct,
                       best_regime, worst_regime, status, error, metrics_json, config_json,
                       fingerprint_hash, duplicate_of_run_id, learning_applied
                FROM backtest_runs
                WHERE id=%s
            """, (run_id,))
            run = cur.fetchone()
            if not run:
                return {'ok': False, 'error': 'Backtest run not found.'}
            cur.execute("""
                SELECT id, ticker, market, horizon, signal_date, entry_date, exit_date, entry_price, exit_price,
                       target_price, stop_price, raw_return_pct, net_return_pct, outcome, regime, score,
                       rsi, volume_ratio, atr_pct, failure_reason, lesson_summary, pattern_key, pattern_verdict,
                       unprotected_exit_price, unprotected_net_return_pct, unprotected_outcome,
                       protected_exit_price, protected_net_return_pct, protected_outcome, protected_action, protected_reason,
                       avoided_loss_pct, missed_profit_pct, entry_learning_return_pct, exit_learning_delta_pct, features_json
                FROM backtest_trades
                WHERE run_id=%s
                ORDER BY signal_date DESC, ticker ASC
                LIMIT 10000
            """, (run_id,))
            trades = [dict(r) for r in cur.fetchall()]
        metrics = run.get('metrics_json') or {}
        if isinstance(metrics, str):
            try:
                metrics = json.loads(metrics)
            except Exception:
                metrics = {}
        run_dict = dict(run)
        per_stock = aggregate_stock_backtest_rows(trades)
        prev_run = None
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur2:
            prev_run = find_previous_same_config_run(cur2, run_dict)
            if prev_run:
                cur2.execute("""
                    SELECT ticker, net_return_pct, outcome, regime, failure_reason, lesson_summary,
                           pattern_key, pattern_verdict, score, rsi, volume_ratio, atr_pct
                    FROM backtest_trades WHERE run_id=%s
                """, (prev_run.get('id'),))
                prev_trades = [dict(r) for r in cur2.fetchall()]
                prev_per_stock = {s['ticker']: s for s in aggregate_stock_backtest_rows(prev_trades)}
                for s in per_stock:
                    prev_s = prev_per_stock.get(s.get('ticker'))
                    if prev_s:
                        s['prev_run_id'] = prev_run.get('id')
                        s['prev_win_rate_pct'] = prev_s.get('win_rate_pct')
                        s['prev_expectancy_pct'] = prev_s.get('expectancy_pct')
                        s['delta_win_rate_pct'] = (round(s.get('win_rate_pct') - prev_s.get('win_rate_pct'), 1)
                                                   if s.get('win_rate_pct') is not None and prev_s.get('win_rate_pct') is not None else None)
                        s['delta_expectancy_pct'] = (round(s.get('expectancy_pct') - prev_s.get('expectancy_pct'), 3)
                                                     if s.get('expectancy_pct') is not None and prev_s.get('expectancy_pct') is not None else None)
        run_dict['rerun_info'] = _rerun_delta(prev_run, run_dict) if prev_run else {'is_rerun': False}
        return {
            'ok': True,
            'run': run_dict,
            'metrics': metrics,
            'trades': trades,
            'per_stock': per_stock,
            'plain_note': 'This is the full history of one past backtest run. It shows old simulated trades used for research/learning, not tomorrow’s live picks.'
        }
    except Exception as e:
        return {'ok': False, 'error': str(e)}
    finally:
        conn.close()


def run_historical_backtest(market: str, horizon: str, start_date: Optional[str] = None, end_date: Optional[str] = None,
                            universe_limit: Optional[int] = None, top_n: Optional[int] = None,
                            rebalance_step_days: Optional[int] = None, learning_mode: str = 'raw',
                            commit_learning: bool = False) -> Dict[str, Any]:
    """commit_learning controls whether this run is allowed to WRITE into
    strategy_signal_stats (the shared pattern/regime memory that 'learned' mode
    reads from). Defaults to False: research/comparison/replication runs must
    opt in explicitly with commit_learning=True. This prevents a run from
    contaminating the evidence base that a sibling or subsequent run reads,
    which was previously making 'learned' mode a moving target between
    otherwise-identical reruns."""
    if not BACKTEST_ENABLED:
        return {'ok': False, 'error': 'Backtesting is disabled by BACKTEST_ENABLED=false'}
    market, horizon = validate_market_horizon(market, horizon)
    learning_mode = 'learned' if str(learning_mode or 'raw').lower() == 'learned' else 'raw'
    universe_limit = int(universe_limit or BACKTEST_UNIVERSE_LIMIT)
    top_n = int(top_n or BACKTEST_TOP_N)
    step = int(rebalance_step_days or BACKTEST_REBALANCE_STEP_DAYS)
    end_dt = datetime.date.fromisoformat(end_date) if end_date else datetime.date.today()
    start_dt = datetime.date.fromisoformat(start_date) if start_date else end_dt - datetime.timedelta(days=365 * BACKTEST_DEFAULT_LOOKBACK_YEARS)
    universe = load_market_universe(market)[:max(1, universe_limit)]
    fingerprint_payload = backtest_fingerprint_payload(market, horizon, start_dt, end_dt, universe_limit, top_n, step, universe, learning_mode=learning_mode)
    fingerprint_hash = backtest_fingerprint_hash(fingerprint_payload)

    if BACKTEST_DUPLICATE_PROTECTION and not BACKTEST_ALLOW_DUPLICATE_LEARNING:
        duplicate = find_duplicate_backtest_run(fingerprint_hash)
        if duplicate:
            payload = load_backtest_run_payload(int(duplicate['id']))
            if payload.get('ok'):
                run = payload.get('run') or {}
                metrics = dict(payload.get('metrics') or {})
                metrics.setdefault('total_trades', run.get('total_trades') or 0)
                metrics.setdefault('win_rate_pct', run.get('win_rate_pct'))
                metrics.setdefault('expectancy_pct', run.get('expectancy_pct'))
                metrics.setdefault('max_drawdown_pct', run.get('max_drawdown_pct'))
                metrics['ok'] = True
                metrics['run_id'] = run.get('id')
                metrics['duplicate_cached'] = True
                metrics['duplicate_of_run_id'] = run.get('id')
                metrics['learning_applied'] = False
                metrics['recent_trades'] = (payload.get('trades') or [])[:60]
                metrics['stock_stats'] = payload.get('per_stock') or []
                metrics['duplicate_protection'] = {
                    'enabled': True,
                    'duplicate_found': True,
                    'existing_run_id': run.get('id'),
                    'existing_created_at': run.get('created_at'),
                    'fingerprint_hash': fingerprint_hash,
                    'learning_applied': False,
                    'plain_summary': 'This exact backtest was already learned from, so the app showed the existing result and did not double-count the same evidence.'
                }
                return metrics

    all_hist: Dict[str, Any] = {}
    started = utc_now_naive()
    source_info = get_market_data_source_info(market, 'historical_backtest')
    source_info['price_mode'] = get_backtest_price_mode_info()
    source_info['data_integrity_guard'] = data_integrity_thresholds()
    deadline_ts = time.monotonic() + max(8, BACKTEST_MAX_RUNTIME_SECONDS)
    skipped_errors = []
    skipped_symbols = []
    skipped_quality = []
    quality_passed = []

    def fetch_hist(ticker):
        try:
            price_mode_info = get_backtest_price_mode_info()
            h = yf.Ticker(ticker).history(
                start=(start_dt - datetime.timedelta(days=220)).isoformat(),
                end=(end_dt + datetime.timedelta(days=10)).isoformat(),
                interval='1d',
                auto_adjust=bool(price_mode_info.get('yfinance_auto_adjust')),
                actions=True,
                timeout=YF_TIMEOUT,
            )
            if h is not None and not h.empty:
                h = normalize_history_frame(h)
                close_series = history_series(h, 'Close')
                if close_series is None or close_series.empty:
                    return ticker, None, 'No usable close-price history from data provider.'
                h = h.loc[close_series.index]
                split_dates = detect_split_dates(h)
                integrity = analyze_backtest_data_integrity(ticker, h, market, split_dates=split_dates)
                if not integrity.get('ok'):
                    return ticker, None, 'Data integrity guard blocked learning: ' + '; '.join((integrity.get('warnings') or [])[:2]), {'data_integrity': integrity}
                quality = analyze_historical_stock_quality(ticker, h, market)
                if not quality.get('ok'):
                    quality['data_integrity'] = integrity
                    return ticker, None, quality.get('reason') or 'Failed active/liquid quality filter.', quality
                quality['data_integrity'] = integrity
                quality['split_dates'] = split_dates
                return ticker, h, None, quality
            return ticker, None, 'No historical data from Yahoo/yfinance. Symbol may be changed, delisted, or unavailable.', None
        except BaseException as e:
            return ticker, None, str(e)[:180] or 'Data provider error.', None

    all_split_dates: Dict[str, set] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, min(BACKTEST_MAX_WORKERS, 4))) as ex:
        for ticker, h, skip_reason, quality in ex.map(fetch_hist, universe):
            if h is not None and not h.empty and len(h) >= BACKTEST_MIN_HISTORY_DAYS + 5:
                all_hist[ticker] = h
                all_split_dates[ticker] = (quality or {}).get('split_dates') or set()
                if quality and len(quality_passed) < 80:
                    quality_passed.append({'ticker': ticker, 'metrics': quality.get('metrics'), 'reason': quality.get('reason')})
            else:
                item = {
                    'ticker': ticker,
                    'reason': skip_reason or f'Not enough clean history. Minimum required: {BACKTEST_MIN_HISTORY_DAYS} daily bars.'
                }
                if quality:
                    item['quality'] = quality
                    if len(skipped_quality) < 80:
                        skipped_quality.append(item)
                if len(skipped_symbols) < 80:
                    skipped_symbols.append(item)
    if not all_hist:
        return {'ok': False, 'error': 'No historical bars available for selected universe/date range.'}
    calendar = []
    for h in all_hist.values():
        calendar = [idx for idx in h.index if idx.date().isoformat() >= start_dt.isoformat() and idx.date().isoformat() <= end_dt.isoformat()]
        if len(calendar) > 10:
            break
    trades = []
    strict_rejections: List[Dict[str, Any]] = []
    strict_rejection_counts: Dict[str, int] = {}
    strict_empty_test_days = 0
    strict_watch_only_tested = 0
    skipped_for_corporate_action = 0
    # 'learned' mode applies everything the system has learned so far (pattern-memory
    # score adjustments + per-stock historical strength) before ranking/filtering each
    # test day. 'raw' mode never applies these, so it always replays the base strategy
    # untouched by any accumulated learning -- this is the control group.
    stock_memory_for_learning = get_stock_backtest_strength(market, horizon, list(all_hist.keys())) if learning_mode == 'learned' else {}
    for cal_pos in range(0, len(calendar), max(step, 1)):
        if time.monotonic() > deadline_ts:
            break
        signal_ts = calendar[cal_pos]
        day_candidates = []
        for ticker, h in all_hist.items():
            try:
                eligible_idx = [i for i, idx in enumerate(h.index) if idx <= signal_ts]
                if not eligible_idx:
                    continue
                idx = eligible_idx[-1]
                if idx < BACKTEST_MIN_HISTORY_DAYS or idx + 1 >= len(h):
                    continue
                snap = historical_snapshot_at(ticker, h, idx)
                if not snap:
                    continue
                snap = score_historical_candidate(snap, market, horizon)
                if snap.get('avg_volume_20', 0) <= 0:
                    continue
                liq_ok, liq_reason = snapshot_passes_liquidity(snap, market)
                if not liq_ok:
                    bump_count(strict_rejection_counts, 'quality/liquidity gate')
                    if len(strict_rejections) < 80:
                        strict_rejections.append({'ticker': ticker, 'date': str(signal_ts.date()), 'reason': 'Quality/liquidity gate: ' + liq_reason, 'score': snap.get('final_pre_ai_score')})
                    continue
                snap['quality_gate'] = {'passed': True, 'reason': liq_reason}
                day_candidates.append((ticker, idx, snap))
            except BaseException as e:
                if len(skipped_errors) < 20:
                    skipped_errors.append({'ticker': ticker, 'date': str(signal_ts), 'error': str(e)[:160]})
                continue
        if learning_mode == 'learned' and day_candidates:
            regime_for_memory = None
            if day_candidates:
                regime_for_memory = (day_candidates[0][2].get('regime_info') or {}).get('regime')
            memory = get_strategy_memory(market, horizon, regime=regime_for_memory)
            for ticker, idx, snap in day_candidates:
                adj = strategy_memory_adjustment(snap, memory, regime=regime_for_memory)
                snap['strategy_memory_adjustment'] = adj
                snap['strategy_pattern_key'] = backtest_pattern_key(snap)
                if adj:
                    snap['final_pre_ai_score'] = round(float(snap.get('final_pre_ai_score', 0)) + adj, 2)
        # Strict Learning Mode makes the past simulation match the future policy:
        # do not force low-score/low-volume/weak-RR setups just to fill Top N.
        day_candidates.sort(key=lambda item: item[2].get('final_pre_ai_score', 0), reverse=True)
        if STRICT_LEARNING_ENABLED:
            strict_day_candidates = []
            for ticker, idx, snap in day_candidates:
                stock_mem_for_ticker = stock_memory_for_learning if learning_mode == 'learned' else None
                review = strict_learning_review(snap, market, horizon, engine='backtest', market_context=snap.get('regime_info'),
                                                stock_memory=stock_mem_for_ticker)
                snap['strict_learning'] = review
                snap['strict_learning_verdict'] = review.get('verdict')
                # For historical research we block true no-trade setups, but by default
                # still test watch-only setups so the system has enough examples to learn from.
                # Live/paper automation remains stricter and can block these entries.
                verdict = review.get('verdict')
                if verdict == 'no_trade' or (verdict == 'watch_only' and not STRICT_BACKTEST_ALLOW_WATCH_ONLY):
                    label = strict_rejection_label_from_review(review)
                    bump_count(strict_rejection_counts, label)
                    if len(strict_rejections) < 80:
                        strict_rejections.append({'ticker': ticker, 'date': str(signal_ts.date()), 'reason': '; '.join((review.get('hard_blocks') or review.get('cautions') or [])[:2]), 'score': review.get('score_before'), 'label': label})
                    continue
                if verdict == 'watch_only':
                    strict_watch_only_tested += 1
                    snap['strict_learning_warning'] = '; '.join((review.get('cautions') or [])[:2])
                snap['strict_score_after'] = review.get('score_after')
                strict_day_candidates.append((ticker, idx, snap))
            if day_candidates and not strict_day_candidates:
                strict_empty_test_days += 1
            strict_day_candidates.sort(key=lambda item: item[2].get('strict_score_after', item[2].get('final_pre_ai_score', 0)), reverse=True)
            day_candidates = strict_day_candidates
        for ticker, idx, snap in day_candidates[:top_n]:
            ticker_hist = all_hist[ticker]
            hold_sessions = 1 if horizon == 'day' else 5
            exit_limit_idx = min(idx + 1 + hold_sessions, len(ticker_hist) - 1)
            ticker_splits = all_split_dates.get(ticker) or set()
            if ticker_splits:
                window_dates = {
                    ticker_hist.index[j].date() if hasattr(ticker_hist.index[j], 'date') else ticker_hist.index[j]
                    for j in range(idx + 1, min(exit_limit_idx + 1, len(ticker_hist)))
                }
                if window_dates & ticker_splits:
                    skipped_for_corporate_action += 1
                    continue
            trade = simulate_backtest_trade(ticker, ticker_hist, idx, snap, horizon)
            if trade:
                trade['market'] = market
                trade['horizon'] = horizon
                trade['strict_learning_verdict'] = snap.get('strict_learning_verdict')
                trade['strict_learning_warning'] = snap.get('strict_learning_warning')
                if snap.get('strict_learning_warning'):
                    trade['failure_reason'] = trade.get('failure_reason') or 'strict_warning_setup'
                    trade['lesson_summary'] = (trade.get('lesson_summary') or '') + (' Strict note: ' + str(snap.get('strict_learning_warning'))[:220])
                trades.append(trade)
    metrics = compute_backtest_metrics(trades)
    metrics.update(compute_split_and_benchmark_metrics(trades, all_hist, start_dt, end_dt))
    metrics.update({'start_date': start_dt.isoformat(), 'end_date': end_dt.isoformat(), 'universe_size_requested': universe_limit, 'universe_size_with_data': len(all_hist), 'top_n': top_n, 'rebalance_step_days': step, 'source': source_info, 'lookahead_policy': 'No future bars used for signal creation; entry is next session open; free historical mode excludes non-point-in-time fundamentals/news. Current macro/news risk is used only for forward scans/trading, not old-date backtests.', 'partial': time.monotonic() > deadline_ts, 'runtime_limit_seconds': BACKTEST_MAX_RUNTIME_SECONDS, 'skipped_errors': skipped_errors, 'skipped_symbols': skipped_symbols, 'skipped_quality': skipped_quality, 'skipped_for_corporate_action': skipped_for_corporate_action, 'quality_passed_sample': quality_passed, 'quality_filter': {'enabled': QUALITY_STOCK_FILTER_ENABLED, 'thresholds': quality_thresholds_for_market(market), 'policy': 'Backtests include only active, liquid, tradeable symbols with usable price and volume history.'}, 'data_integrity_guard': {'enabled': BACKTEST_DATA_INTEGRITY_ENABLED, 'price_mode': get_backtest_price_mode_info(), 'thresholds': data_integrity_thresholds(), 'policy': 'Backtests store the selected raw/adjusted price mode and block suspicious candle data before learning from it. Confirmed stock splits/bonus issues are detected via yfinance corporate-action data and excluded both from the integrity penalty and from any trade whose holding window would straddle the split, so a real split cannot masquerade as a fake stop-loss/target hit.'}, 'exit_aware_learning_policy': backtest_exit_learning_policy(), 'duplicate_protection': {'enabled': BACKTEST_DUPLICATE_PROTECTION, 'duplicate_found': False, 'fingerprint_hash': fingerprint_hash, 'learning_applied': True, 'plain_summary': 'Exact repeated backtests are protected from double-counting. Same market, dates, universe, filters, and settings will reuse the old result instead of inflating learning.'}, 'stock_selection_policy': {'quality_universe_step': 'First choose the requested market universe, then keep only active/liquid stocks with enough clean price and volume history.', 'daily_pick_step': 'On each old test date, every quality stock is scored using trend, RSI, MACD, volume, risk/reward, market type, pattern memory, and stock-specific history. The top ranked setups are tested only if strict filters allow them.', 'not_random': True}, 'known_symbol_aliases': KNOWN_TICKER_ALIASES, 'strict_learning': {'enabled': STRICT_LEARNING_ENABLED, 'mode': STRICT_BACKTEST_RELAXED_LABEL, 'policy': 'balanced research mode: block true no-trade setups, but still test watch-only setups so the system has enough history to learn from; live automation stays stricter', 'rejected_setups_sample': strict_rejections, 'rejection_counts': strict_rejection_counts, 'empty_test_days': strict_empty_test_days, 'watch_only_setups_tested': strict_watch_only_tested, 'allow_watch_only_in_backtest': STRICT_BACKTEST_ALLOW_WATCH_ONLY, 'architecture': strict_learning_architecture()}})
    metrics['learning_mode'] = learning_mode
    metrics['learning_mode_note'] = (
        'Raw mode replays the base strategy exactly as originally coded, ignoring any pattern/regime memory learned since.'
        if learning_mode == 'raw' else
        'Learned mode applies the pattern-memory score adjustments and per-stock historical strength learned from past backtests before ranking/filtering each test day.'
    )
    run_id = persist_backtest_result(market, horizon, start_dt, end_dt, universe_limit, top_n, step, trades, metrics, started, fingerprint_hash=fingerprint_hash, fingerprint_payload=fingerprint_payload, learning_applied=commit_learning)
    metrics['commit_learning'] = commit_learning
    metrics['commit_learning_note'] = (
        'This run WROTE its results into strategy_signal_stats and will influence future learned-mode runs.'
        if commit_learning else
        'This run did NOT write into strategy_signal_stats (research/comparison mode). It reflects the memory state as of when it started, and it will not change what any other run sees.'
    )
    metrics['run_id'] = run_id
    metrics['recent_trades'] = trades[-60:][::-1]
    metrics['ok'] = True
    return metrics


def aggregate_stock_backtest_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    buckets: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        buckets.setdefault(r.get('ticker') or 'UNKNOWN', []).append(dict(r))
    stats = []
    for ticker, trades in buckets.items():
        rets = [float(t.get('net_return_pct') or 0) for t in trades]
        wins = [r for r in rets if r > 0]
        losses = [r for r in rets if r <= 0]
        outcomes, reasons, regimes = {}, {}, {}
        for t in trades:
            outcomes[t.get('outcome') or 'unknown'] = outcomes.get(t.get('outcome') or 'unknown', 0) + 1
            if t.get('failure_reason'):
                reasons[t.get('failure_reason')] = reasons.get(t.get('failure_reason'), 0) + 1
            regimes[t.get('regime') or 'unknown'] = regimes.get(t.get('regime') or 'unknown', 0) + 1
        top_reason = max(reasons.items(), key=lambda kv: kv[1])[0] if reasons else None
        stats.append({
            'ticker': ticker,
            'trades': len(rets),
            'win_rate_pct': round((len(wins) / len(rets)) * 100, 1) if rets else None,
            'expectancy_pct': round(float(np.mean(rets)), 3) if rets else None,
            'avg_win_pct': round(float(np.mean(wins)), 3) if wins else None,
            'avg_loss_pct': round(float(np.mean(losses)), 3) if losses else None,
            'best_trade_pct': round(max(rets), 3) if rets else None,
            'worst_trade_pct': round(min(rets), 3) if rets else None,
            'target_hits': outcomes.get('target_hit', 0),
            'stop_hits': outcomes.get('stop_loss_hit', 0),
            'horizon_exits': outcomes.get('horizon_exit', 0),
            'top_failure_reason': top_reason,
            'dominant_regime': max(regimes.items(), key=lambda kv: kv[1])[0] if regimes else None,
            'last_signal_date': max([str(t.get('signal_date')) for t in trades if t.get('signal_date')] or ['']),
            'lesson': (f"Avoid or require stronger confirmation when {top_reason.replace('_', ' ')} repeats." if top_reason else 'No repeated failure pattern yet.'),
        })
    stats.sort(key=lambda x: ((x.get('trades') or 0), (x.get('expectancy_pct') or -999)), reverse=True)
    return stats[:BACKTEST_STOCK_STATS_LIMIT]


def _backtest_config_key(run: Dict[str, Any]) -> Optional[tuple]:
    """Identity of 'the same test, run again' -- same market/horizon/window/universe/
    top_n/rebalance/learning_mode. Deliberately ignores fingerprint_hash (which for
    'learned' mode changes every time memory changes) so a genuine rerun of the same
    experiment is still recognized as a rerun even though its hash differs."""
    cfg = run.get('config_json')
    if isinstance(cfg, str):
        try:
            cfg = json.loads(cfg)
        except Exception:
            cfg = {}
    fp = (cfg or {}).get('fingerprint_payload') or {}
    learning_mode = fp.get('learning_mode') or ('learned' if run.get('learning_mode') == 'learned' else 'raw')
    try:
        return (
            run.get('market'), run.get('horizon'),
            str(run.get('start_date')), str(run.get('end_date')),
            int(run.get('universe_limit') or 0), int(run.get('top_n') or 0),
            int(run.get('rebalance_step_days') or 0), str(learning_mode),
        )
    except Exception:
        return None


def _rerun_delta(prev: Dict[str, Any], cur: Dict[str, Any]) -> Dict[str, Any]:
    def d(key, decimals=3):
        a, b = prev.get(key), cur.get(key)
        if a is None or b is None:
            return None
        return round(float(b) - float(a), decimals)
    delta_pf = d('profit_factor', 3)
    delta_exp = d('expectancy_pct', 3)
    delta_win = d('win_rate_pct', 1)
    if delta_pf is None and delta_exp is None:
        verdict = 'incomplete_data'
    elif (delta_pf or 0) > 0.001 and (delta_exp or 0) >= 0:
        verdict = 'improved'
    elif (delta_pf or 0) < -0.001 and (delta_exp or 0) <= 0:
        verdict = 'worse'
    elif delta_pf == 0 and delta_exp == 0:
        verdict = 'no_change'
    else:
        verdict = 'mixed'
    return {
        'is_rerun': True,
        'previous_run_id': prev.get('id'),
        'delta_profit_factor': delta_pf,
        'delta_expectancy_pct': delta_exp,
        'delta_win_rate_pct': delta_win,
        'delta_sharpe': d('sharpe', 2),
        'delta_max_drawdown_pct': d('max_drawdown_pct', 2),
        'verdict': verdict,
    }


def attach_rerun_info(runs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Mutates and returns `runs`, tagging each one with `rerun_info` describing
    whether it's a fresh test or a rerun of an earlier identical config, and if so
    the stat deltas vs that earlier run. Also assigns a stable `display_number`
    (position in chronological/id order) so the UI can show a real, consistent
    backtest number instead of just a timestamp."""
    by_id_asc = sorted(runs, key=lambda r: int(r.get('id') or 0))
    last_seen_for_key: Dict[tuple, Dict[str, Any]] = {}
    number_for_id: Dict[int, int] = {}
    for i, r in enumerate(by_id_asc):
        number_for_id[int(r.get('id') or 0)] = i + 1
        key = _backtest_config_key(r)
        if key is not None and key in last_seen_for_key:
            r['rerun_info'] = _rerun_delta(last_seen_for_key[key], r)
        else:
            r['rerun_info'] = {'is_rerun': False}
        if key is not None:
            last_seen_for_key[key] = r
    for r in runs:
        r['display_number'] = number_for_id.get(int(r.get('id') or 0), r.get('id'))
    return runs


def find_previous_same_config_run(cur, run: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Find the most recent earlier backtest_runs row that matches `run`'s exact
    config (same identity used by attach_rerun_info), used to diff per-stock stats
    between an original run and its rerun."""
    key = _backtest_config_key(run)
    if key is None:
        return None
    cur.execute("""
        SELECT id, created_at, market, horizon, start_date, end_date, universe_limit, top_n,
               rebalance_step_days, total_trades, win_rate_pct, expectancy_pct, profit_factor,
               sharpe, sortino, max_drawdown_pct, config_json
        FROM backtest_runs
        WHERE market=%s AND horizon=%s AND start_date=%s AND end_date=%s
              AND universe_limit=%s AND top_n=%s AND rebalance_step_days=%s AND id < %s
        ORDER BY id DESC LIMIT 5
    """, (run.get('market'), run.get('horizon'), run.get('start_date'), run.get('end_date'),
          run.get('universe_limit'), run.get('top_n'), run.get('rebalance_step_days'), run.get('id')))
    for candidate in cur.fetchall():
        cand = dict(candidate)
        if _backtest_config_key(cand) == key:
            return cand
    return None


def get_research_dashboard(limit: int = 20) -> Dict[str, Any]:
    conn = get_db_connection()
    if conn is None:
        return {'error': 'DATABASE_URL not set; research history disabled.', 'latest': [], 'strategy_memory': [], 'per_stock': [], 'recent_lessons': []}
    runs, memory, trade_rows, lessons = [], [], [], []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Pull a larger pool than `limit` so rerun-chain detection (attach_rerun_info)
            # can find an earlier run even if it has scrolled past the displayed page;
            # only the first `limit` rows are actually shown.
            rerun_pool_limit = max(int(limit), 200)
            cur.execute("""
                SELECT id, created_at, market, horizon, start_date, end_date, universe_limit, top_n,
                       rebalance_step_days, total_trades, win_rate_pct, expectancy_pct, avg_trade_pct, profit_factor,
                       sharpe, sortino, max_drawdown_pct, best_regime, worst_regime, status, error, metrics_json,
                       config_json, fingerprint_hash, duplicate_of_run_id, learning_applied
                FROM backtest_runs
                ORDER BY created_at DESC
                LIMIT %s
            """, (rerun_pool_limit,))
            runs = cur.fetchall()
            latest_run_id = runs[0]['id'] if runs else None
            if latest_run_id:
                cur.execute("""
                    SELECT ticker, market, horizon, signal_date, entry_date, exit_date, net_return_pct, outcome,
                           regime, score, failure_reason, lesson_summary, pattern_key, pattern_verdict
                    FROM backtest_trades
                    WHERE run_id=%s
                    ORDER BY signal_date DESC
                    LIMIT 5000
                """, (latest_run_id,))
                trade_rows = cur.fetchall()
            cur.execute("""
                SELECT market, horizon, regime, pattern_key, trades, win_rate_pct, expectancy_pct, confidence_adjustment, notes, updated_at
                FROM strategy_signal_stats
                ORDER BY updated_at DESC, trades DESC
                LIMIT 60
            """)
            memory = cur.fetchall()
            cur.execute("""
                SELECT created_at, market, horizon, ticker, signal, outcome_label, outcome_status,
                       failure_reason, lesson_summary, actual_gain_pct
                FROM prediction_lessons
                ORDER BY created_at DESC
                LIMIT 40
            """)
            lessons = cur.fetchall()
    finally:
        conn.close()
    per_stock = aggregate_stock_backtest_rows([dict(r) for r in trade_rows])
    all_runs = attach_rerun_info([dict(r) for r in runs])
    return {
        'latest': all_runs[:limit],
        'strategy_memory': [dict(r) for r in memory],
        'per_stock': per_stock,
        'recent_lessons': [dict(r) for r in lessons],
        'config': {
            'backtest_enabled': BACKTEST_ENABLED,
            'default_lookback_years': BACKTEST_DEFAULT_LOOKBACK_YEARS,
            'default_universe_limit': BACKTEST_UNIVERSE_LIMIT,
            'default_top_n': BACKTEST_TOP_N,
            'rebalance_step_days': BACKTEST_REBALANCE_STEP_DAYS,
            'stock_stats_limit': BACKTEST_STOCK_STATS_LIMIT,
            'execution_costs_bps': EXECUTION_SLIPPAGE_BPS + EXECUTION_SPREAD_BPS + EXECUTION_FEE_BPS,
            'uses_claude': False,
            'quality_stock_filter': {'enabled': QUALITY_STOCK_FILTER_ENABLED, 'thresholds': quality_thresholds_for_market('IN'), 'plain_summary': 'Backtests skip inactive, illiquid, unavailable, renamed, or low-turnover stocks so the system learns only from tradeable quality symbols.'},
            'duplicate_learning_protection': {'enabled': BACKTEST_DUPLICATE_PROTECTION, 'allow_duplicate_learning': BACKTEST_ALLOW_DUPLICATE_LEARNING, 'plain_summary': 'If the exact same backtest was already learned from, the app shows the existing result and does not count the same trades again.'},
            'stock_selection_policy': {'plain_summary': 'Quality stocks are chosen by tradeability first: clean data, active symbol, enough volume, enough turnover, and enough history. Best picks per test day are then ranked by bullish setup, volume confirmation, risk/reward, market type, past pattern behavior, and stock-specific behavior.'},
            'macro_risk_layer': {'enabled': MACRO_RISK_ENABLED, 'plain_summary': 'Forward scans/trading watch war, trade deals, tariffs, rates, oil shocks, regulation and other macro/news risks. Historical backtests do not use today’s news.'},
            'data_integrity_guard': {'enabled': BACKTEST_DATA_INTEGRITY_ENABLED, 'price_mode': get_backtest_price_mode_info(), 'thresholds': data_integrity_thresholds(), 'plain_summary': 'Backtests record whether prices are raw or adjusted and block suspicious candle data before it can teach the learning engine.'},
            'exit_aware_learning_policy': backtest_exit_learning_policy(),
            'learning_policy': 'failed predictions and failed backtest patterns are converted into pattern memory; future scans penalize repeated weak setups and reward historically durable setups',
            'strict_learning_mode': strict_learning_architecture(),
            'lookahead_policy': 'uses bars available before each simulated signal date; free historical mode excludes non-point-in-time fundamentals/news to avoid lookahead bias'
        }
    }

def ai_rank_candidates(candidates: List[Dict[str, Any]], market: str, horizon: str, market_context: Optional[Dict[str, Any]], learning_context: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], str]:
    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        return deterministic_rank_candidates(candidates, market, horizon), 'deterministic-fallback-no-api-key'

    estimated_input_tokens = SCAN_ESTIMATED_INPUT_TOKENS_BASE + min(len(candidates), DEEP_ANALYSIS_LIMIT) * SCAN_ESTIMATED_INPUT_TOKENS_PER_CANDIDATE
    estimated_cost = calculate_claude_cost(estimated_input_tokens, SCAN_ESTIMATED_OUTPUT_TOKENS)
    budget = get_ai_budget_status(estimated_cost.get('cost_inr', 0))
    if not budget.get('allowed'):
        print(f"[AI-BUDGET] Claude ranking skipped for {market}/{horizon}: {budget.get('reasons')}. Using deterministic ranker.")
        ranked = deterministic_rank_candidates(candidates, market, horizon)
        for item in ranked:
            item['ai_budget_blocked'] = True
            item['ai_budget_reasons'] = budget.get('reasons')
        return ranked, 'deterministic-fallback-ai-budget-guard'

    horizon_label = 'next 1 trading day' if horizon == 'day' else 'next 5 trading days (1 week)'
    today = datetime.date.today().strftime('%A, %B %d, %Y')
    compact_candidates = candidates[:DEEP_ANALYSIS_LIMIT]

    prompt = f"""You are ranking public stock opportunities for an educational market-scanning app. Today is {today}.

Use transparent, publicly documented analysis methods only. Do not claim to follow, copy, reverse-engineer, or replicate any specific certified financial adviser's proprietary methodology. Do not present the output as financial advice.

Market: {market}
Horizon: {horizon_label}
Market volatility context: {json.dumps(market_context or {}, default=_json_safe)}
Learning context from prior verified predictions: {json.dumps(learning_context or {}, default=_json_safe)}

The app has already applied a multi-stage funnel: liquidity, momentum, technical screening, fundamentals, and news sentiment. Rank the best opportunities from these deep-analysis candidates.

Framework to apply:
- Technicals: RSI, MACD, MA20/MA50/MA200 alignment, Bollinger Band position, volume confirmation, 20-day support/resistance, 52-week context.
- Fundamentals: revenue growth, EPS/earnings growth, ROE, debt-to-equity, operating/free cash flow, profit margin, valuation where available.
- News: headline sentiment and risk catalysts. Negative legal/regulatory/guidance headlines must reduce confidence.
- Risk management: set entry near current price, stop-loss around support/volatility, target near resistance or risk/reward extension.
- Learning loop: if prior verified predictions show repeated failure patterns, reduce confidence for similar setups and mention the adjustment briefly.

Return ONLY a valid JSON array of exactly {FINAL_PICK_LIMIT} items. No markdown. Each item must have:
rank, ticker, company, sector, current_price, signal, predicted_gain, gain_number, entry_price, target_exit_pct, stop_loss_pct, target_price, stop_price, risk_reward, confidence, rsi_reading, macd_reading, volume_reading, fundamental_reading, news_reading, horizon_tags, reasoning.

Rules:
- signal must be BUY, WATCH, or AVOID. Use WATCH instead of HOLD because users may not own the stock yet.
- confidence is an integer 0-100.
- risk_reward must look like "2.1:1".
- reasoning must cite actual numbers from the provided data and be 2-3 concise sentences.
- Do not invent backtest win rates or non-provided facts.

CANDIDATES_JSON:
{json.dumps(compact_candidates, separators=(',', ':'), default=_json_safe)}
"""
    try:
        client = anthropic.Anthropic(api_key=api_key)
        model_name = os.environ.get('ANTHROPIC_MODEL', 'claude-sonnet-5')
        response = client.messages.create(
            model=model_name,
            max_tokens=5000,
            messages=[{'role': 'user', 'content': prompt}],
        )
        usage = getattr(response, 'usage', None)
        if usage:
            log_api_cost(
                'anthropic_ai_ranking', market, horizon, model_name,
                int(getattr(usage, 'input_tokens', 0) or 0),
                int(getattr(usage, 'output_tokens', 0) or 0),
                notes='Final AI ranking for locked market scan',
                metadata={'candidate_count': len(compact_candidates)}
            )
        text_blocks = [b.text for b in response.content if getattr(b, 'type', None) == 'text']
        if not text_blocks:
            raise RuntimeError(f'Claude returned no text content (stop_reason={response.stop_reason}).')
        text = re.sub(r'```json|```', '', text_blocks[0].strip()).strip()
        try:
            picks = json.loads(text)
        except json.JSONDecodeError:
            match = re.search(r'\[[\s\S]*\]', text)
            if not match:
                raise
            picks = json.loads(match.group())
        return validate_ranked_picks(picks, candidates, horizon), model_name
    except Exception as e:
        print(f"[WARN] AI ranking failed; using deterministic fallback: {e}")
        return deterministic_rank_candidates(candidates, market, horizon), 'deterministic-fallback-ai-error'


def normalize_picks(picks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized = []
    for i, p in enumerate((picks or [])[:FINAL_PICK_LIMIT], start=1):
        try:
            p['rank'] = int(p.get('rank') or i)
        except Exception:
            p['rank'] = i
        p['signal'] = 'WATCH' if p.get('signal') == 'HOLD' else (p.get('signal') if p.get('signal') in ['BUY', 'WATCH', 'AVOID'] else 'WATCH')
        try:
            p['confidence'] = int(float(p.get('confidence', 50)))
        except Exception:
            p['confidence'] = 50
        p['confidence'] = max(min(p['confidence'], 100), 0)
        for k in ['current_price', 'entry_price', 'target_price', 'stop_price', 'gain_number']:
            if p.get(k) is not None:
                try:
                    p[k] = round(float(p[k]), 2)
                except Exception:
                    pass
        if not p.get('entry_price') and p.get('current_price'):
            p['entry_price'] = p['current_price']
        normalized.append(p)
    return normalized


def _risk_reward_number(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).replace('x', '').strip()
    try:
        return float(text.split(':')[0])
    except Exception:
        return None


def validate_ranked_picks(picks: List[Dict[str, Any]], candidates: List[Dict[str, Any]], horizon: str) -> List[Dict[str, Any]]:
    """Keep AI output grounded in the candidate data and risk controls.

    The AI can format/rank, but the app keeps prices, stop/target sanity, and
    BUY/WATCH/AVOID gates deterministic. This makes the final predictions more
    rigorous and prevents hallucinated tickers or unrealistic risk/reward.
    """
    candidate_map = {str(c.get('ticker', '')).upper(): c for c in candidates}
    normalized = normalize_picks(picks)
    cleaned: List[Dict[str, Any]] = []
    seen = set()

    for p in normalized:
        ticker = str(p.get('ticker', '')).upper()
        c = candidate_map.get(ticker)
        if not ticker or not c or ticker in seen:
            continue
        seen.add(ticker)
        current = float(c.get('current_price') or p.get('current_price') or 0)
        if current <= 0:
            continue

        p['ticker'] = ticker
        p['company'] = p.get('company') or c.get('company') or ticker
        p['sector'] = p.get('sector') or c.get('sector') or 'N/A'
        p['current_price'] = round(current, 2)
        currency = get_currency_info('IN' if ticker.endswith('.NS') else '', ticker)
        p['currency'] = currency['currency']
        p['currency_symbol'] = currency['symbol']

        # Entry must stay close to the current scan price. If the AI drifts too
        # far, use the observed market price from the scan.
        try:
            entry = float(p.get('entry_price') or current)
        except Exception:
            entry = current
        if abs(entry - current) / current > 0.03:
            entry = current
        p['entry_price'] = round(entry, 2)

        support = float(c.get('support_20') or current * 0.97)
        resistance = float(c.get('resistance_20') or current * 1.04)
        try:
            stop_price = float(p.get('stop_price') or support)
        except Exception:
            stop_price = support
        if stop_price >= entry or (entry - stop_price) / entry > 0.10:
            stop_price = min(support, entry * 0.97)
        stop_price = round(max(stop_price, 0.01), 2)
        p['stop_price'] = stop_price

        try:
            target_price = float(p.get('target_price') or resistance)
        except Exception:
            target_price = resistance
        if target_price <= entry:
            target_price = max(resistance, entry * (1.025 if horizon == 'day' else 1.05))
        target_price = round(target_price, 2)
        p['target_price'] = target_price

        risk_pct = max(((entry - stop_price) / entry) * 100, 0.01)
        reward_pct = max(((target_price - entry) / entry) * 100, 0.0)
        rr = round(reward_pct / risk_pct, 2) if risk_pct else 0.0
        p['risk_reward'] = f"{rr}:1"
        p['gain_number'] = round(reward_pct, 2)
        p['predicted_gain'] = f"+{round(reward_pct, 1)}%"
        p['target_exit_pct'] = f"+{round(reward_pct, 1)}%"
        p['stop_loss_pct'] = f"-{round(risk_pct, 1)}%"

        confidence = int(max(min(p.get('confidence', 50), 100), 0))
        # Guardrail: a BUY needs minimum confidence and risk/reward. Otherwise
        # the setup remains visible but becomes WATCH/AVOID.
        if p.get('signal') == 'BUY' and (confidence < MIN_BUY_CONFIDENCE or rr < MIN_BUY_RISK_REWARD):
            p['signal'] = 'WATCH'
        if c.get('news_score', 0) <= -16 and confidence < 72:
            p['signal'] = 'AVOID'
            p['confidence'] = min(confidence, 60)
        else:
            p['confidence'] = confidence

        p['features_json'] = {
            'rsi_14': c.get('rsi_14'),
            'macd_histogram': c.get('macd_histogram'),
            'volume_ratio_vs_20d_avg': c.get('volume_ratio_vs_20d_avg'),
            'price_vs_ma20_pct': c.get('price_vs_ma20_pct'),
            'support_20': c.get('support_20'),
            'resistance_20': c.get('resistance_20'),
            'news_score': c.get('news_score'),
            'learning_penalty': c.get('learning_penalty', 0),
            'learning_patterns_considered': c.get('learning_patterns_considered', {}),
        }
        p['analysis_quality_flags'] = {
            'candidate_verified': True,
            'price_grounded_to_scan': True,
            'risk_reward_recomputed': True,
            'buy_requires_min_confidence': MIN_BUY_CONFIDENCE,
            'buy_requires_min_risk_reward': MIN_BUY_RISK_REWARD,
        }
        cleaned.append(p)

    # If AI output dropped items during validation, fill remaining slots using the
    # deterministic ranker so the UI still has enough grounded picks.
    if len(cleaned) < FINAL_PICK_LIMIT:
        fallback = deterministic_rank_candidates(candidates, '', horizon)
        for p in fallback:
            ticker = str(p.get('ticker', '')).upper()
            if ticker and ticker not in seen:
                seen.add(ticker)
                p['analysis_quality_flags'] = {'candidate_verified': True, 'source': 'deterministic_fill'}
                cleaned.append(p)
            if len(cleaned) >= FINAL_PICK_LIMIT:
                break

    for i, p in enumerate(cleaned[:FINAL_PICK_LIMIT], start=1):
        p['rank'] = i
    return cleaned[:FINAL_PICK_LIMIT]


def get_methodology(funnel_counts: Dict[str, int]) -> Dict[str, Any]:
    return {
        'engine': 'precomputed_background_scan',
        'cache_ttl_minutes': CACHE_TTL_MINUTES,
        'prediction_lock_enabled': PREDICTION_LOCK_ENABLED,
        'lock_policy': 'Daily predictions lock to one result per calendar day; weekly predictions lock to one result per ISO week unless an explicit force scan is run.',
        'accuracy_policy': 'On user scan requests, due unchecked predictions are scored once and then marked checked so they are not scored repeatedly.',
        'funnel_counts': funnel_counts,
        'stages': [
            {'stage': 'Liquidity', 'method': 'Keep active/liquid symbols using 20-day average share volume and 20-day average traded value.'},
            {'stage': 'Momentum', 'method': 'Score 1-week, 1-month and 3-month price change, price vs moving averages, MACD histogram, RSI zone and volume expansion.'},
            {'stage': 'Technical screening', 'method': 'Use RSI, MACD, MA20/MA50/MA200 alignment, Bollinger Band position, support/resistance, volume confirmation and 52-week context.'},
            {'stage': 'Fundamentals', 'method': 'Evaluate revenue growth, EPS/earnings growth, ROE, debt-to-equity, cash flow, margins and valuation where available.'},
            {'stage': 'News sentiment', 'method': 'Apply transparent headline keyword sentiment and downgrade legal/regulatory/guidance risk.'},
            {'stage': 'AI ranking', 'method': 'Only the narrowed 50-100 candidate set is sent to the AI ranker for final reasoning, confidence and risk/reward formatting.'},
            {'stage': 'Post-AI validation', 'method': 'The app rejects hallucinated tickers, grounds prices to the scan, recomputes risk/reward, and downgrades weak BUY signals to WATCH/AVOID.'},
            {'stage': 'Strict Learning Mode', 'method': 'Apply minimum score, reward/risk, volume, regime, pattern-memory and stock-history filters. If edge is weak, show WATCH/AVOID/no-trade instead of forcing a BUY.'},
        ],
        'proprietary_methodology_notice': 'This app uses public, documented technical/fundamental/news frameworks and does not claim to copy any certified financial adviser methodology.',
    }


def get_check_date(market: str, horizon: str, created_at_utc: Optional[datetime.datetime] = None):
    return get_prediction_check_datetime(market, horizon, created_at_utc)


def save_predictions(picks, market, horizon, check_date, prediction_date=None):
    conn = get_db_connection()
    if conn is None:
        return
    prediction_date = prediction_date or datetime.date.today()
    try:
        with conn:
            with conn.cursor() as cur:
                for p in picks:
                    try:
                        cur.execute("""
                            INSERT INTO predictions
                                (prediction_date, market, horizon, ticker, company, rank, signal,
                                 entry_price, predicted_gain_pct, target_price, stop_price,
                                 confidence, reasoning, check_date, features_json)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                            ON CONFLICT (market, horizon, ticker, prediction_date) DO NOTHING
                        """, (
                            prediction_date, market, horizon, p.get('ticker'), p.get('company'), p.get('rank'),
                            'WATCH' if p.get('signal') == 'HOLD' else p.get('signal'),
                            p.get('entry_price') or p.get('current_price'), p.get('gain_number'),
                            p.get('target_price'), p.get('stop_price'), p.get('confidence'),
                            p.get('reasoning'), check_date, json.dumps(p.get('features_json') or {}, default=_json_safe)
                        ))
                    except Exception as e:
                        print(f"[WARN] Could not save prediction for {p.get('ticker')}: {e}")
    except Exception as e:
        print(f"[WARN] save_predictions failed: {e}")
    finally:
        conn.close()

def upsert_scan_cache(market: str, horizon: str, payload: Dict[str, Any], status: str = 'complete', error: Optional[str] = None):
    conn = get_db_connection()
    if conn is None:
        return
    counts = payload.get('funnel_counts', {})
    now = datetime.datetime.now()
    expires_at = now + datetime.timedelta(minutes=CACHE_TTL_MINUTES)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO stock_scan_cache
                        (market, horizon, status, started_at, completed_at, expires_at,
                         total_universe, total_liquid, total_momentum, total_technical, total_deep_candidates,
                         model, prediction_lock_key, locked, results_json, methodology_json, error, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s,%s)
                    ON CONFLICT (market, horizon) DO UPDATE SET
                        status=EXCLUDED.status,
                        started_at=EXCLUDED.started_at,
                        completed_at=EXCLUDED.completed_at,
                        expires_at=EXCLUDED.expires_at,
                        total_universe=EXCLUDED.total_universe,
                        total_liquid=EXCLUDED.total_liquid,
                        total_momentum=EXCLUDED.total_momentum,
                        total_technical=EXCLUDED.total_technical,
                        total_deep_candidates=EXCLUDED.total_deep_candidates,
                        model=EXCLUDED.model,
                        prediction_lock_key=EXCLUDED.prediction_lock_key,
                        locked=EXCLUDED.locked,
                        results_json=EXCLUDED.results_json,
                        methodology_json=EXCLUDED.methodology_json,
                        error=EXCLUDED.error,
                        updated_at=EXCLUDED.updated_at
                """, (
                    market, horizon, status, payload.get('started_at'), payload.get('completed_at'), expires_at,
                    counts.get('total_universe', 0), counts.get('liquidity_pass', 0), counts.get('momentum_pass', 0),
                    counts.get('technical_pass', 0), counts.get('deep_analysis', 0), payload.get('model'),
                    payload.get('prediction_lock_key'), bool(payload.get('locked', True)),
                    json.dumps(payload, default=_json_safe), json.dumps(payload.get('methodology', {}), default=_json_safe), error, now
                ))
    finally:
        conn.close()


def insert_scan_run(market: str, horizon: str, status: str, started_at, completed_at=None, counts=None, error=None):
    conn = get_db_connection()
    if conn is None:
        return
    counts = counts or {}
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO stock_scan_runs
                        (market, horizon, started_at, completed_at, status, total_universe, total_liquid,
                         total_momentum, total_technical, total_deep_candidates, error)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    market, horizon, started_at, completed_at, status,
                    counts.get('total_universe', 0), counts.get('liquidity_pass', 0), counts.get('momentum_pass', 0),
                    counts.get('technical_pass', 0), counts.get('deep_analysis', 0), error
                ))
    finally:
        conn.close()


def get_locked_cached_scan(market: str, horizon: str) -> Optional[Dict[str, Any]]:
    if not PREDICTION_LOCK_ENABLED:
        return None
    cached = get_cached_scan(market, horizon)
    if cached and payload_matches_current_lock(cached, horizon):
        cached['locked'] = True
        cached['lock_reused'] = True
        cached['prediction_lock_key'] = cached.get('prediction_lock_key') or get_prediction_lock_key(horizon)
        cached['lock_message'] = 'Prediction is already locked for the current period, so no new paid scan was run.'
        return cached
    return None


def run_market_scan(market: str, horizon: str, force: bool = False) -> Dict[str, Any]:
    started_at = utc_now_naive()
    if not force:
        locked = get_locked_cached_scan(market, horizon)
        if locked:
            print(f"[SCAN] Reusing locked prediction for market={market}, horizon={horizon}, lock={locked.get('prediction_lock_key')}")
            return locked
    if ACCURACY_CHECK_ON_SCAN:
        run_accuracy_check(market=market, horizon=horizon, triggered_by='background-scan')
    print(f"[SCAN] Starting background scan for market={market}, horizon={horizon}, force={force}")
    universe = load_market_universe(market)
    snapshots = download_universe_snapshots(universe)
    liquid = liquidity_filter(snapshots, market)
    momentum = apply_momentum_stage(liquid)
    technical = apply_technical_stage(momentum)
    deep = enrich_deep_candidates(technical)
    learning_context = get_learning_context(market, horizon)
    deep = apply_learning_adjustments(deep, learning_context)
    market_context = get_market_context(market)
    deep = apply_strategy_memory_adjustments(deep, market, horizon, market_context)
    strategy_memory = get_strategy_memory(market, horizon, (market_context or {}).get('regime') if isinstance(market_context, dict) else None)
    combined_learning_context = dict(learning_context or {})
    combined_learning_context['strategy_memory'] = strategy_memory
    picks, model = ai_rank_candidates(deep, market, horizon, market_context, combined_learning_context)
    strict_result = apply_strict_learning_to_picks(picks, deep, market, horizon, market_context)
    picks = strict_result.get('picks', picks)

    check_date = get_check_date(market, horizon, started_at)
    currency = get_currency_info(market)
    # Show the exchange-local verification/exit date to users. The stored check_date is UTC.
    sched = get_market_schedule(market)
    check_local = check_date.replace(tzinfo=datetime.timezone.utc).astimezone(ZoneInfo(sched['tz']))
    exit_date_label = check_local.strftime('%a, %b %d, %I:%M %p')
    for p in picks:
        p['exit_date'] = check_date.isoformat()
        p['exit_date_label'] = exit_date_label
        p['currency'] = p.get('currency') or currency['currency']
        p['currency_symbol'] = p.get('currency_symbol') or currency['symbol']

    counts = {
        'total_universe': len(universe),
        'price_snapshots': len(snapshots),
        'liquidity_pass': len(liquid),
        'momentum_pass': len(momentum),
        'technical_pass': len(technical),
        'deep_analysis': len(deep),
        'final_picks': len(picks),
    }
    methodology = get_methodology(counts)
    methodology['learning_context'] = {'patterns': learning_context.get('patterns', {}), 'lessons_considered': len(learning_context.get('lessons', []))}
    methodology['strategy_memory'] = {'patterns_considered': len(strategy_memory.get('patterns', [])), 'regime': (market_context or {}).get('regime') if isinstance(market_context, dict) else None}
    methodology['execution_policy'] = get_execution_policy()
    methodology['strict_learning_mode'] = strict_result.get('summary')
    methodology['learning_architecture'] = strict_learning_architecture()
    completed_at = utc_now_naive()
    payload = {
        'picks': picks,
        'total_scanned': len(universe),
        'market': market,
        'horizon': horizon,
        'prediction_date': completed_at.date().isoformat(),
        'prediction_lock_key': get_prediction_lock_key(horizon, completed_at),
        'locked': True,
        'lock_policy': 'one prediction per market/horizon lock period unless force scan is explicitly used',
        'timestamp': completed_at.isoformat(),
        'cached': True,
        'stale': False,
        'cache_ttl_minutes': CACHE_TTL_MINUTES,
        'funnel_counts': counts,
        'market_context': market_context,
        'methodology': methodology,
        'model': model,
        'learning_context': methodology.get('learning_context'),
        'strict_learning_mode': strict_result.get('summary'),
        'no_trade_recommendation': bool((strict_result.get('summary') or {}).get('no_trade_recommendation')),
        'execution_policy': get_execution_policy(),
        'ai_budget': get_ai_budget_status(),
        'cost_estimate_next_scan': estimate_next_scan_cost(market, horizon),
        'started_at': started_at.isoformat(),
        'completed_at': completed_at.isoformat(),
    }

    upsert_scan_cache(market, horizon, payload)
    insert_scan_run(market, horizon, 'complete', started_at, completed_at, counts)
    save_predictions(picks, market, horizon, check_date, completed_at.date())
    print(f"[SCAN] Completed {market}/{horizon}: {counts}")
    return payload


def get_cached_scan(market: str, horizon: str) -> Optional[Dict[str, Any]]:
    conn = get_db_connection()
    if conn is None:
        return None
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT results_json, updated_at, expires_at, status, error, prediction_lock_key, locked
                FROM stock_scan_cache
                WHERE market=%s AND horizon=%s
                LIMIT 1
            """, (market, horizon))
            row = cur.fetchone()
            if not row or not row.get('results_json'):
                return None
            payload = row['results_json']
            if isinstance(payload, str):
                payload = json.loads(payload)
            now = datetime.datetime.now(row['expires_at'].tzinfo) if row.get('expires_at') and row['expires_at'].tzinfo else datetime.datetime.now()
            expires_at = row.get('expires_at')
            stale = bool(expires_at and expires_at < now)
            updated_at = row.get('updated_at')
            payload['cached'] = True
            payload['stale'] = stale
            payload['cache_updated_at'] = updated_at.isoformat() if updated_at else payload.get('timestamp')
            if updated_at:
                delta = datetime.datetime.now(updated_at.tzinfo) - updated_at if updated_at.tzinfo else datetime.datetime.now() - updated_at
                payload['cache_age_minutes'] = round(delta.total_seconds() / 60, 1)
            payload['cache_status'] = row.get('status')
            if row.get('prediction_lock_key'):
                payload['prediction_lock_key'] = row.get('prediction_lock_key')
            payload['locked'] = bool(row.get('locked', True))
            if payload_matches_current_lock(payload, horizon):
                payload['lock_status'] = 'current_period_locked'
            else:
                payload['lock_status'] = 'previous_period_cache'
            if row.get('error'):
                payload['cache_error'] = row['error']
            return payload
    finally:
        conn.close()



def validate_market_horizon(market: str, horizon: str) -> Tuple[str, str]:
    market = market if market in DEFAULT_UNIVERSES else 'US'
    horizon = horizon if horizon in HORIZON_DAYS else 'day'
    return market, horizon


def get_scan_cache_status(market: str, horizon: str) -> Dict[str, Any]:
    """Return lightweight cache/job status without requiring a completed payload."""
    conn = get_db_connection()
    if conn is None:
        return {'status': 'disabled', 'error': 'DATABASE_URL not set'}
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT status, started_at, completed_at, updated_at, error, prediction_lock_key, locked, results_json
                FROM stock_scan_cache
                WHERE market=%s AND horizon=%s
                LIMIT 1
            """, (market, horizon))
            row = cur.fetchone()
            if not row:
                return {'status': 'empty', 'market': market, 'horizon': horizon}
            payload = row.get('results_json')
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except Exception:
                    payload = None
            has_current_lock = bool(payload_matches_current_lock(payload, horizon))
            now = datetime.datetime.now(row['updated_at'].tzinfo) if row.get('updated_at') and row['updated_at'].tzinfo else datetime.datetime.now()
            updated_at = row.get('updated_at')
            age_minutes = None
            if updated_at:
                age = now - updated_at if not updated_at.tzinfo else datetime.datetime.now(updated_at.tzinfo) - updated_at
                age_minutes = round(age.total_seconds() / 60, 1)
            return {
                'status': row.get('status') or 'empty',
                'market': market,
                'horizon': horizon,
                'started_at': row['started_at'].isoformat() if row.get('started_at') else None,
                'completed_at': row['completed_at'].isoformat() if row.get('completed_at') else None,
                'updated_at': row['updated_at'].isoformat() if row.get('updated_at') else None,
                'age_minutes': age_minutes,
                'error': row.get('error'),
                'prediction_lock_key': row.get('prediction_lock_key'),
                'locked': bool(row.get('locked', True)),
                'has_payload': payload is not None,
                'has_current_lock': has_current_lock,
                'running_is_stale': bool(row.get('status') == 'running' and age_minutes is not None and age_minutes > FIRST_SCAN_RUNNING_TIMEOUT_MINUTES),
            }
    finally:
        conn.close()


def mark_first_scan_running(market: str, horizon: str) -> Tuple[bool, str, Dict[str, Any]]:
    """Atomically mark a user-started scan as running when no current lock exists.

    Returns (can_start, reason, status). This prevents repeated paid scans from
    multiple user clicks while still allowing the very first/current-period scan.
    """
    conn = get_db_connection()
    if conn is None:
        return False, 'database_disabled', {'error': 'DATABASE_URL not set'}

    now = datetime.datetime.now()
    lock_key = get_prediction_lock_key(horizon, now)
    try:
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT status, updated_at, results_json, prediction_lock_key, error
                    FROM stock_scan_cache
                    WHERE market=%s AND horizon=%s
                    FOR UPDATE
                """, (market, horizon))
                row = cur.fetchone()
                if row:
                    payload = row.get('results_json')
                    if isinstance(payload, str):
                        try:
                            payload = json.loads(payload)
                        except Exception:
                            payload = None
                    if payload_matches_current_lock(payload, horizon):
                        return False, 'already_locked', {'status': 'complete', 'prediction_lock_key': payload.get('prediction_lock_key') if payload else lock_key}
                    updated_at = row.get('updated_at')
                    age_minutes = None
                    if updated_at:
                        ref_now = datetime.datetime.now(updated_at.tzinfo) if updated_at.tzinfo else datetime.datetime.now()
                        age_minutes = (ref_now - updated_at).total_seconds() / 60
                    if row.get('status') == 'running' and (age_minutes is None or age_minutes <= FIRST_SCAN_RUNNING_TIMEOUT_MINUTES):
                        return False, 'already_running', {'status': 'running', 'started_at': row.get('updated_at').isoformat() if row.get('updated_at') else None}

                cur.execute("""
                    INSERT INTO stock_scan_cache
                        (market, horizon, status, started_at, completed_at, expires_at,
                         prediction_lock_key, locked, results_json, methodology_json, error, updated_at)
                    VALUES (%s,%s,'running',%s,NULL,NULL,%s,TRUE,NULL,NULL,NULL,%s)
                    ON CONFLICT (market, horizon) DO UPDATE SET
                        status='running',
                        started_at=EXCLUDED.started_at,
                        completed_at=NULL,
                        error=NULL,
                        prediction_lock_key=EXCLUDED.prediction_lock_key,
                        locked=TRUE,
                        updated_at=EXCLUDED.updated_at
                """, (market, horizon, now, lock_key, now))
        return True, 'started', {'status': 'running', 'prediction_lock_key': lock_key, 'started_at': now.isoformat()}
    finally:
        conn.close()


def mark_first_scan_failed(market: str, horizon: str, error: str):
    conn = get_db_connection()
    if conn is None:
        return
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE stock_scan_cache
                    SET status='failed', error=%s, completed_at=NOW(), updated_at=NOW()
                    WHERE market=%s AND horizon=%s AND status='running'
                """, (error[:1000], market, horizon))
    finally:
        conn.close()


def _first_scan_worker(market: str, horizon: str):
    key = f"{market}:{horizon}"
    try:
        print(f"[FIRST-SCAN] User-started first/current-period scan running for {key}")
        run_market_scan(market, horizon, force=False)
    except Exception as e:
        import traceback
        traceback.print_exc()
        mark_first_scan_failed(market, horizon, str(e))
    finally:
        with FIRST_SCAN_THREADS_LOCK:
            FIRST_SCAN_THREADS.pop(key, None)


def start_first_scan_thread(market: str, horizon: str):
    key = f"{market}:{horizon}"
    with FIRST_SCAN_THREADS_LOCK:
        thread = FIRST_SCAN_THREADS.get(key)
        if thread and thread.is_alive():
            return False
        thread = threading.Thread(target=_first_scan_worker, args=(market, horizon), daemon=True)
        FIRST_SCAN_THREADS[key] = thread
        thread.start()
    return True

def score_outcome(signal, actual_gain_pct):
    # Backward-compatible fallback. Detailed scoring is handled by score_prediction_outcome.
    signal = 'WATCH' if signal == 'HOLD' else signal
    if signal == 'BUY':
        return actual_gain_pct > 0
    if signal == 'AVOID':
        return actual_gain_pct <= 0
    if signal == 'WATCH':
        return None
    return None


def fetch_prediction_price_window(ticker: str, market: str, scan_timestamp: datetime.datetime, check_date: datetime.datetime) -> Optional[Dict[str, float]]:
    """Fetch OHLC data for the prediction window.

    Accuracy uses high/low/close, not just the last visible price. yfinance is a
    free market-data fetch here; it does not call Claude and does not create a new
    prediction.
    """
    try:
        # Longer period gives a buffer for holidays and delayed EOD updates.
        hist = yf.Ticker(ticker).history(period='15d', timeout=YF_TIMEOUT)
        if hist is None or hist.empty:
            return None
        hist = hist.dropna(subset=['Close'])
        if hist.empty:
            return None

        # Prefer rows that fall inside the predicted horizon date window. Daily
        # yfinance bars are exchange-local dates, so date filtering is safer than
        # relying on exact intraday timestamps.
        sched = get_market_schedule(market)
        tz = ZoneInfo(sched['tz'])
        start_date = scan_timestamp.replace(tzinfo=datetime.timezone.utc).astimezone(tz).date() if scan_timestamp.tzinfo is None else scan_timestamp.astimezone(tz).date()
        end_date = check_date.replace(tzinfo=datetime.timezone.utc).astimezone(tz).date() if check_date.tzinfo is None else check_date.astimezone(tz).date()

        rows = hist
        try:
            idx_dates = []
            for idx in hist.index:
                if hasattr(idx, 'to_pydatetime'):
                    dt = idx.to_pydatetime()
                else:
                    dt = idx
                if getattr(dt, 'tzinfo', None):
                    idx_dates.append(dt.astimezone(tz).date())
                else:
                    idx_dates.append(dt.date())
            mask = [(d >= start_date and d <= end_date) for d in idx_dates]
            filtered = hist.loc[mask]
            if not filtered.empty:
                rows = filtered
        except Exception:
            rows = hist.tail(1)

        close_price = float(rows['Close'].dropna().iloc[-1])
        high_price = float(rows['High'].dropna().max()) if 'High' in rows else close_price
        low_price = float(rows['Low'].dropna().min()) if 'Low' in rows else close_price
        return {'close': round(close_price, 4), 'high': round(high_price, 4), 'low': round(low_price, 4)}
    except Exception as e:
        print(f"[WARN] Could not fetch prediction price window for {ticker}: {e}")
        return None


def score_prediction_outcome(row: Dict[str, Any], prices: Dict[str, float]) -> Dict[str, Any]:
    signal = 'WATCH' if row.get('signal') == 'HOLD' else row.get('signal')
    entry = float(row['entry_price']) if row.get('entry_price') is not None else None
    target = float(row['target_price']) if row.get('target_price') is not None else None
    stop = float(row['stop_price']) if row.get('stop_price') is not None else None
    close = float(prices['close'])
    high = float(prices.get('high', close))
    low = float(prices.get('low', close))
    actual_gain_pct = round(((close - entry) / entry) * 100, 2) if entry else None
    target_hit = bool(target and high >= target)
    stop_hit = bool(stop and low <= stop)

    if signal == 'BUY':
        if target_hit:
            label, status, correct, reason = 'Target Hit', 'success', True, None
        elif stop_hit:
            label, status, correct, reason = 'Stop Loss Hit', 'failed', False, 'stop_loss_hit'
        elif actual_gain_pct is not None and actual_gain_pct > 0:
            label, status, correct, reason = 'Partial Success', 'partial_success', True, None
        else:
            label, status, correct, reason = 'Closed Below Entry', 'failed', False, infer_failure_reason(row, actual_gain_pct, target_hit, stop_hit)
    elif signal == 'AVOID':
        if actual_gain_pct is not None and actual_gain_pct <= 0:
            label, status, correct, reason = 'Correct Avoid', 'success', True, None
        elif target_hit or (actual_gain_pct is not None and actual_gain_pct >= 2):
            label, status, correct, reason = 'Avoid Missed Rally', 'failed', False, 'missed_positive_move'
        else:
            label, status, correct, reason = 'Neutral Avoid', 'neutral', None, None
    else:  # WATCH
        if stop_hit or (actual_gain_pct is not None and actual_gain_pct <= -1.5):
            label, status, correct, reason = 'Correct Watch', 'success', True, None
        elif target_hit or (actual_gain_pct is not None and actual_gain_pct >= 2):
            label, status, correct, reason = 'Missed Opportunity', 'missed_opportunity', False, 'watch_rallied_without_buy'
        else:
            label, status, correct, reason = 'Neutral Watch', 'neutral', None, None

    lesson_summary = build_lesson_summary(row, label, status, reason, actual_gain_pct)
    return {
        'actual_price': close,
        'actual_high': high,
        'actual_low': low,
        'actual_gain_pct': actual_gain_pct,
        'outcome_correct': correct,
        'outcome_label': label,
        'outcome_status': status,
        'failure_reason': reason,
        'lesson_summary': lesson_summary,
    }


def infer_failure_reason(row: Dict[str, Any], actual_gain_pct: Optional[float], target_hit: bool, stop_hit: bool) -> str:
    features = row.get('features_json') or {}
    if isinstance(features, str):
        try:
            features = json.loads(features)
        except Exception:
            features = {}
    try:
        rr = None
        entry = float(row['entry_price']) if row.get('entry_price') is not None else None
        target = float(row['target_price']) if row.get('target_price') is not None else None
        stop = float(row['stop_price']) if row.get('stop_price') is not None else None
        if entry and target and stop and entry > stop:
            rr = (target - entry) / (entry - stop)
    except Exception:
        rr = None

    if stop_hit:
        return 'stop_loss_hit'
    if features.get('volume_ratio_vs_20d_avg') is not None and float(features.get('volume_ratio_vs_20d_avg') or 0) < 0.8:
        return 'low_volume_confirmation'
    if features.get('rsi_14') is not None and float(features.get('rsi_14') or 0) > 68:
        return 'overextended_or_chasing'
    if rr is not None and rr < MIN_BUY_RISK_REWARD:
        return 'weak_risk_reward'
    if features.get('news_score') is not None and float(features.get('news_score') or 0) < 0:
        return 'negative_news_or_event_risk'
    if features.get('news_score') is not None and float(features.get('news_score') or 0) == 0:
        return 'no_news_catalyst'
    return 'setup_did_not_follow_through'


def build_lesson_summary(row: Dict[str, Any], label: str, status: str, reason: Optional[str], actual_gain_pct: Optional[float]) -> str:
    signal = 'WATCH' if row.get('signal') == 'HOLD' else row.get('signal')
    ticker = row.get('ticker')
    if status == 'success':
        return f"{ticker} {signal} outcome worked as expected ({label}); keep similar confirmations when ranking future setups."
    if status == 'neutral':
        return f"{ticker} {signal} stayed neutral; do not count it as a strong win or loss when adjusting the model."
    reason_text = (reason or 'unknown').replace('_', ' ')
    return f"{ticker} {signal} failed/underperformed because of {reason_text}; reduce confidence for similar future setups unless stronger confirmation appears."


def save_prediction_lesson(row: Dict[str, Any], scored: Dict[str, Any]):
    conn = get_db_connection()
    if conn is None:
        return
    features = row.get('features_json') or {}
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO prediction_lessons
                        (prediction_id, market, horizon, ticker, signal, outcome_label, outcome_status,
                         failure_reason, lesson_summary, actual_gain_pct, features_json)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                    ON CONFLICT (prediction_id) WHERE (prediction_id IS NOT NULL) DO NOTHING
                """, (
                    row.get('id'), row.get('market'), row.get('horizon'), row.get('ticker'),
                    'WATCH' if row.get('signal') == 'HOLD' else row.get('signal'),
                    scored.get('outcome_label'), scored.get('outcome_status'), scored.get('failure_reason'),
                    scored.get('lesson_summary'), scored.get('actual_gain_pct'),
                    json.dumps(features, default=_json_safe)
                ))
    except Exception as e:
        print(f"[WARN] Could not save prediction lesson: {e}")
    finally:
        conn.close()


def get_trading_settings() -> Dict[str, Any]:
    defaults = {
        'enabled': False,
        'mode': AUTO_TRADING_MODE if AUTO_TRADING_MODE in ('paper', 'assisted', 'live') else 'paper',
        'require_confirmation': True,
        'broker': BROKER_NAME,
        'capital_inr': TRADING_DEFAULT_CAPITAL_INR,
        'capital_usd': TRADING_DEFAULT_CAPITAL_USD,
        'max_position_pct': TRADING_MAX_POSITION_PCT,
        'risk_per_trade_pct': TRADING_RISK_PER_TRADE_PCT,
        'daily_loss_limit_pct': TRADING_DAILY_LOSS_LIMIT_PCT,
        'max_open_positions': TRADING_MAX_OPEN_POSITIONS,
        'min_confidence': TRADING_MIN_CONFIDENCE,
        'min_risk_reward': TRADING_MIN_RISK_REWARD,
        'allow_us_with_inr_capital': TRADING_ALLOW_US_WITH_INR_CAPITAL,
        'weekly_profit_target_pct': TRADING_WEEKLY_PROFIT_TARGET_PCT,
        'max_weekly_loss_pct': TRADING_MAX_WEEKLY_LOSS_PCT,
        'cover_api_costs_from_profit': TRADING_COVER_API_COSTS_FROM_PROFIT,
        'stop_after_weekly_target': TRADING_STOP_AFTER_WEEKLY_TARGET,
        'stop_after_weekly_loss': TRADING_STOP_AFTER_WEEKLY_LOSS,
        'broker_api_monthly_cost_inr': TRADING_BROKER_API_MONTHLY_COST_INR,
        'opening_confirmation_enabled': OPENING_CONFIRMATION_ENABLED,
        'opening_wait_minutes': OPENING_CONFIRMATION_WAIT_MINUTES,
        'opening_max_entry_chase_pct': OPENING_MAX_ENTRY_CHASE_PCT,
        'opening_max_entry_pullback_pct': OPENING_MAX_ENTRY_PULLBACK_PCT,
        'opening_min_volume_multiplier': OPENING_MIN_VOLUME_MULTIPLIER,
        'profit_protection_enabled': PROFIT_PROTECTION_ENABLED,
        'profit_protect_progress_pct': PROFIT_PROTECT_PROGRESS_PCT,
        'trailing_stop_activation_pct': TRAILING_STOP_ACTIVATION_PCT,
        'trailing_stop_giveback_pct': TRAILING_STOP_GIVEBACK_PCT,
        'exit_at_horizon_end': EXIT_AT_HORIZON_END,
        'selection_mode': TRADING_SELECTION_MODE_DEFAULT if TRADING_SELECTION_MODE_DEFAULT in ('locked_scan', 'self_scan', 'auto') else 'locked_scan',
        'self_scan_universe_limit': TRADING_SELF_SCAN_UNIVERSE_LIMIT,
        'self_scan_min_score': TRADING_SELF_SCAN_MIN_SCORE,
    }
    conn = get_db_connection()
    if conn is None:
        return defaults
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM trading_settings WHERE id=1")
            row = cur.fetchone()
            if not row:
                return defaults
            return {
                'enabled': bool(row.get('enabled')),
                'mode': row.get('mode') or defaults['mode'],
                'require_confirmation': bool(row.get('require_confirmation')),
                'broker': row.get('broker') or defaults['broker'],
                'capital_inr': float(row.get('capital_inr') or defaults['capital_inr']),
                'capital_usd': float(row.get('capital_usd') or defaults['capital_usd']),
                'max_position_pct': float(row.get('max_position_pct') or defaults['max_position_pct']),
                'risk_per_trade_pct': float(row.get('risk_per_trade_pct') or defaults['risk_per_trade_pct']),
                'daily_loss_limit_pct': float(row.get('daily_loss_limit_pct') or defaults['daily_loss_limit_pct']),
                'max_open_positions': int(row.get('max_open_positions') or defaults['max_open_positions']),
                'min_confidence': int(row.get('min_confidence') or defaults['min_confidence']),
                'min_risk_reward': float(row.get('min_risk_reward') or defaults['min_risk_reward']),
                'allow_us_with_inr_capital': bool(row.get('allow_us_with_inr_capital')),
                'weekly_profit_target_pct': float(row.get('weekly_profit_target_pct') or defaults['weekly_profit_target_pct']),
                'max_weekly_loss_pct': float(row.get('max_weekly_loss_pct') or defaults['max_weekly_loss_pct']),
                'cover_api_costs_from_profit': bool(row.get('cover_api_costs_from_profit')),
                'stop_after_weekly_target': bool(row.get('stop_after_weekly_target')),
                'stop_after_weekly_loss': bool(row.get('stop_after_weekly_loss')),
                'broker_api_monthly_cost_inr': float(row.get('broker_api_monthly_cost_inr') or defaults['broker_api_monthly_cost_inr']),
                'opening_confirmation_enabled': bool(row.get('opening_confirmation_enabled')),
                'opening_wait_minutes': int(row.get('opening_wait_minutes') or defaults['opening_wait_minutes']),
                'opening_max_entry_chase_pct': float(row.get('opening_max_entry_chase_pct') or defaults['opening_max_entry_chase_pct']),
                'opening_max_entry_pullback_pct': float(row.get('opening_max_entry_pullback_pct') or defaults['opening_max_entry_pullback_pct']),
                'opening_min_volume_multiplier': float(row.get('opening_min_volume_multiplier') or defaults['opening_min_volume_multiplier']),
                'profit_protection_enabled': bool(row.get('profit_protection_enabled')),
                'profit_protect_progress_pct': float(row.get('profit_protect_progress_pct') or defaults['profit_protect_progress_pct']),
                'trailing_stop_activation_pct': float(row.get('trailing_stop_activation_pct') or defaults['trailing_stop_activation_pct']),
                'trailing_stop_giveback_pct': float(row.get('trailing_stop_giveback_pct') or defaults['trailing_stop_giveback_pct']),
                'exit_at_horizon_end': bool(row.get('exit_at_horizon_end')),
                'selection_mode': row.get('selection_mode') if row.get('selection_mode') in ('locked_scan', 'self_scan', 'auto') else defaults['selection_mode'],
                'self_scan_universe_limit': int(row.get('self_scan_universe_limit') or defaults['self_scan_universe_limit']),
                'self_scan_min_score': float(row.get('self_scan_min_score') or defaults['self_scan_min_score']),
            }
    finally:
        conn.close()


def update_trading_settings(data: Dict[str, Any]) -> Dict[str, Any]:
    mode = str(data.get('mode', 'paper')).lower()
    if mode not in ('paper', 'assisted', 'live'):
        mode = 'paper'
    selection_mode = str(data.get('selection_mode') or TRADING_SELECTION_MODE_DEFAULT or 'locked_scan').lower()
    if selection_mode not in ('locked_scan', 'self_scan', 'auto'):
        selection_mode = 'locked_scan'
    # Live mode stays blocked unless the server owner explicitly enables it.
    if mode == 'live' and not LIVE_TRADING_ENABLED:
        mode = 'assisted'
    settings = {
        'enabled': bool(data.get('enabled', False)),
        'mode': mode,
        'require_confirmation': True if mode != 'paper' else bool(data.get('require_confirmation', True)),
        'broker': str(data.get('broker') or BROKER_NAME)[:80],
        'capital_inr': max(0.0, float(data.get('capital_inr') or TRADING_DEFAULT_CAPITAL_INR)),
        'capital_usd': max(0.0, float(data.get('capital_usd') or TRADING_DEFAULT_CAPITAL_USD)),
        'max_position_pct': min(100.0, max(1.0, float(data.get('max_position_pct') or TRADING_MAX_POSITION_PCT))),
        'risk_per_trade_pct': min(20.0, max(0.1, float(data.get('risk_per_trade_pct') or TRADING_RISK_PER_TRADE_PCT))),
        'daily_loss_limit_pct': min(50.0, max(0.5, float(data.get('daily_loss_limit_pct') or TRADING_DAILY_LOSS_LIMIT_PCT))),
        'max_open_positions': min(20, max(1, int(data.get('max_open_positions') or TRADING_MAX_OPEN_POSITIONS))),
        'min_confidence': min(99, max(1, int(data.get('min_confidence') or TRADING_MIN_CONFIDENCE))),
        'min_risk_reward': min(10.0, max(0.1, float(data.get('min_risk_reward') or TRADING_MIN_RISK_REWARD))),
        'allow_us_with_inr_capital': bool(data.get('allow_us_with_inr_capital', False)),
        'weekly_profit_target_pct': min(100.0, max(0.0, float(data.get('weekly_profit_target_pct') or TRADING_WEEKLY_PROFIT_TARGET_PCT))),
        'max_weekly_loss_pct': min(50.0, max(0.5, float(data.get('max_weekly_loss_pct') or TRADING_MAX_WEEKLY_LOSS_PCT))),
        'cover_api_costs_from_profit': bool(data.get('cover_api_costs_from_profit', TRADING_COVER_API_COSTS_FROM_PROFIT)),
        'stop_after_weekly_target': bool(data.get('stop_after_weekly_target', TRADING_STOP_AFTER_WEEKLY_TARGET)),
        'stop_after_weekly_loss': bool(data.get('stop_after_weekly_loss', TRADING_STOP_AFTER_WEEKLY_LOSS)),
        'broker_api_monthly_cost_inr': min(100000.0, max(0.0, float(data.get('broker_api_monthly_cost_inr') or TRADING_BROKER_API_MONTHLY_COST_INR))),
        'opening_confirmation_enabled': bool(data.get('opening_confirmation_enabled', OPENING_CONFIRMATION_ENABLED)),
        'opening_wait_minutes': min(60, max(0, int(data.get('opening_wait_minutes') if data.get('opening_wait_minutes') is not None else OPENING_CONFIRMATION_WAIT_MINUTES))),
        'opening_max_entry_chase_pct': min(10.0, max(0.0, float(data.get('opening_max_entry_chase_pct') if data.get('opening_max_entry_chase_pct') is not None else OPENING_MAX_ENTRY_CHASE_PCT))),
        'opening_max_entry_pullback_pct': min(10.0, max(0.0, float(data.get('opening_max_entry_pullback_pct') if data.get('opening_max_entry_pullback_pct') is not None else OPENING_MAX_ENTRY_PULLBACK_PCT))),
        'opening_min_volume_multiplier': min(10.0, max(0.0, float(data.get('opening_min_volume_multiplier') if data.get('opening_min_volume_multiplier') is not None else OPENING_MIN_VOLUME_MULTIPLIER))),
        'profit_protection_enabled': bool(data.get('profit_protection_enabled', PROFIT_PROTECTION_ENABLED)),
        'profit_protect_progress_pct': min(100.0, max(5.0, float(data.get('profit_protect_progress_pct') if data.get('profit_protect_progress_pct') is not None else PROFIT_PROTECT_PROGRESS_PCT))),
        'trailing_stop_activation_pct': min(100.0, max(5.0, float(data.get('trailing_stop_activation_pct') if data.get('trailing_stop_activation_pct') is not None else TRAILING_STOP_ACTIVATION_PCT))),
        'trailing_stop_giveback_pct': min(95.0, max(5.0, float(data.get('trailing_stop_giveback_pct') if data.get('trailing_stop_giveback_pct') is not None else TRAILING_STOP_GIVEBACK_PCT))),
        'exit_at_horizon_end': bool(data.get('exit_at_horizon_end', EXIT_AT_HORIZON_END)),
        'selection_mode': selection_mode,
        'self_scan_universe_limit': min(500, max(20, int(data.get('self_scan_universe_limit') if data.get('self_scan_universe_limit') is not None else TRADING_SELF_SCAN_UNIVERSE_LIMIT))),
        'self_scan_min_score': min(100.0, max(0.0, float(data.get('self_scan_min_score') if data.get('self_scan_min_score') is not None else TRADING_SELF_SCAN_MIN_SCORE))),
    }
    conn = get_db_connection()
    if conn is None:
        return settings
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO trading_settings
                        (id, enabled, mode, require_confirmation, broker, capital_inr, capital_usd, max_position_pct,
                         risk_per_trade_pct, daily_loss_limit_pct, max_open_positions, min_confidence,
                         min_risk_reward, allow_us_with_inr_capital, weekly_profit_target_pct, max_weekly_loss_pct,
                         cover_api_costs_from_profit, stop_after_weekly_target, stop_after_weekly_loss, broker_api_monthly_cost_inr,
                         opening_confirmation_enabled, opening_wait_minutes, opening_max_entry_chase_pct, opening_max_entry_pullback_pct,
                         opening_min_volume_multiplier, profit_protection_enabled, profit_protect_progress_pct,
                         trailing_stop_activation_pct, trailing_stop_giveback_pct, exit_at_horizon_end,
                         selection_mode, self_scan_universe_limit, self_scan_min_score, updated_at)
                    VALUES (1,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                    ON CONFLICT (id) DO UPDATE SET
                        enabled=EXCLUDED.enabled,
                        mode=EXCLUDED.mode,
                        require_confirmation=EXCLUDED.require_confirmation,
                        broker=EXCLUDED.broker,
                        capital_inr=EXCLUDED.capital_inr,
                        capital_usd=EXCLUDED.capital_usd,
                        max_position_pct=EXCLUDED.max_position_pct,
                        risk_per_trade_pct=EXCLUDED.risk_per_trade_pct,
                        daily_loss_limit_pct=EXCLUDED.daily_loss_limit_pct,
                        max_open_positions=EXCLUDED.max_open_positions,
                        min_confidence=EXCLUDED.min_confidence,
                        min_risk_reward=EXCLUDED.min_risk_reward,
                        allow_us_with_inr_capital=EXCLUDED.allow_us_with_inr_capital,
                        weekly_profit_target_pct=EXCLUDED.weekly_profit_target_pct,
                        max_weekly_loss_pct=EXCLUDED.max_weekly_loss_pct,
                        cover_api_costs_from_profit=EXCLUDED.cover_api_costs_from_profit,
                        stop_after_weekly_target=EXCLUDED.stop_after_weekly_target,
                        stop_after_weekly_loss=EXCLUDED.stop_after_weekly_loss,
                        broker_api_monthly_cost_inr=EXCLUDED.broker_api_monthly_cost_inr,
                        opening_confirmation_enabled=EXCLUDED.opening_confirmation_enabled,
                        opening_wait_minutes=EXCLUDED.opening_wait_minutes,
                        opening_max_entry_chase_pct=EXCLUDED.opening_max_entry_chase_pct,
                        opening_max_entry_pullback_pct=EXCLUDED.opening_max_entry_pullback_pct,
                        opening_min_volume_multiplier=EXCLUDED.opening_min_volume_multiplier,
                        profit_protection_enabled=EXCLUDED.profit_protection_enabled,
                        profit_protect_progress_pct=EXCLUDED.profit_protect_progress_pct,
                        trailing_stop_activation_pct=EXCLUDED.trailing_stop_activation_pct,
                        trailing_stop_giveback_pct=EXCLUDED.trailing_stop_giveback_pct,
                        exit_at_horizon_end=EXCLUDED.exit_at_horizon_end,
                        selection_mode=EXCLUDED.selection_mode,
                        self_scan_universe_limit=EXCLUDED.self_scan_universe_limit,
                        self_scan_min_score=EXCLUDED.self_scan_min_score,
                        updated_at=NOW()
                """, (
                    settings['enabled'], settings['mode'], settings['require_confirmation'], settings['broker'],
                    settings['capital_inr'], settings['capital_usd'], settings['max_position_pct'], settings['risk_per_trade_pct'],
                    settings['daily_loss_limit_pct'], settings['max_open_positions'], settings['min_confidence'],
                    settings['min_risk_reward'], settings['allow_us_with_inr_capital'], settings['weekly_profit_target_pct'],
                    settings['max_weekly_loss_pct'], settings['cover_api_costs_from_profit'], settings['stop_after_weekly_target'],
                    settings['stop_after_weekly_loss'], settings['broker_api_monthly_cost_inr'],
                    settings['opening_confirmation_enabled'], settings['opening_wait_minutes'], settings['opening_max_entry_chase_pct'],
                    settings['opening_max_entry_pullback_pct'], settings['opening_min_volume_multiplier'], settings['profit_protection_enabled'],
                    settings['profit_protect_progress_pct'], settings['trailing_stop_activation_pct'], settings['trailing_stop_giveback_pct'],
                    settings['exit_at_horizon_end'], settings['selection_mode'], settings['self_scan_universe_limit'], settings['self_scan_min_score']
                ))
    finally:
        conn.close()
    return settings


def parse_rr(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float, np.generic)):
        return float(value)
    m = re.search(r"([0-9.]+)", str(value))
    return float(m.group(1)) if m else 0.0


def get_open_positions(market: Optional[str] = None, mode: Optional[str] = None) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    if conn is None:
        return []
    filters = ["status='OPEN'"]
    params: List[Any] = []
    if market:
        filters.append("market=%s")
        params.append(market)
    if mode:
        filters.append("mode=%s")
        params.append(mode)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"SELECT * FROM trade_positions WHERE {' AND '.join(filters)} ORDER BY opened_at DESC", params)
            return list(cur.fetchall())
    finally:
        conn.close()


def fetch_last_price(ticker: str) -> Optional[float]:
    try:
        hist = yf.Ticker(ticker).history(period='2d', interval='1d', timeout=YF_TIMEOUT)
        if hist is not None and not hist.empty:
            return float(hist['Close'].dropna().iloc[-1])
    except Exception:
        return None
    return None


def get_market_session_state(market: str, wait_minutes: Optional[int] = None, now_utc: Optional[datetime.datetime] = None) -> Dict[str, Any]:
    # Return exchange-session state for deterministic entry gating. No paid AI call.
    wait = OPENING_CONFIRMATION_WAIT_MINUTES if wait_minutes is None else int(wait_minutes)
    sched = get_market_schedule(market)
    tz = ZoneInfo(sched['tz'])
    now_utc = now_utc or utc_now_naive()
    if now_utc.tzinfo is None:
        aware = now_utc.replace(tzinfo=datetime.timezone.utc)
    else:
        aware = now_utc.astimezone(datetime.timezone.utc)
    local_now = aware.astimezone(tz)
    day = local_now.date()
    open_dt = datetime.datetime.combine(day, sched['open'], tzinfo=tz)
    close_dt = datetime.datetime.combine(day, sched['close'], tzinfo=tz)
    ready_dt = open_dt + datetime.timedelta(minutes=max(wait, 0))
    if not is_trading_day(market, day):
        state = 'closed_non_trading_day'
    elif local_now < open_dt:
        state = 'before_open'
    elif local_now < ready_dt:
        state = 'opening_wait'
    elif local_now <= close_dt:
        state = 'open_ready'
    else:
        state = 'after_close'
    minutes_since_open = round((local_now - open_dt).total_seconds() / 60, 1)
    return {
        'state': state,
        'market': market,
        'timezone': sched['tz'],
        'local_now': local_now.isoformat(),
        'open_time': open_dt.isoformat(),
        'ready_time': ready_dt.isoformat(),
        'close_time': close_dt.isoformat(),
        'wait_minutes': wait,
        'minutes_since_open': minutes_since_open,
        'can_enter': state == 'open_ready',
    }



def get_intraday_interval_minutes() -> int:
    """Return the candle interval in minutes for wait-window and freshness math."""
    interval = str(INTRADAY_DATA_INTERVAL or '1m').strip().lower()
    m = re.match(r'^(\d+)(m|h|d)$', interval)
    if not m:
        return 1
    value = max(1, int(m.group(1)))
    unit = m.group(2)
    if unit == 'h':
        return value * 60
    if unit == 'd':
        return value * 1440
    return value


def get_intraday_last_candle_meta(hist: Any, market: str) -> Dict[str, Any]:
    """Measure provider freshness so stale/delayed candles are visible and blocked if needed."""
    meta = {'last_candle_at': None, 'data_age_minutes': None, 'is_stale': False, 'freshness_note': 'freshness_unknown'}
    try:
        if hist is None or hist.empty or len(hist.index) == 0:
            return meta
        ts = hist.index[-1]
        if hasattr(ts, 'to_pydatetime'):
            dt = ts.to_pydatetime()
        elif isinstance(ts, datetime.datetime):
            dt = ts
        else:
            return meta
        if dt.tzinfo is None:
            sched = get_market_schedule(market)
            dt = dt.replace(tzinfo=ZoneInfo(sched.get('tz') or 'UTC'))
        now = datetime.datetime.now(datetime.timezone.utc)
        dt_utc = dt.astimezone(datetime.timezone.utc)
        age = max(0.0, (now - dt_utc).total_seconds() / 60.0)
        max_age = max(1, int(INTRADAY_MAX_DATA_AGE_MINUTES))
        meta.update({
            'last_candle_at': dt_utc.isoformat(),
            'data_age_minutes': round(age, 1),
            'is_stale': age > max_age,
            'freshness_note': 'fresh' if age <= max_age else f'stale_or_delayed: last candle {round(age,1)} min old; max allowed {max_age} min',
        })
    except Exception as e:
        meta['freshness_note'] = f'freshness_check_failed: {str(e)[:80]}'
    return meta


def attach_intraday_trade_metrics(candidate: Dict[str, Any], settings: Dict[str, Any]) -> Dict[str, Any]:
    """Attach entry/target/stop/RR before strict learning reviews the candidate.

    Previously strict learning saw no target/stop yet, so every intraday row looked
    like reward/risk 0 and got blocked. This mirrors build_intraday_plan's risk
    math early enough for the reviewer and UI.
    """
    try:
        entry = float(candidate.get('last') or candidate.get('entry_price') or 0)
        if entry <= 0:
            return candidate
        max_stop_pct = float(settings.get('max_stop_loss_pct') or INTRADAY_MAX_STOP_LOSS_PCT)
        min_rr = float(settings.get('min_risk_reward') or INTRADAY_MIN_RISK_REWARD)
        quick_target_pct = float(settings.get('quick_target_pct') or INTRADAY_QUICK_TARGET_PCT)
        stop_by_pct = entry * (1 - max_stop_pct / 100)
        opening_low = float(candidate.get('opening_low') or stop_by_pct)
        vwap = float(candidate.get('vwap') or stop_by_pct)
        stop = max(stop_by_pct, min(vwap * 0.998, opening_low * 0.998) if vwap and opening_low else stop_by_pct)
        if stop >= entry:
            stop = stop_by_pct
        risk_per_share = max(entry - stop, 0.01)
        quick_target = entry * (1 + quick_target_pct / 100)
        rr_target = entry + risk_per_share * min_rr
        target = max(quick_target, rr_target)
        rr = (target - entry) / risk_per_share if risk_per_share > 0 else 0.0
        candidate['entry_price'] = round(entry, 2)
        candidate['stop_price'] = round(stop, 2)
        candidate['target_price'] = round(target, 2)
        candidate['rr_proxy'] = round(rr, 2)
        candidate['risk_reward'] = f"{round(rr, 2)}:1"
        candidate['min_risk_reward'] = min_rr
        candidate['min_score'] = float(settings.get('min_score') or INTRADAY_MIN_SCORE)
        candidate['min_volume_multiplier'] = float(settings.get('min_volume_multiplier') or INTRADAY_MIN_VOLUME_MULTIPLIER)
    except Exception as e:
        candidate['trade_metric_error'] = str(e)[:120]
    return candidate

def fetch_intraday_snapshot(ticker: str, market: Optional[str] = None) -> Dict[str, Any]:
    # Fetch a lightweight intraday snapshot for rule-based confirmation/exits.
    try:
        hist = yf.Ticker(ticker).history(period='1d', interval=INTRADAY_DATA_INTERVAL, timeout=YF_TIMEOUT)
        if hist is not None and not hist.empty:
            close = hist['Close'].dropna()
            high = hist['High'].dropna()
            low = hist['Low'].dropna()
            vol = hist['Volume'].dropna() if 'Volume' in hist else None
            last = float(close.iloc[-1]) if not close.empty else None
            open_price = float(hist['Open'].dropna().iloc[0]) if 'Open' in hist and not hist['Open'].dropna().empty else last
            session_high = float(high.max()) if not high.empty else last
            session_low = float(low.min()) if not low.empty else last
            recent_closes = [float(x) for x in close.tail(4).tolist()]
            recent_trend = 'flat'
            if len(recent_closes) >= 3:
                if recent_closes[-1] < recent_closes[-2] < recent_closes[-3]:
                    recent_trend = 'falling'
                elif recent_closes[-1] > recent_closes[-2] > recent_closes[-3]:
                    recent_trend = 'rising'
            avg_bar_volume = float(vol.tail(20).mean()) if vol is not None and not vol.empty else None
            last_bar_volume = float(vol.iloc[-1]) if vol is not None and not vol.empty else None
            vwap = None
            try:
                if vol is not None and not vol.empty and float(vol.sum()) > 0:
                    typical = (hist['High'] + hist['Low'] + hist['Close']) / 3
                    vwap = float((typical * hist['Volume']).sum() / hist['Volume'].sum())
            except Exception:
                vwap = None
            return {
                'ok': True,
                'ticker': ticker,
                'last': last,
                'open': open_price,
                'high': session_high,
                'low': session_low,
                'recent_closes': recent_closes,
                'recent_trend': recent_trend,
                'vwap': vwap,
                'last_bar_volume': last_bar_volume,
                'avg_bar_volume': avg_bar_volume,
                'bars': int(len(hist)),
            }
    except Exception as e:
        return {'ok': False, 'ticker': ticker, 'error': str(e)}
    last = fetch_last_price(ticker)
    return {'ok': bool(last), 'ticker': ticker, 'last': last, 'open': last, 'high': last, 'low': last, 'recent_trend': 'unknown', 'bars': 0}


def evaluate_opening_confirmation(plan: Dict[str, Any], settings: Dict[str, Any]) -> Dict[str, Any]:
    # Block/approve BUY entries using first-15-min and price-action rules.
    if not settings.get('opening_confirmation_enabled', True):
        return {'allowed': True, 'reason': 'opening_confirmation_disabled', 'session': None, 'snapshot': None}
    market = plan.get('market') or 'US'
    session = get_market_session_state(market, settings.get('opening_wait_minutes'))
    if not session.get('can_enter'):
        return {'allowed': False, 'reason': session.get('state'), 'session': session, 'snapshot': None}
    snap = fetch_intraday_snapshot(plan['ticker'], market)
    if not snap.get('ok') or not snap.get('last'):
        return {'allowed': False, 'reason': 'no_intraday_confirmation_data', 'session': session, 'snapshot': snap}
    last = float(snap['last'])
    entry = float(plan.get('limit_price') or 0)
    stop = float(plan.get('stop_price') or 0)
    target = float(plan.get('target_price') or 0)
    if entry <= 0 or stop <= 0 or target <= 0:
        return {'allowed': False, 'reason': 'invalid_plan_prices', 'session': session, 'snapshot': snap}
    max_chase = float(settings.get('opening_max_entry_chase_pct') or 0)
    max_pullback = float(settings.get('opening_max_entry_pullback_pct') or 0)
    if last > entry * (1 + max_chase / 100):
        return {'allowed': False, 'reason': 'price_too_far_above_entry_do_not_chase', 'session': session, 'snapshot': snap}
    if last < entry * (1 - max_pullback / 100):
        return {'allowed': False, 'reason': 'price_too_far_below_entry_or_setup_weakening', 'session': session, 'snapshot': snap}
    if last <= stop:
        return {'allowed': False, 'reason': 'price_near_or_below_stop_risk_zone', 'session': session, 'snapshot': snap}
    if target and last >= target:
        return {'allowed': False, 'reason': 'target_already_reached_before_entry', 'session': session, 'snapshot': snap}
    open_price = snap.get('open')
    day_low = snap.get('low')
    if open_price and last < float(open_price) * 0.995 and snap.get('recent_trend') == 'falling':
        return {'allowed': False, 'reason': 'opening_reversal_down', 'session': session, 'snapshot': snap}
    if day_low and last <= float(day_low) * 1.002 and snap.get('recent_trend') == 'falling':
        return {'allowed': False, 'reason': 'breaking_opening_range_low', 'session': session, 'snapshot': snap}
    min_vol = float(settings.get('opening_min_volume_multiplier') or 0)
    if min_vol > 0 and snap.get('avg_bar_volume') and snap.get('last_bar_volume'):
        if float(snap['last_bar_volume']) < float(snap['avg_bar_volume']) * min_vol:
            return {'allowed': False, 'reason': 'volume_confirmation_failed', 'session': session, 'snapshot': snap}
    confirmed_entry = round(last, 2)
    reward = target - confirmed_entry
    risk = confirmed_entry - stop
    rr = reward / risk if risk > 0 else 0
    if rr < float(settings.get('min_risk_reward') or 0):
        return {'allowed': False, 'reason': 'risk_reward_deteriorated_after_open', 'session': session, 'snapshot': snap, 'confirmed_rr': round(rr, 2)}
    return {'allowed': True, 'reason': 'opening_confirmation_passed', 'session': session, 'snapshot': snap, 'confirmed_entry': confirmed_entry, 'confirmed_rr': round(rr, 2)}


def is_position_horizon_due(pos: Dict[str, Any], settings: Dict[str, Any]) -> bool:
    if not settings.get('exit_at_horizon_end', True):
        return False
    opened_at = pos.get('opened_at')
    if not opened_at:
        return False
    if isinstance(opened_at, str):
        try:
            opened_at = datetime.datetime.fromisoformat(opened_at.replace('Z', '+00:00')).replace(tzinfo=None)
        except Exception:
            return False
    due = get_prediction_check_datetime(pos.get('market') or 'US', pos.get('horizon') or 'day', opened_at)
    return utc_now_naive() >= due



def get_current_trading_week_bounds() -> Tuple[datetime.datetime, datetime.datetime]:
    """Return current India trading week start/end as UTC-naive timestamps."""
    tz = ZoneInfo('Asia/Kolkata')
    now_local = datetime.datetime.now(tz)
    start_local = (now_local - datetime.timedelta(days=now_local.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    end_local = start_local + datetime.timedelta(days=7)
    return (
        start_local.astimezone(datetime.timezone.utc).replace(tzinfo=None),
        end_local.astimezone(datetime.timezone.utc).replace(tzinfo=None),
    )


def get_weekly_goal_status(settings: Dict[str, Any]) -> Dict[str, Any]:
    """Measure whether automation may open new trades this week.

    If cover_api_costs_from_profit is enabled, paid API costs are subtracted
    before the weekly profit target is considered reached.
    """
    capital = float(settings.get('capital_inr') or 0)
    target_pct = float(settings.get('weekly_profit_target_pct') or 0)
    max_loss_pct = float(settings.get('max_weekly_loss_pct') or 0)
    week_start, week_end = get_current_trading_week_bounds()
    conn = get_db_connection()
    realised_pnl = 0.0
    api_cost_inr = 0.0
    broker_api_monthly_cost = float(settings.get('broker_api_monthly_cost_inr') or 0)
    broker_api_weekly_cost = round((broker_api_monthly_cost * 12 / 52), 2) if broker_api_monthly_cost > 0 else 0.0
    closed_positions = 0
    paid_calls = 0
    if conn is not None:
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT COALESCE(SUM(pnl_amount),0) AS pnl, COUNT(*) AS closed_positions
                    FROM trade_positions
                    WHERE status='CLOSED' AND closed_at >= %s AND closed_at < %s
                """, (week_start, week_end))
                pnl_row = cur.fetchone() or {}
                realised_pnl = float(pnl_row.get('pnl') or 0)
                closed_positions = int(pnl_row.get('closed_positions') or 0)
                cur.execute("""
                    SELECT COALESCE(SUM(cost_inr),0) AS api_cost, COUNT(*) AS paid_calls
                    FROM api_cost_log
                    WHERE created_at >= %s AND created_at < %s
                """, (week_start, week_end))
                cost_row = cur.fetchone() or {}
                api_cost_inr = float(cost_row.get('api_cost') or 0)
                paid_calls = int(cost_row.get('paid_calls') or 0)
        finally:
            conn.close()
    target_profit_inr = round(capital * target_pct / 100, 2)
    max_loss_inr = round(capital * max_loss_pct / 100, 2)
    cover_api = bool(settings.get('cover_api_costs_from_profit'))
    total_api_cost_inr = round(api_cost_inr + broker_api_weekly_cost, 2)
    net_after_api = round(realised_pnl - total_api_cost_inr, 2) if cover_api else round(realised_pnl, 2)
    gross_needed = round(target_profit_inr + (total_api_cost_inr if cover_api else 0), 2)
    target_reached = target_profit_inr > 0 and net_after_api >= target_profit_inr
    loss_limit_hit = max_loss_inr > 0 and net_after_api <= -max_loss_inr
    halt_reason = None
    target_hard_ceiling = bool(settings.get('stop_after_weekly_target'))
    if target_hard_ceiling and target_reached:
        halt_reason = 'weekly_target_reached_after_api_costs'
    if bool(settings.get('stop_after_weekly_loss')) and loss_limit_hit:
        halt_reason = 'weekly_loss_limit_hit'
    progress_pct = round((net_after_api / target_profit_inr) * 100, 2) if target_profit_inr else 0.0
    return {
        'week_start': week_start.isoformat(),
        'week_end': week_end.isoformat(),
        'capital_inr': round(capital, 2),
        'weekly_profit_target_pct': target_pct,
        'target_profit_inr': target_profit_inr,
        'max_weekly_loss_pct': max_loss_pct,
        'max_weekly_loss_inr': max_loss_inr,
        'realised_pnl_inr': round(realised_pnl, 2),
        'paid_ai_cost_inr': round(api_cost_inr, 2),
        'broker_api_monthly_cost_inr': round(broker_api_monthly_cost, 2),
        'broker_api_weekly_cost_inr': round(broker_api_weekly_cost, 2),
        'api_cost_inr': total_api_cost_inr,
        'net_after_api_inr': net_after_api,
        'gross_profit_needed_to_cover_target_and_api_inr': gross_needed,
        'api_costs_covered': realised_pnl >= total_api_cost_inr if total_api_cost_inr > 0 else True,
        'target_reached': target_reached,
        'weekly_target_is_hard_ceiling': target_hard_ceiling,
        'profit_target_policy': 'Weekly target is a profit milestone, not a hard ceiling. The system can continue only if new setups pass strict risk checks; capital/profit protection still applies.',
        'loss_limit_hit': loss_limit_hit,
        'progress_pct': progress_pct,
        'closed_positions': closed_positions,
        'paid_api_calls': paid_calls,
        'can_open_new_trades': halt_reason is None,
        'halt_reason': halt_reason,
        'policy': ('Net weekly goal subtracts paid AI cost plus configured broker/API subscription allocation before target is considered achieved. The target is treated as a milestone, not a profit ceiling, unless you enable Use weekly target as hard ceiling.' if cover_api else 'Weekly goal uses gross realised trading P/L. The target is a milestone, not a profit ceiling, unless you enable Use weekly target as hard ceiling.'),
        'execution_policy': get_execution_policy(),
    }



def normalize_claude_intraday_mode(value: Any) -> str:
    mode = str(value or 'off').strip().lower().replace('-', '_')
    aliases = {
        'none': 'off',
        'disabled': 'off',
        'review': 'review_only',
        'approval': 'approval_required',
        'full_control': 'paper_full_control',
        'paper': 'paper_full_control',
    }
    mode = aliases.get(mode, mode)
    return mode if mode in CLAUDE_INTRADAY_ALLOWED_MODES else 'off'

def get_intraday_settings() -> Dict[str, Any]:
    defaults = {
        'enabled': INTRADAY_ENGINE_ENABLED,
        'market': INTRADAY_DEFAULT_MARKET if INTRADAY_DEFAULT_MARKET in DEFAULT_UNIVERSES else 'IN',
        'execution_mode': INTRADAY_DEFAULT_MODE if INTRADAY_DEFAULT_MODE in ('paper', 'assisted', 'live') else 'paper',
        'require_confirmation': INTRADAY_REQUIRE_CONFIRMATION,
        'auto_enabled': INTRADAY_AUTO_ENABLED,
        'auto_interval_seconds': INTRADAY_AUTO_INTERVAL_SECONDS,
        'opening_wait_minutes': INTRADAY_OPENING_WAIT_MINUTES,
        'universe_limit': INTRADAY_UNIVERSE_LIMIT,
        'max_candidates': INTRADAY_MAX_CANDIDATES,
        'min_score': INTRADAY_MIN_SCORE,
        'price_filter_enabled': INTRADAY_PRICE_FILTER_ENABLED,
        'min_stock_price': INTRADAY_MIN_STOCK_PRICE_INR,
        'max_stock_price': INTRADAY_MAX_STOCK_PRICE_INR,
        'min_stock_price_inr': INTRADAY_MIN_STOCK_PRICE_INR,
        'max_stock_price_inr': INTRADAY_MAX_STOCK_PRICE_INR,
        'min_stock_price_usd': INTRADAY_MIN_STOCK_PRICE_USD,
        'max_stock_price_usd': INTRADAY_MAX_STOCK_PRICE_USD,
        'min_order_quantity': INTRADAY_MIN_ORDER_QUANTITY,
        'min_position_value': INTRADAY_MIN_POSITION_VALUE_INR,
        'min_position_value_inr': INTRADAY_MIN_POSITION_VALUE_INR,
        'min_position_value_usd': INTRADAY_MIN_POSITION_VALUE_USD,
        'min_price_change_pct': INTRADAY_MIN_PRICE_CHANGE_PCT,
        'max_chase_pct': INTRADAY_MAX_CHASE_PCT,
        'require_vwap': INTRADAY_REQUIRE_VWAP,
        'require_opening_breakout': INTRADAY_REQUIRE_OPENING_BREAKOUT,
        'min_volume_multiplier': INTRADAY_MIN_VOLUME_MULTIPLIER,
        'quick_target_pct': INTRADAY_QUICK_TARGET_PCT,
        'max_stop_loss_pct': INTRADAY_MAX_STOP_LOSS_PCT,
        'min_risk_reward': INTRADAY_MIN_RISK_REWARD,
        'max_trades_per_day': INTRADAY_MAX_TRADES_PER_DAY,
        'profit_book_pct': INTRADAY_PROFIT_BOOK_PCT,
        'force_exit_before_close_minutes': INTRADAY_FORCE_EXIT_BEFORE_CLOSE_MINUTES,
        'simulation_capital_inr': TRADING_DEFAULT_CAPITAL_INR,
        'simulation_capital_usd': TRADING_DEFAULT_CAPITAL_USD,
        'min_expected_net_profit_inr': INTRADAY_MIN_EXPECTED_NET_PROFIT_INR,
        'min_expected_net_profit_usd': INTRADAY_MIN_EXPECTED_NET_PROFIT_USD,
        'min_gross_profit_to_cost_ratio': INTRADAY_MIN_GROSS_PROFIT_TO_COST_RATIO,
        'claude_control_mode': CLAUDE_INTRADAY_CONTROL_MODE,
        'claude_max_reviews_per_run': CLAUDE_INTRADAY_MAX_REVIEWS_PER_RUN,
        'claude_min_confidence': CLAUDE_INTRADAY_MIN_CONFIDENCE,
        'claude_target_instruction': CLAUDE_INTRADAY_TARGET_INSTRUCTION,
    }
    conn = get_db_connection()
    if conn is None:
        return defaults
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM intraday_settings WHERE id=1")
            row = cur.fetchone()
            if not row:
                return defaults
            return {
                'enabled': bool(row.get('enabled')),
                'market': row.get('market') if row.get('market') in DEFAULT_UNIVERSES else defaults['market'],
                'execution_mode': row.get('execution_mode') if row.get('execution_mode') in ('paper', 'assisted', 'live') else defaults['execution_mode'],
                'require_confirmation': bool(row.get('require_confirmation')),
                'auto_enabled': bool(row.get('auto_enabled')),
                'auto_interval_seconds': int(row.get('auto_interval_seconds') or defaults['auto_interval_seconds']),
                'opening_wait_minutes': int(row.get('opening_wait_minutes') or defaults['opening_wait_minutes']),
                'universe_limit': int(row.get('universe_limit') or defaults['universe_limit']),
                'max_candidates': int(row.get('max_candidates') or defaults['max_candidates']),
                'min_score': int(row.get('min_score') or defaults['min_score']),
                'price_filter_enabled': bool(row.get('price_filter_enabled')) if row.get('price_filter_enabled') is not None else defaults['price_filter_enabled'],
                'min_stock_price': float(row.get('min_stock_price') or defaults['min_stock_price']),
                'max_stock_price': float(row.get('max_stock_price') or defaults['max_stock_price']),
                'min_stock_price_inr': float(row.get('min_stock_price_inr') or row.get('min_stock_price') or defaults['min_stock_price_inr']),
                'max_stock_price_inr': float(row.get('max_stock_price_inr') or row.get('max_stock_price') or defaults['max_stock_price_inr']),
                'min_stock_price_usd': float(row.get('min_stock_price_usd') or defaults['min_stock_price_usd']),
                'max_stock_price_usd': float(row.get('max_stock_price_usd') or defaults['max_stock_price_usd']),
                'min_order_quantity': int(row.get('min_order_quantity') or defaults['min_order_quantity']),
                'min_position_value': float(row.get('min_position_value') or defaults['min_position_value']),
                'min_position_value_inr': float(row.get('min_position_value_inr') or row.get('min_position_value') or defaults['min_position_value_inr']),
                'min_position_value_usd': float(row.get('min_position_value_usd') or defaults['min_position_value_usd']),
                'min_price_change_pct': float(row.get('min_price_change_pct') or defaults['min_price_change_pct']),
                'max_chase_pct': float(row.get('max_chase_pct') or defaults['max_chase_pct']),
                'require_vwap': bool(row.get('require_vwap')),
                'require_opening_breakout': bool(row.get('require_opening_breakout')),
                'min_volume_multiplier': float(row.get('min_volume_multiplier') or defaults['min_volume_multiplier']),
                'quick_target_pct': float(row.get('quick_target_pct') or defaults['quick_target_pct']),
                'max_stop_loss_pct': float(row.get('max_stop_loss_pct') or defaults['max_stop_loss_pct']),
                'min_risk_reward': float(row.get('min_risk_reward') or defaults['min_risk_reward']),
                'max_trades_per_day': int(row.get('max_trades_per_day') or defaults['max_trades_per_day']),
                'profit_book_pct': float(row.get('profit_book_pct') or defaults['profit_book_pct']),
                'force_exit_before_close_minutes': int(row.get('force_exit_before_close_minutes') or defaults['force_exit_before_close_minutes']),
                'simulation_capital_inr': float(row.get('simulation_capital_inr') or defaults['simulation_capital_inr']),
                'simulation_capital_usd': float(row.get('simulation_capital_usd') or defaults['simulation_capital_usd']),
                'min_expected_net_profit_inr': float(row.get('min_expected_net_profit_inr') or defaults['min_expected_net_profit_inr']),
                'min_expected_net_profit_usd': float(row.get('min_expected_net_profit_usd') or defaults['min_expected_net_profit_usd']),
                'min_gross_profit_to_cost_ratio': float(row.get('min_gross_profit_to_cost_ratio') or defaults['min_gross_profit_to_cost_ratio']),
                'claude_control_mode': normalize_claude_intraday_mode(row.get('claude_control_mode') or defaults['claude_control_mode']),
                'claude_max_reviews_per_run': int(row.get('claude_max_reviews_per_run') or defaults['claude_max_reviews_per_run']),
                'claude_min_confidence': int(row.get('claude_min_confidence') or defaults['claude_min_confidence']),
                'claude_target_instruction': (row.get('claude_target_instruction') or defaults['claude_target_instruction']),
            }
    finally:
        conn.close()


def save_intraday_settings(data: Dict[str, Any]) -> Dict[str, Any]:
    market_value = data.get('market') if data.get('market') in DEFAULT_UNIVERSES else 'IN'
    settings = {
        'enabled': bool(data.get('enabled', INTRADAY_ENGINE_ENABLED)),
        'market': market_value,
        'execution_mode': str(data.get('execution_mode') or data.get('mode') or INTRADAY_DEFAULT_MODE).lower(),
        'require_confirmation': bool(data.get('require_confirmation', INTRADAY_REQUIRE_CONFIRMATION)),
        'auto_enabled': bool(data.get('auto_enabled', INTRADAY_AUTO_ENABLED)),
        'auto_interval_seconds': min(3600, max(30, int(data.get('auto_interval_seconds') if data.get('auto_interval_seconds') is not None else INTRADAY_AUTO_INTERVAL_SECONDS))),
        'opening_wait_minutes': min(90, max(0, int(data.get('opening_wait_minutes') if data.get('opening_wait_minutes') is not None else INTRADAY_OPENING_WAIT_MINUTES))),
        'universe_limit': min(300, max(10, int(data.get('universe_limit') if data.get('universe_limit') is not None else INTRADAY_UNIVERSE_LIMIT))),
        'max_candidates': min(30, max(1, int(data.get('max_candidates') if data.get('max_candidates') is not None else INTRADAY_MAX_CANDIDATES))),
        'min_score': min(99, max(1, int(data.get('min_score') if data.get('min_score') is not None else INTRADAY_MIN_SCORE))),
        'price_filter_enabled': bool(data.get('price_filter_enabled', INTRADAY_PRICE_FILTER_ENABLED)),
        'min_stock_price': min(1000000.0, max(0.0, float(data.get('min_stock_price') if data.get('min_stock_price') is not None else INTRADAY_MIN_STOCK_PRICE_INR))),
        'max_stock_price': min(1000000.0, max(0.0, float(data.get('max_stock_price') if data.get('max_stock_price') is not None else INTRADAY_MAX_STOCK_PRICE_INR))),
        'min_stock_price_inr': min(1000000.0, max(0.0, float(data.get('min_stock_price_inr') if data.get('min_stock_price_inr') is not None else data.get('min_stock_price') if data.get('min_stock_price') is not None else INTRADAY_MIN_STOCK_PRICE_INR))),
        'max_stock_price_inr': min(1000000.0, max(0.0, float(data.get('max_stock_price_inr') if data.get('max_stock_price_inr') is not None else data.get('max_stock_price') if data.get('max_stock_price') is not None else INTRADAY_MAX_STOCK_PRICE_INR))),
        'min_stock_price_usd': min(1000000.0, max(0.0, float(data.get('min_stock_price_usd') if data.get('min_stock_price_usd') is not None else INTRADAY_MIN_STOCK_PRICE_USD))),
        'max_stock_price_usd': min(1000000.0, max(0.0, float(data.get('max_stock_price_usd') if data.get('max_stock_price_usd') is not None else INTRADAY_MAX_STOCK_PRICE_USD))),
        'min_order_quantity': min(1000000, max(1, int(data.get('min_order_quantity') if data.get('min_order_quantity') is not None else INTRADAY_MIN_ORDER_QUANTITY))),
        'min_position_value': min(100000000.0, max(0.0, float(data.get('min_position_value') if data.get('min_position_value') is not None else INTRADAY_MIN_POSITION_VALUE_INR))),
        'min_position_value_inr': min(100000000.0, max(0.0, float(data.get('min_position_value_inr') if data.get('min_position_value_inr') is not None else data.get('min_position_value') if data.get('min_position_value') is not None else INTRADAY_MIN_POSITION_VALUE_INR))),
        'min_position_value_usd': min(100000000.0, max(0.0, float(data.get('min_position_value_usd') if data.get('min_position_value_usd') is not None else INTRADAY_MIN_POSITION_VALUE_USD))),
        'min_price_change_pct': min(20.0, max(0.0, float(data.get('min_price_change_pct') if data.get('min_price_change_pct') is not None else INTRADAY_MIN_PRICE_CHANGE_PCT))),
        'max_chase_pct': min(20.0, max(0.1, float(data.get('max_chase_pct') if data.get('max_chase_pct') is not None else INTRADAY_MAX_CHASE_PCT))),
        'require_vwap': bool(data.get('require_vwap', INTRADAY_REQUIRE_VWAP)),
        'require_opening_breakout': bool(data.get('require_opening_breakout', INTRADAY_REQUIRE_OPENING_BREAKOUT)),
        'min_volume_multiplier': min(10.0, max(0.0, float(data.get('min_volume_multiplier') if data.get('min_volume_multiplier') is not None else INTRADAY_MIN_VOLUME_MULTIPLIER))),
        'quick_target_pct': min(20.0, max(0.05, float(data.get('quick_target_pct') if data.get('quick_target_pct') is not None else INTRADAY_QUICK_TARGET_PCT))),
        'max_stop_loss_pct': min(10.0, max(0.05, float(data.get('max_stop_loss_pct') if data.get('max_stop_loss_pct') is not None else INTRADAY_MAX_STOP_LOSS_PCT))),
        'min_risk_reward': min(10.0, max(0.2, float(data.get('min_risk_reward') if data.get('min_risk_reward') is not None else INTRADAY_MIN_RISK_REWARD))),
        'max_trades_per_day': min(50, max(1, int(data.get('max_trades_per_day') if data.get('max_trades_per_day') is not None else INTRADAY_MAX_TRADES_PER_DAY))),
        'profit_book_pct': min(20.0, max(0.05, float(data.get('profit_book_pct') if data.get('profit_book_pct') is not None else INTRADAY_PROFIT_BOOK_PCT))),
        'force_exit_before_close_minutes': min(120, max(1, int(data.get('force_exit_before_close_minutes') if data.get('force_exit_before_close_minutes') is not None else INTRADAY_FORCE_EXIT_BEFORE_CLOSE_MINUTES))),
        'simulation_capital_inr': min(100000000.0, max(100.0, float(data.get('simulation_capital_inr') if data.get('simulation_capital_inr') is not None else TRADING_DEFAULT_CAPITAL_INR))),
        'simulation_capital_usd': min(100000000.0, max(10.0, float(data.get('simulation_capital_usd') if data.get('simulation_capital_usd') is not None else TRADING_DEFAULT_CAPITAL_USD))),
        'min_expected_net_profit_inr': min(10000000.0, max(0.0, float(data.get('min_expected_net_profit_inr') if data.get('min_expected_net_profit_inr') is not None else INTRADAY_MIN_EXPECTED_NET_PROFIT_INR))),
        'min_expected_net_profit_usd': min(1000000.0, max(0.0, float(data.get('min_expected_net_profit_usd') if data.get('min_expected_net_profit_usd') is not None else INTRADAY_MIN_EXPECTED_NET_PROFIT_USD))),
        'min_gross_profit_to_cost_ratio': min(20.0, max(0.0, float(data.get('min_gross_profit_to_cost_ratio') if data.get('min_gross_profit_to_cost_ratio') is not None else INTRADAY_MIN_GROSS_PROFIT_TO_COST_RATIO))),
        'claude_control_mode': normalize_claude_intraday_mode(data.get('claude_control_mode') or CLAUDE_INTRADAY_CONTROL_MODE),
        'claude_max_reviews_per_run': min(10, max(0, int(data.get('claude_max_reviews_per_run') if data.get('claude_max_reviews_per_run') is not None else CLAUDE_INTRADAY_MAX_REVIEWS_PER_RUN))),
        'claude_min_confidence': min(100, max(0, int(data.get('claude_min_confidence') if data.get('claude_min_confidence') is not None else CLAUDE_INTRADAY_MIN_CONFIDENCE))),
        'claude_target_instruction': str(data.get('claude_target_instruction') or CLAUDE_INTRADAY_TARGET_INSTRUCTION)[:1200],
    }
    if settings['execution_mode'] not in ('paper', 'assisted', 'live'):
        settings['execution_mode'] = 'paper'
    if settings['execution_mode'] == 'live' and not LIVE_TRADING_ENABLED:
        settings['execution_mode'] = 'assisted'
    if settings['execution_mode'] != 'paper':
        settings['require_confirmation'] = True if settings['execution_mode'] == 'assisted' else settings['require_confirmation']
    if settings['max_stock_price'] and settings['min_stock_price'] and settings['max_stock_price'] < settings['min_stock_price']:
        settings['max_stock_price'] = settings['min_stock_price']
    if settings['max_stock_price_inr'] and settings['min_stock_price_inr'] and settings['max_stock_price_inr'] < settings['min_stock_price_inr']:
        settings['max_stock_price_inr'] = settings['min_stock_price_inr']
    if settings['max_stock_price_usd'] and settings['min_stock_price_usd'] and settings['max_stock_price_usd'] < settings['min_stock_price_usd']:
        settings['max_stock_price_usd'] = settings['min_stock_price_usd']
    conn = get_db_connection()
    if conn is None:
        return settings
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO intraday_settings
                        (id, enabled, market, execution_mode, require_confirmation, auto_enabled, auto_interval_seconds,
                         opening_wait_minutes, universe_limit, max_candidates, min_score, price_filter_enabled,
                         min_stock_price, max_stock_price, min_stock_price_inr, max_stock_price_inr,
                         min_stock_price_usd, max_stock_price_usd, min_order_quantity, min_position_value,
                         min_position_value_inr, min_position_value_usd,
                         min_price_change_pct, max_chase_pct, require_vwap, require_opening_breakout,
                         min_volume_multiplier, quick_target_pct, max_stop_loss_pct, min_risk_reward,
                         max_trades_per_day, profit_book_pct, force_exit_before_close_minutes, simulation_capital_inr, simulation_capital_usd,
                         min_expected_net_profit_inr, min_expected_net_profit_usd, min_gross_profit_to_cost_ratio,
                         claude_control_mode, claude_max_reviews_per_run, claude_min_confidence, claude_target_instruction, updated_at)
                    VALUES (1,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                    ON CONFLICT (id) DO UPDATE SET
                        enabled=EXCLUDED.enabled,
                        market=EXCLUDED.market,
                        execution_mode=EXCLUDED.execution_mode,
                        require_confirmation=EXCLUDED.require_confirmation,
                        auto_enabled=EXCLUDED.auto_enabled,
                        auto_interval_seconds=EXCLUDED.auto_interval_seconds,
                        opening_wait_minutes=EXCLUDED.opening_wait_minutes,
                        universe_limit=EXCLUDED.universe_limit,
                        max_candidates=EXCLUDED.max_candidates,
                        min_score=EXCLUDED.min_score,
                        price_filter_enabled=EXCLUDED.price_filter_enabled,
                        min_stock_price=EXCLUDED.min_stock_price,
                        max_stock_price=EXCLUDED.max_stock_price,
                        min_stock_price_inr=EXCLUDED.min_stock_price_inr,
                        max_stock_price_inr=EXCLUDED.max_stock_price_inr,
                        min_stock_price_usd=EXCLUDED.min_stock_price_usd,
                        max_stock_price_usd=EXCLUDED.max_stock_price_usd,
                        min_order_quantity=EXCLUDED.min_order_quantity,
                        min_position_value=EXCLUDED.min_position_value,
                        min_position_value_inr=EXCLUDED.min_position_value_inr,
                        min_position_value_usd=EXCLUDED.min_position_value_usd,
                        min_price_change_pct=EXCLUDED.min_price_change_pct,
                        max_chase_pct=EXCLUDED.max_chase_pct,
                        require_vwap=EXCLUDED.require_vwap,
                        require_opening_breakout=EXCLUDED.require_opening_breakout,
                        min_volume_multiplier=EXCLUDED.min_volume_multiplier,
                        quick_target_pct=EXCLUDED.quick_target_pct,
                        max_stop_loss_pct=EXCLUDED.max_stop_loss_pct,
                        min_risk_reward=EXCLUDED.min_risk_reward,
                        max_trades_per_day=EXCLUDED.max_trades_per_day,
                        profit_book_pct=EXCLUDED.profit_book_pct,
                        force_exit_before_close_minutes=EXCLUDED.force_exit_before_close_minutes,
                        simulation_capital_inr=EXCLUDED.simulation_capital_inr,
                        simulation_capital_usd=EXCLUDED.simulation_capital_usd,
                        min_expected_net_profit_inr=EXCLUDED.min_expected_net_profit_inr,
                        min_expected_net_profit_usd=EXCLUDED.min_expected_net_profit_usd,
                        min_gross_profit_to_cost_ratio=EXCLUDED.min_gross_profit_to_cost_ratio,
                        claude_control_mode=EXCLUDED.claude_control_mode,
                        claude_max_reviews_per_run=EXCLUDED.claude_max_reviews_per_run,
                        claude_min_confidence=EXCLUDED.claude_min_confidence,
                        claude_target_instruction=EXCLUDED.claude_target_instruction,
                        updated_at=NOW()
                """, (
                    settings['enabled'], settings['market'], settings['execution_mode'], settings['require_confirmation'],
                    settings['auto_enabled'], settings['auto_interval_seconds'], settings['opening_wait_minutes'], settings['universe_limit'],
                    settings['max_candidates'], settings['min_score'], settings['price_filter_enabled'],
                    settings['min_stock_price'], settings['max_stock_price'], settings['min_stock_price_inr'], settings['max_stock_price_inr'],
                    settings['min_stock_price_usd'], settings['max_stock_price_usd'], settings['min_order_quantity'], settings['min_position_value'],
                    settings['min_position_value_inr'], settings['min_position_value_usd'],
                    settings['min_price_change_pct'], settings['max_chase_pct'],
                    settings['require_vwap'], settings['require_opening_breakout'], settings['min_volume_multiplier'],
                    settings['quick_target_pct'], settings['max_stop_loss_pct'], settings['min_risk_reward'],
                    settings['max_trades_per_day'], settings['profit_book_pct'], settings['force_exit_before_close_minutes'],
                    settings['simulation_capital_inr'], settings['simulation_capital_usd'],
                    settings['min_expected_net_profit_inr'], settings['min_expected_net_profit_usd'], settings['min_gross_profit_to_cost_ratio'],
                    settings['claude_control_mode'], settings['claude_max_reviews_per_run'], settings['claude_min_confidence'], settings['claude_target_instruction']
                ))
    finally:
        conn.close()
    return settings


def get_intraday_effective_trading_settings(trading_settings: Dict[str, Any], intraday_settings: Dict[str, Any]) -> Dict[str, Any]:
    """Build execution settings for the separate Intraday Engine page.

    Intraday has its own execution mode so it can run in paper/assisted/live without
    forcing the user to change the broader Trading Automation tab. Risk sizing,
    capital, weekly goal and max-loss controls still come from trading_settings.
    """
    effective = dict(trading_settings or {})
    mode = str(intraday_settings.get('execution_mode') or effective.get('mode') or 'paper').lower()
    if mode not in ('paper', 'assisted', 'live'):
        mode = 'paper'
    mode_downgraded = False
    if mode == 'live' and not LIVE_TRADING_ENABLED:
        mode = 'assisted'
        mode_downgraded = True
    effective['enabled'] = bool(intraday_settings.get('enabled'))
    if intraday_settings.get('simulation_capital_inr') is not None:
        effective['capital_inr'] = float(intraday_settings.get('simulation_capital_inr') or effective.get('capital_inr') or TRADING_DEFAULT_CAPITAL_INR)
        effective['simulation_capital_inr'] = effective['capital_inr']
    if intraday_settings.get('simulation_capital_usd') is not None:
        effective['capital_usd'] = float(intraday_settings.get('simulation_capital_usd') or effective.get('capital_usd') or TRADING_DEFAULT_CAPITAL_USD)
        effective['simulation_capital_usd'] = effective['capital_usd']
    effective['mode'] = mode
    effective['require_confirmation'] = True if mode == 'assisted' else bool(intraday_settings.get('require_confirmation', True))
    # Intraday should not inherit overly aggressive global profit protection.
    # Keep winners alive until at least ~70% of target progress and only then
    # allow a wider giveback. This prevents 0.01%-0.06% gross exits.
    effective['trailing_stop_activation_pct'] = max(float(effective.get('trailing_stop_activation_pct') or 0), TRAILING_STOP_ACTIVATION_PCT)
    effective['trailing_stop_giveback_pct'] = max(float(effective.get('trailing_stop_giveback_pct') or 0), TRAILING_STOP_GIVEBACK_PCT)
    effective['mode_source'] = 'intraday'
    effective['live_mode_downgraded'] = mode_downgraded
    effective['execution_note'] = (
        'Live intraday mode was downgraded to assisted because LIVE_TRADING_ENABLED is false.'
        if mode_downgraded else
        'Intraday execution mode is controlled from the Intraday Engine page.'
    )
    return effective


def get_intraday_auto_state() -> Dict[str, Any]:
    with INTRADAY_AUTO_LOCK:
        state = dict(INTRADAY_AUTO_STATE)
        state['thread_alive'] = bool(INTRADAY_AUTO_THREAD and INTRADAY_AUTO_THREAD.is_alive())
        return state


def _sleep_with_stop(seconds: int) -> bool:
    remaining = max(1, int(seconds))
    while remaining > 0:
        if INTRADAY_AUTO_STOP_EVENT.wait(timeout=min(5, remaining)):
            return True
        remaining -= min(5, remaining)
    return False


def intraday_auto_loop():
    global INTRADAY_AUTO_THREAD
    with INTRADAY_AUTO_LOCK:
        INTRADAY_AUTO_STATE.update({
            'running': True,
            'started_at': utc_now_naive().isoformat(),
            'last_run_at': None,
            'last_message': 'Auto intraday engine started.',
            'last_error': None,
            'last_orders': 0,
            'check_count': 0,
            'last_result': None,
            'recent_runs': [],
        })
    try:
        while not INTRADAY_AUTO_STOP_EVENT.is_set():
            settings = get_intraday_settings()
            if not settings.get('enabled') or not settings.get('auto_enabled'):
                with INTRADAY_AUTO_LOCK:
                    INTRADAY_AUTO_STATE['last_message'] = 'Auto stopped because intraday engine or auto-run is disabled.'
                break
            try:
                result = run_intraday_engine(trigger='auto')
                compact_result = {
                    'message': result.get('message'),
                    'session': result.get('session') or {},
                    'enabled': result.get('enabled'),
                    'trigger': result.get('trigger'),
                    'checked_at': result.get('checked_at'),
                    'scanned_count': result.get('scanned_count', 0),
                    'shortlist_count': result.get('shortlist_count'),
                    'eligible_count': result.get('eligible_count', len(result.get('candidates') or [])),
                    'data_source': result.get('data_source'),
                    'selection_metadata': result.get('selection_metadata'),
                    'candidates': (result.get('candidates') or [])[:25],
                    'watch': (result.get('watch') or [])[:25],
                    'errors': (result.get('errors') or [])[:25],
                    'orders': (result.get('orders') or [])[:25],
                    'candidate_count': len(result.get('candidates') or []),
                    'watch_count': len(result.get('watch') or []),
                    'order_count': len(result.get('orders') or []),
                }
                should_stop_after_close = (not INTRADAY_AUTO_CONTINUE_AFTER_CLOSE) and (compact_result.get('session') or {}).get('state') in ('after_close', 'closed_non_trading_day')
                with INTRADAY_AUTO_LOCK:
                    recent = list(INTRADAY_AUTO_STATE.get('recent_runs') or [])
                    recent.insert(0, compact_result)
                    INTRADAY_AUTO_STATE.update({
                        'last_run_at': utc_now_naive().isoformat(),
                        'last_checked_at': result.get('checked_at') or utc_now_naive().isoformat(),
                        'last_message': result.get('message'),
                        'last_error': None,
                        'last_orders': len(result.get('orders') or []),
                        'check_count': int(INTRADAY_AUTO_STATE.get('check_count') or 0) + 1,
                        'last_result': compact_result,
                        'recent_runs': recent[:10],
                    })
                if should_stop_after_close:
                    with INTRADAY_AUTO_LOCK:
                        INTRADAY_AUTO_STATE['last_message'] = (result.get('message') or '') + ' Auto stopped because the market session is closed.'
                    break
            except Exception as e:
                with INTRADAY_AUTO_LOCK:
                    # Preserve the last successful scan instead of replacing the dashboard with
                    # 0 scanned / 0 eligible. The error is shown separately.
                    prev_result = INTRADAY_AUTO_STATE.get('last_result')
                    update_payload = {
                        'last_run_at': utc_now_naive().isoformat(),
                        'last_error': str(e),
                        'last_message': 'Auto intraday run failed.',
                    }
                    if not prev_result:
                        update_payload['last_result'] = {'message': 'Auto intraday run failed.', 'error': str(e), 'candidates': [], 'watch': [], 'orders': []}
                    INTRADAY_AUTO_STATE.update(update_payload)
            if _sleep_with_stop(int(settings.get('auto_interval_seconds') or INTRADAY_AUTO_INTERVAL_SECONDS)):
                break
    finally:
        with INTRADAY_AUTO_LOCK:
            INTRADAY_AUTO_STATE['running'] = False
            INTRADAY_AUTO_STATE['last_message'] = INTRADAY_AUTO_STATE.get('last_message') or 'Auto intraday engine stopped.'
        INTRADAY_AUTO_STOP_EVENT.clear()


def start_intraday_auto_worker() -> Dict[str, Any]:
    global INTRADAY_AUTO_THREAD
    settings = get_intraday_settings()
    if not settings.get('enabled'):
        return {'ok': False, 'message': 'Enable Intraday Engine before starting auto mode.', 'auto_state': get_intraday_auto_state()}
    if not settings.get('auto_enabled'):
        return {'ok': False, 'message': 'Turn on Auto-run in Intraday Engine settings first.', 'auto_state': get_intraday_auto_state()}
    already_running = False
    with INTRADAY_AUTO_LOCK:
        already_running = bool(INTRADAY_AUTO_THREAD and INTRADAY_AUTO_THREAD.is_alive())
        if not already_running:
            INTRADAY_AUTO_STOP_EVENT.clear()
            INTRADAY_AUTO_THREAD = threading.Thread(target=intraday_auto_loop, daemon=True, name='intraday-auto-engine')
            INTRADAY_AUTO_THREAD.start()
    if already_running:
        return {'ok': True, 'message': 'Intraday auto mode is already running.', 'auto_state': get_intraday_auto_state()}
    return {'ok': True, 'message': 'Intraday auto mode started.', 'auto_state': get_intraday_auto_state()}


def stop_intraday_auto_worker() -> Dict[str, Any]:
    INTRADAY_AUTO_STOP_EVENT.set()
    with INTRADAY_AUTO_LOCK:
        INTRADAY_AUTO_STATE['last_message'] = 'Stop requested for intraday auto mode.'
    return {'ok': True, 'message': 'Intraday auto stop requested.', 'auto_state': get_intraday_auto_state()}


def parse_market_csv(raw: str, default: Optional[List[str]] = None) -> List[str]:
    default = default or ['IN']
    out = []
    for item in str(raw or '').split(','):
        v = item.strip()
        if v in DEFAULT_UNIVERSES and v not in out:
            out.append(v)
    return out or default


def parse_horizon_csv(raw: str, default: Optional[List[str]] = None) -> List[str]:
    default = default or ['day']
    out = []
    for item in str(raw or '').split(','):
        v = item.strip().lower()
        if v in ('day', 'week') and v not in out:
            out.append(v)
    return out or default


def get_paper_auto_state() -> Dict[str, Any]:
    with PAPER_AUTO_LOCK:
        state = dict(PAPER_AUTO_STATE)
        state['thread_alive'] = bool(PAPER_AUTO_THREAD and PAPER_AUTO_THREAD.is_alive())
        state['config'] = {
            'enabled_env': PAPER_AUTO_ENABLED,
            'on_start_env': PAPER_AUTO_ON_START,
            'markets': parse_market_csv(PAPER_AUTO_MARKETS, ['IN','US']),
            'horizons': parse_horizon_csv(PAPER_AUTO_HORIZONS, ['day','week']),
            'intraday_markets': parse_market_csv(PAPER_AUTO_INTRADAY_MARKETS, ['IN','US']),
            'interval_seconds': PAPER_AUTO_INTERVAL_SECONDS,
            'paper_only': PAPER_AUTO_FORCE_PAPER_MODE,
            'separate_learning_policy': 'Daily/week paper positions use engine=trading_automation. Intraday paper positions use engine=intraday. Their P/L and learning are not mixed.'
        }
        return state


def build_paper_auto_trading_settings(market: str) -> Dict[str, Any]:
    settings = dict(get_trading_settings())
    settings['enabled'] = True
    settings['mode'] = 'paper'
    settings['require_confirmation'] = False
    settings['broker'] = 'paper-simulation'
    settings.setdefault('capital_usd', TRADING_DEFAULT_CAPITAL_USD)
    settings.setdefault('capital_inr', TRADING_DEFAULT_CAPITAL_INR)
    settings['auto_paper_market'] = market
    return settings


def run_paper_auto_cycle(trigger: str = 'paper-auto') -> Dict[str, Any]:
    """Run one paper-only cycle for daily/week and intraday.

    It checks exits first through each engine, then opens only paper positions if
    the relevant market is open/ready and strict rules allow the setup. No live
    orders are ever submitted from this scheduler.
    """
    started = utc_now_naive()
    markets = parse_market_csv(PAPER_AUTO_MARKETS, ['IN','US'])
    horizons = parse_horizon_csv(PAPER_AUTO_HORIZONS, ['day','week'])
    intraday_markets = parse_market_csv(PAPER_AUTO_INTRADAY_MARKETS, markets)
    results: Dict[str, Any] = {'started_at': started.isoformat(), 'trigger': trigger, 'daily_week': [], 'intraday': [], 'orders_created': 0, 'exits_closed': 0, 'skipped': []}

    for market in markets:
        session = get_market_session_state(market, wait_minutes=0)
        if PAPER_AUTO_REQUIRE_MARKET_OPEN and session.get('state') not in ('opening_wait', 'open_ready'):
            results['skipped'].append({'engine': 'daily_week', 'market': market, 'reason': f"market not open ({session.get('state')})", 'session': session})
            continue
        settings = build_paper_auto_trading_settings(market)
        for horizon in horizons:
            try:
                r = run_trading_automation(market, horizon, settings_override=settings, trigger=trigger, order_source='paper-auto-daily-week')
                results['daily_week'].append({'market': market, 'horizon': horizon, 'result': r})
                results['orders_created'] += len(r.get('orders') or [])
                results['exits_closed'] += len(((r.get('exits') or {}).get('closed')) or [])
            except Exception as e:
                results['daily_week'].append({'market': market, 'horizon': horizon, 'error': str(e)})

    for market in intraday_markets:
        session = get_market_session_state(market, wait_minutes=0)
        if PAPER_AUTO_REQUIRE_MARKET_OPEN and session.get('state') not in ('opening_wait', 'open_ready'):
            results['skipped'].append({'engine': 'intraday', 'market': market, 'reason': f"market not open ({session.get('state')})", 'session': session})
            continue
        tsettings = build_paper_auto_trading_settings(market)
        isettings = {
            'enabled': True,
            'market': market,
            'execution_mode': 'paper',
            'require_confirmation': False,
            'auto_enabled': True,
        }
        try:
            r = run_intraday_engine(trigger=f'{trigger}-intraday', market_override=market, settings_override=isettings, trading_settings_override=tsettings)
            results['intraday'].append({'market': market, 'result': r})
            results['orders_created'] += len(r.get('orders') or [])
            results['exits_closed'] += len(((r.get('exits') or {}).get('closed')) or [])
        except Exception as e:
            results['intraday'].append({'market': market, 'error': str(e)})

    finished = utc_now_naive()
    results['finished_at'] = finished.isoformat()
    results['message'] = f"Paper auto cycle checked {len(markets)} daily/week market(s) and {len(intraday_markets)} intraday market(s); created {results['orders_created']} paper order(s), closed {results['exits_closed']} position(s)."
    log_paper_auto_run(results, started, finished)
    return results


def paper_auto_loop():
    global PAPER_AUTO_THREAD
    with PAPER_AUTO_LOCK:
        PAPER_AUTO_STATE.update({
            'running': True,
            'started_at': utc_now_naive().isoformat(),
            'last_run_at': None,
            'last_checked_at': None,
            'last_message': 'Paper auto scheduler started.',
            'last_error': None,
            'last_orders': 0,
            'check_count': 0,
            'last_result': None,
            'recent_runs': [],
        })
    try:
        while not PAPER_AUTO_STOP_EVENT.is_set():
            try:
                result = run_paper_auto_cycle(trigger='paper-auto')
                compact = {
                    'message': result.get('message'),
                    'started_at': result.get('started_at'),
                    'finished_at': result.get('finished_at'),
                    'orders_created': result.get('orders_created', 0),
                    'exits_closed': result.get('exits_closed', 0),
                    'skipped': (result.get('skipped') or [])[:10],
                    'daily_week_count': len(result.get('daily_week') or []),
                    'intraday_count': len(result.get('intraday') or []),
                }
                with PAPER_AUTO_LOCK:
                    recent = list(PAPER_AUTO_STATE.get('recent_runs') or [])
                    recent.insert(0, compact)
                    PAPER_AUTO_STATE.update({
                        'last_run_at': utc_now_naive().isoformat(),
                        'last_checked_at': utc_now_naive().isoformat(),
                        'last_message': result.get('message'),
                        'last_error': None,
                        'last_orders': int(result.get('orders_created') or 0),
                        'check_count': int(PAPER_AUTO_STATE.get('check_count') or 0) + 1,
                        'last_result': compact,
                        'recent_runs': recent[:10],
                    })
            except Exception as e:
                with PAPER_AUTO_LOCK:
                    PAPER_AUTO_STATE.update({'last_error': str(e), 'last_message': 'Paper auto cycle failed.', 'last_result': {'error': str(e)}})
            if _sleep_with_stop_paper(PAPER_AUTO_INTERVAL_SECONDS):
                break
    finally:
        with PAPER_AUTO_LOCK:
            PAPER_AUTO_STATE['running'] = False
            PAPER_AUTO_STATE['last_message'] = PAPER_AUTO_STATE.get('last_message') or 'Paper auto scheduler stopped.'
        PAPER_AUTO_STOP_EVENT.clear()


def _sleep_with_stop_paper(seconds: int) -> bool:
    remaining = max(1, int(seconds))
    while remaining > 0:
        if PAPER_AUTO_STOP_EVENT.wait(timeout=min(5, remaining)):
            return True
        remaining -= min(5, remaining)
    return False


def start_paper_auto_worker() -> Dict[str, Any]:
    global PAPER_AUTO_THREAD
    already_running = False
    with PAPER_AUTO_LOCK:
        already_running = bool(PAPER_AUTO_THREAD and PAPER_AUTO_THREAD.is_alive())
        if not already_running:
            PAPER_AUTO_STOP_EVENT.clear()
            PAPER_AUTO_THREAD = threading.Thread(target=paper_auto_loop, daemon=True, name='paper-auto-scheduler')
            PAPER_AUTO_THREAD.start()
    if already_running:
        return {'ok': True, 'message': 'Paper auto scheduler is already running.', 'auto_state': get_paper_auto_state()}
    return {'ok': True, 'message': 'Paper auto scheduler started.', 'auto_state': get_paper_auto_state()}


def stop_paper_auto_worker() -> Dict[str, Any]:
    PAPER_AUTO_STOP_EVENT.set()
    with PAPER_AUTO_LOCK:
        PAPER_AUTO_STATE['last_message'] = 'Stop requested for paper auto scheduler.'
    return {'ok': True, 'message': 'Paper auto stop requested.', 'auto_state': get_paper_auto_state()}


def get_intraday_session_state(settings: Dict[str, Any]) -> Dict[str, Any]:
    return get_market_session_state(settings.get('market') or 'IN', settings.get('opening_wait_minutes'))


def get_intraday_entry_block_message(session: Dict[str, Any]) -> str:
    state = session.get('state')
    if state == 'before_open':
        return 'Market has not opened yet. Intraday auto is waiting; no entries are allowed yet. Exits were still checked.'
    if state == 'opening_wait':
        return 'Opening wait window is active. The engine is observing the first candles and will allow entries only after the wait period finishes. Exits were still checked.'
    if state == 'after_close':
        return 'Market is closed for this session. Intraday entries are stopped; exits were checked. Auto mode will stop after the close check.'
    if state == 'closed_non_trading_day':
        return 'Today is not a trading day for this market. Intraday entries are stopped; exits were checked. Auto mode will stay idle/stop.'
    return 'Intraday entries are blocked until the market is open and the opening wait window has finished. Exits were still checked.'


def get_intraday_today_bounds(market: str) -> Tuple[datetime.datetime, datetime.datetime]:
    sched = get_market_schedule(market)
    tz = ZoneInfo(sched['tz'])
    now_local = datetime.datetime.now(tz)
    local_day = now_local.date()
    start_local = datetime.datetime.combine(local_day, sched['open'], tzinfo=tz)
    end_local = datetime.datetime.combine(local_day, sched['close'], tzinfo=tz)
    return (
        start_local.astimezone(datetime.timezone.utc).replace(tzinfo=None),
        end_local.astimezone(datetime.timezone.utc).replace(tzinfo=None),
    )


def get_intraday_trade_count_today(settings: Dict[str, Any], mode: Optional[str] = None) -> int:
    conn = get_db_connection()
    if conn is None:
        return 0
    start, end = get_intraday_today_bounds(settings.get('market') or 'IN')
    params: List[Any] = [start, end]
    mode_filter = ''
    if mode:
        mode_filter = ' AND mode=%s'
        params.append(mode)
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT COUNT(*) FROM trade_orders
                WHERE source IN ('intraday-engine','paper-auto-intraday') AND side='BUY' AND created_at >= %s AND created_at < %s{mode_filter}
            """, params)
            return int(cur.fetchone()[0] or 0)
    finally:
        conn.close()


def fetch_intraday_candidate_snapshot(ticker: str, settings: Dict[str, Any]) -> Dict[str, Any]:
    market = settings.get('market') or 'IN'
    source_info = get_market_data_source_info(market, 'intraday_5m_candles')
    try:
        hist = yf.Ticker(ticker).history(period='1d', interval=INTRADAY_DATA_INTERVAL, timeout=YF_TIMEOUT)
        if hist is None or hist.empty or len(hist) < 3:
            return {'ok': False, 'ticker': ticker, 'reason': 'not_enough_intraday_bars', 'data_source': source_info, 'data_provider': source_info['actual_provider'], 'data_source_label': source_info['source_label']}
        close = hist['Close'].dropna()
        high = hist['High'].dropna()
        low = hist['Low'].dropna()
        opens = hist['Open'].dropna()
        vol = hist['Volume'].dropna() if 'Volume' in hist else None
        if close.empty or high.empty or low.empty or opens.empty:
            return {'ok': False, 'ticker': ticker, 'reason': 'incomplete_intraday_data', 'data_source': source_info, 'data_provider': source_info['actual_provider'], 'data_source_label': source_info['source_label']}
        last = float(close.iloc[-1])
        open_price = float(opens.iloc[0])
        session_high = float(high.max())
        session_low = float(low.min())
        wait_bars = max(1, int(math.ceil(float(settings.get('opening_wait_minutes') or 15) / max(1, get_intraday_interval_minutes()))))
        opening_slice = hist.head(wait_bars)
        opening_high = float(opening_slice['High'].max())
        opening_low = float(opening_slice['Low'].min())
        recent = [float(x) for x in close.tail(4).tolist()]
        trend = 'flat'
        if len(recent) >= 3:
            if recent[-1] > recent[-2] > recent[-3]:
                trend = 'rising'
            elif recent[-1] < recent[-2] < recent[-3]:
                trend = 'falling'
        vwap = None
        avg_bar_volume = None
        last_bar_volume = None
        volume_multiplier = None
        volume_data_available = False
        volume_data_reliable = False
        try:
            if vol is not None and not vol.empty and float(vol.sum()) > 0:
                volume_data_available = True
                typical = (hist['High'] + hist['Low'] + hist['Close']) / 3
                vwap = float((typical * hist['Volume']).sum() / hist['Volume'].sum())
                avg_bar_volume = float(vol.tail(20).mean())
                last_bar_volume = float(vol.iloc[-1])
                if avg_bar_volume and avg_bar_volume > 0:
                    volume_multiplier = (last_bar_volume / avg_bar_volume)
                    volume_data_reliable = True
            else:
                # Some free NSE Yahoo candles return zero/missing volume intraday.
                # Do not treat missing provider volume as proof that demand is absent.
                volume_data_available = False
                volume_data_reliable = False
        except Exception:
            volume_data_available = False
            volume_data_reliable = False
        candle_meta = get_intraday_last_candle_meta(hist, market)
        change_pct = ((last - open_price) / open_price) * 100 if open_price else 0
        above_vwap = bool(vwap and last > vwap)
        breakout = last >= opening_high * 1.001
        near_high = session_high > 0 and last >= session_high * 0.995
        not_chasing = change_pct <= float(settings.get('max_chase_pct') or INTRADAY_MAX_CHASE_PCT)
        # Intraday score is a confirmation score, not just percentage gain.
        # Higher % move helps, but a fading stock, missing breakout, or over-chased move
        # should not rank above a cleaner setup just because it is already up more.
        score_breakdown = []
        score = 0.0
        if change_pct > 0:
            score += 10
            score_breakdown.append({'factor': 'positive_move', 'points': 10, 'detail': 'price is above session open'})
        move_points = min(24, max(0, change_pct) * 9)
        if move_points:
            score += move_points
            score_breakdown.append({'factor': 'move_strength', 'points': round(move_points, 1), 'detail': f'{round(change_pct, 2)}% from open'})
        if above_vwap:
            score += 18
            score_breakdown.append({'factor': 'above_vwap', 'points': 18, 'detail': 'last price above VWAP'})
        if breakout:
            score += 22
            score_breakdown.append({'factor': 'opening_breakout', 'points': 22, 'detail': 'above opening range high'})
        elif near_high:
            score += 10
            score_breakdown.append({'factor': 'near_day_high', 'points': 10, 'detail': 'near session high but not confirmed breakout'})
        if trend == 'rising':
            score += 15
            score_breakdown.append({'factor': 'recent_trend_rising', 'points': 15, 'detail': 'last candles rising'})
        elif trend == 'falling':
            score -= 18
            score_breakdown.append({'factor': 'recent_trend_falling', 'points': -18, 'detail': 'recent candles falling/fading'})
        if volume_multiplier is not None:
            volume_points = min(13, max(0, (volume_multiplier - 1) * 10))
            if volume_points:
                score += volume_points
                score_breakdown.append({'factor': 'volume_expansion', 'points': round(volume_points, 1), 'detail': f'{round(volume_multiplier, 2)}x recent average'})
        if not not_chasing:
            score -= 20
            score_breakdown.append({'factor': 'chase_penalty', 'points': -20, 'detail': f'move {round(change_pct, 2)}% above max chase limit'})
        blocked_reasons = []
        if change_pct < float(settings.get('min_price_change_pct') or 0):
            blocked_reasons.append('weak_open_to_now_move')
        if settings.get('require_vwap') and not above_vwap:
            blocked_reasons.append('below_vwap')
        if settings.get('require_opening_breakout') and not breakout:
            blocked_reasons.append('no_opening_range_breakout')
        caution_reasons = []
        min_vol = float(settings.get('min_volume_multiplier') or 0)
        if min_vol > 0 and volume_data_reliable and volume_multiplier is not None and volume_multiplier < min_vol:
            if INTRADAY_VOLUME_CONFIRMATION_MODE == 'hard':
                blocked_reasons.append('volume_spike_missing')
            else:
                caution_reasons.append(f'volume_soft_warning: {round(volume_multiplier, 2)}x; target {min_vol}x')
        elif min_vol > 0 and not volume_data_reliable:
            caution_reasons.append('volume_data_unavailable_from_provider_not_blocking')
        if INTRADAY_BLOCK_STALE_DATA and candle_meta.get('is_stale'):
            blocked_reasons.append(candle_meta.get('freshness_note') or 'stale_or_delayed_intraday_data')
        if not not_chasing:
            blocked_reasons.append('too_extended_do_not_chase')
        if trend == 'falling':
            blocked_reasons.append('recent_momentum_fading')
        row = {
            'ok': True,
            'ticker': ticker,
            'market': market,
            'last': round(last, 2),
            'open': round(open_price, 2),
            'high': round(session_high, 2),
            'low': round(session_low, 2),
            'opening_high': round(opening_high, 2),
            'opening_low': round(opening_low, 2),
            'vwap': round(vwap, 2) if vwap else None,
            'change_pct': round(change_pct, 2),
            'volume_multiplier': round(volume_multiplier, 2) if volume_multiplier is not None else None,
            'volume_data_available': volume_data_available,
            'volume_data_reliable': volume_data_reliable,
            'volume_confirmation_mode': INTRADAY_VOLUME_CONFIRMATION_MODE,
            'last_bar_volume': round(last_bar_volume, 0) if last_bar_volume is not None else None,
            'avg_bar_volume': round(avg_bar_volume, 0) if avg_bar_volume is not None else None,
            'recent_trend': trend,
            'breakout': breakout,
            'above_vwap': above_vwap,
            'score': round(max(0, min(100, score)), 1),
            'score_breakdown': score_breakdown,
            'score_explanation': '; '.join([f"{x.get('factor')}: {x.get('points')}" for x in score_breakdown[:8]]),
            'blocked_reasons': blocked_reasons,
            'caution_reasons': caution_reasons,
            'bars': int(len(hist)),
            'interval': INTRADAY_DATA_INTERVAL,
            **candle_meta,
            'data_source': source_info,
            'data_provider': source_info['actual_provider'],
            'data_source_label': source_info['source_label'],
            'data_freshness': source_info['freshness'],
            'fetched_at': source_info['fetched_at'],
        }
        return attach_intraday_trade_metrics(row, settings)
    except Exception as e:
        return {'ok': False, 'ticker': ticker, 'reason': str(e), 'data_source': source_info, 'data_provider': source_info['actual_provider'], 'data_source_label': source_info['source_label']}


def score_intraday_prescan_candidate(row: Dict[str, Any], settings: Dict[str, Any]) -> float:
    """Rank live intraday symbols before the heavier strict-learning/final entry pass.

    The engine is buy-only, so this favors upside momentum, confirmed volume,
    VWAP strength and opening-range pressure. It also gives a smaller score to
    volatility/activity so the shortlist is not permanently tied to a fixed
    first-60 universe.
    """
    try:
        change = float(row.get('change_pct') or 0)
        high = float(row.get('high') or 0)
        low = float(row.get('low') or 0)
        open_price = float(row.get('open') or 0)
        range_pct = ((high - low) / open_price) * 100 if open_price else 0.0
        volume_multiplier = float(row.get('volume_multiplier') or 0)
    except Exception:
        change, range_pct, volume_multiplier = 0.0, 0.0, 0.0
    score = 0.0
    score += min(32.0, max(0.0, change) * 9.0)
    score += min(18.0, max(0.0, range_pct) * 4.0)
    score += min(18.0, max(0.0, volume_multiplier - 1.0) * 9.0)
    if row.get('above_vwap'):
        score += 14.0
    if row.get('breakout'):
        score += 22.0
    elif row.get('high') and row.get('last') and float(row.get('last') or 0) >= float(row.get('high') or 0) * 0.995:
        score += 8.0
    trend = row.get('recent_trend')
    if trend == 'rising':
        score += 12.0
    elif trend == 'falling':
        score -= 16.0
    max_chase = float(settings.get('max_chase_pct') or INTRADAY_MAX_CHASE_PCT)
    if change > max_chase:
        score -= min(25.0, (change - max_chase) * 10.0)
    if change < 0:
        score -= min(25.0, abs(change) * 8.0)
    return round(max(0.0, min(100.0, score)), 1)


def build_intraday_dynamic_shortlist(settings: Dict[str, Any]) -> Dict[str, Any]:
    market = settings.get('market') or 'IN'
    configured_universe = load_market_universe(market)
    shortlist_limit = max(1, int(settings.get('universe_limit') or INTRADAY_UNIVERSE_LIMIT))
    pre_pool_limit = max(shortlist_limit, int(os.environ.get('INTRADAY_PRE_SCAN_POOL_LIMIT', INTRADAY_PRE_SCAN_POOL_LIMIT)))
    pool = configured_universe[:min(len(configured_universe), pre_pool_limit)]
    candidates: List[Dict[str, Any]] = []
    errors: List[str] = []
    if not pool:
        return {'candidates': [], 'errors': ['No symbols found in configured market universe.'], 'metadata': {'selection_mode': 'dynamic', 'pre_scan_pool_count': 0, 'shortlist_limit': shortlist_limit}}
    workers = min(max(1, INTRADAY_MAX_WORKERS), max(1, len(pool)))
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {executor.submit(fetch_intraday_candidate_snapshot, t, settings): t for t in pool}
        for fut in concurrent.futures.as_completed(future_map):
            t = future_map[fut]
            try:
                row = fut.result()
                if row.get('ok'):
                    row['pre_scan_score'] = score_intraday_prescan_candidate(row, settings)
                    candidates.append(row)
                else:
                    errors.append(f"{t}: {row.get('reason') or row.get('error')}")
            except Exception as e:
                errors.append(f"{t}: {e}")
    candidates.sort(key=lambda r: (float(r.get('pre_scan_score') or 0), float(r.get('score') or 0), float(r.get('change_pct') or 0)), reverse=True)
    return {
        'candidates': candidates[:shortlist_limit],
        'errors': errors[:20],
        'metadata': {
            'selection_mode': 'dynamic_prescan',
            'configured_universe_count': len(configured_universe),
            'pre_scan_pool_count': len(pool),
            'shortlist_limit': shortlist_limit,
            'shortlisted_count': min(len(candidates), shortlist_limit),
            'not_random': True,
            'ranking': 'pre_scan_score = upside move + volume expansion + volatility/range + VWAP + opening-range breakout + recent candle trend; then strict intraday rules decide eligibility.',
        }
    }




def get_intraday_price_bounds(settings: Dict[str, Any], market: str) -> Dict[str, float]:
    market_code = str(market or settings.get('market') or 'IN').upper()
    if market_code == 'IN':
        default_min = INTRADAY_MIN_STOCK_PRICE_INR
        default_max = INTRADAY_MAX_STOCK_PRICE_INR
        default_min_value = INTRADAY_MIN_POSITION_VALUE_INR
        min_key, max_key, pos_key = 'min_stock_price_inr', 'max_stock_price_inr', 'min_position_value_inr'
    else:
        default_min = INTRADAY_MIN_STOCK_PRICE_USD
        default_max = INTRADAY_MAX_STOCK_PRICE_USD
        default_min_value = INTRADAY_MIN_POSITION_VALUE_USD
        min_key, max_key, pos_key = 'min_stock_price_usd', 'max_stock_price_usd', 'min_position_value_usd'
    min_price = float(settings.get(min_key) if settings.get(min_key) is not None else default_min)
    max_price = float(settings.get(max_key) if settings.get(max_key) is not None else default_max)
    min_position_value = float(settings.get(pos_key) if settings.get(pos_key) is not None else default_min_value)
    if max_price and min_price and max_price < min_price:
        max_price = min_price
    return {
        'enabled': bool(settings.get('price_filter_enabled', INTRADAY_PRICE_FILTER_ENABLED)),
        'min_price': min_price,
        'max_price': max_price,
        'min_quantity': int(settings.get('min_order_quantity') or INTRADAY_MIN_ORDER_QUANTITY),
        'min_position_value': min_position_value,
    }


def apply_intraday_price_focus_filter(candidate: Dict[str, Any], settings: Dict[str, Any]) -> None:
    """Mark candidates outside the low-capital cost-efficient price/size band.

    This prevents high-value qty-1 trades such as EICHERMOT/BAJAJ-AUTO from
    consuming slots when capital is small. It also avoids very cheap/penny names
    where spread/liquidity can make paper fills unrealistic.
    """
    market = candidate.get('market') or settings.get('market') or 'IN'
    bounds = get_intraday_price_bounds(settings, market)
    if not bounds.get('enabled'):
        return
    price = float(candidate.get('last') or candidate.get('entry_price') or 0)
    if price <= 0:
        candidate.setdefault('blocked_reasons', []).append('price_filter: latest price unavailable')
        return
    sym = get_currency_info(market, candidate.get('ticker')).get('symbol', '₹')
    min_price = float(bounds.get('min_price') or 0)
    max_price = float(bounds.get('max_price') or 0)
    if min_price > 0 and price < min_price:
        candidate.setdefault('blocked_reasons', []).append(f'price_filter: price {sym}{price:.2f} below minimum {sym}{min_price:.2f}')
    if max_price > 0 and price > max_price:
        candidate.setdefault('blocked_reasons', []).append(f'price_filter: price {sym}{price:.2f} above maximum {sym}{max_price:.2f}')


def apply_intraday_size_efficiency_gate(candidate: Dict[str, Any], sizing: Dict[str, Any], settings: Dict[str, Any]) -> List[str]:
    market = candidate.get('market') or settings.get('market') or 'IN'
    bounds = get_intraday_price_bounds(settings, market)
    if not bounds.get('enabled'):
        return []
    qty = int(sizing.get('quantity') or 0)
    price = float(candidate.get('last') or candidate.get('entry_price') or 0)
    position_value = qty * price
    sym = get_currency_info(market, candidate.get('ticker')).get('symbol', '₹')
    reasons = []
    min_qty = int(bounds.get('min_quantity') or 0)
    min_pos = float(bounds.get('min_position_value') or 0)
    if min_qty > 0 and qty < min_qty:
        reasons.append(f'size_filter: quantity {qty} below minimum {min_qty}; avoids high-price qty-1/2 trades')
    if min_pos > 0 and position_value < min_pos:
        reasons.append(f'size_filter: position value {sym}{position_value:.2f} below minimum {sym}{min_pos:.2f}; target profit may not cover costs')
    return reasons

def scan_intraday_candidates(settings: Dict[str, Any]) -> Dict[str, Any]:
    market = settings.get('market') or 'IN'
    source_info = get_market_data_source_info(market, 'intraday_5m_candles')
    configured_universe = load_market_universe(market)
    dynamic_enabled = bool(INTRADAY_DYNAMIC_UNIVERSE_ENABLED)
    if dynamic_enabled:
        shortlist = build_intraday_dynamic_shortlist(settings)
        candidates = shortlist.get('candidates') or []
        errors = shortlist.get('errors') or []
        selection_metadata = shortlist.get('metadata') or {}
        universe_count_for_display = int(selection_metadata.get('pre_scan_pool_count') or len(configured_universe))
    else:
        universe = configured_universe[:int(settings.get('universe_limit') or INTRADAY_UNIVERSE_LIMIT)]
        candidates: List[Dict[str, Any]] = []
        errors: List[str] = []
        workers = min(max(1, INTRADAY_MAX_WORKERS), max(1, len(universe)))
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            future_map = {executor.submit(fetch_intraday_candidate_snapshot, t, settings): t for t in universe}
            for fut in concurrent.futures.as_completed(future_map):
                t = future_map[fut]
                try:
                    row = fut.result()
                    if row.get('ok'):
                        row['pre_scan_score'] = score_intraday_prescan_candidate(row, settings)
                        candidates.append(row)
                    else:
                        errors.append(f"{t}: {row.get('reason') or row.get('error')}")
                except Exception as e:
                    errors.append(f"{t}: {e}")
        selection_metadata = {'selection_mode': 'fixed_universe_limit', 'configured_universe_count': len(configured_universe), 'pre_scan_pool_count': len(universe), 'shortlist_limit': len(universe), 'not_random': True}
        universe_count_for_display = len(universe)
    intraday_history_memory = get_closed_trade_history_strength(market, 'intraday', engine='intraday')
    market_context = get_market_context(market)
    for c in candidates:
        apply_intraday_price_focus_filter(c, settings)
        if c.get('blocked_reasons'):
            # Price/size focus is a hard pre-entry filter; keep the row visible in watchlist
            # but do not waste strict-learning checks on high-price qty-1 setups.
            continue
        review = strict_learning_review(c, market, 'intraday', engine='intraday', market_context=market_context, trade_memory=intraday_history_memory)
        c['strict_learning'] = review
        if review.get('enabled') and not review.get('allowed'):
            reasons = c.setdefault('blocked_reasons', [])
            reasons.append('strict_learning_no_trade: ' + '; '.join((review.get('hard_blocks') or review.get('cautions') or [])[:2]))
        elif review.get('enabled') and review.get('score_adjustment'):
            c['score'] = round(max(0, min(100, float(c.get('score') or 0) + float(review.get('score_adjustment') or 0))), 1)
    candidates.sort(key=lambda r: (float(r.get('score') or 0), float(r.get('change_pct') or 0)), reverse=True)
    min_score_gate = float(settings.get('min_score') or 0)
    eligible = [c for c in candidates if not c.get('blocked_reasons') and float(c.get('score') or 0) >= min_score_gate]
    # The dashboard shows eligible + watch rows in one table. Previously, watch rows
    # that had no hard block but were below the minimum score displayed as "Eligible".
    # Mark them clearly so a 58.5 score never appears eligible when the UI says 60+.
    for c in candidates:
        if c not in eligible and not c.get('blocked_reasons') and float(c.get('score') or 0) < min_score_gate:
            c.setdefault('blocked_reasons', []).append(f"watch_only_below_min_score: score {round(float(c.get('score') or 0), 1)}; needs {min_score_gate}+")
    watch = [c for c in candidates if c not in eligible][:int(settings.get('max_candidates') or 8)]
    return {
        'market': market,
        'universe_count': universe_count_for_display,
        'configured_universe_count': len(configured_universe),
        'selection_metadata': selection_metadata,
        # Display the broader pre-scan pool as scanned_count; candidates is only the final dynamic shortlist.
        'scanned_count': universe_count_for_display,
        'shortlist_count': len(candidates),
        'eligible_count': len(eligible),
        'eligible': eligible[:int(settings.get('max_candidates') or 8)],
        'watch': watch,
        'errors': errors[:20],
        'data_source': source_info,
        'data_provider': source_info['actual_provider'],
        'data_source_label': source_info['source_label'],
        'data_freshness': source_info['freshness'],
        'market_context': market_context,
        'macro_event_risk': (market_context or {}).get('macro_event_risk') if isinstance(market_context, dict) else None,
        'learning_architecture': strict_learning_architecture(),
        'note': f"Intraday scan uses dynamic pre-scan + deterministic intraday candle rules plus intraday trade-history learning. It is not random and no Claude call is made. Data source: {source_info['source_label']} ({source_info['freshness']}).",
    }


def _money_round(value: Any) -> float:
    try:
        return round(float(value or 0), 2)
    except Exception:
        return 0.0


def _pct_amount(value: float, pct: float) -> float:
    return float(value or 0) * float(pct or 0) / 100.0


def _brokerage_amount(turnover: float, market: str = 'IN') -> float:
    if str(market or 'IN').upper() == 'IN':
        percent_cost = _pct_amount(turnover, INDIA_INTRADAY_BROKERAGE_PCT)
        fixed_cost = INDIA_INTRADAY_BROKERAGE_PER_ORDER_INR
        if INDIA_INTRADAY_BROKERAGE_USE_LOWER_OF:
            return min(fixed_cost, percent_cost)
        return fixed_cost if fixed_cost > 0 else percent_cost
    return US_INTRADAY_COMMISSION_PER_ORDER_USD


def get_min_expected_net_profit(market: str) -> float:
    return INTRADAY_MIN_EXPECTED_NET_PROFIT_INR if str(market or 'IN').upper() == 'IN' else INTRADAY_MIN_EXPECTED_NET_PROFIT_USD


def get_min_net_profit_to_protect(market: str) -> float:
    return INTRADAY_MIN_NET_PROFIT_TO_PROTECT_INR if str(market or 'IN').upper() == 'IN' else INTRADAY_MIN_NET_PROFIT_TO_PROTECT_USD


def estimate_round_trip_cost(entry_price: float, exit_price: float, quantity: int, market: str = 'IN') -> Dict[str, Any]:
    """Return conservative round-trip trading cost estimate for live/paper P&L."""
    qty = max(0, int(quantity or 0))
    entry = float(entry_price or 0)
    exit_px = float(exit_price or entry or 0)
    market_code = str(market or 'IN').upper()
    buy_turnover = max(entry * qty, 0.0)
    sell_turnover = max(exit_px * qty, 0.0)
    turnover = buy_turnover + sell_turnover
    if qty <= 0 or entry <= 0 or turnover <= 0 or not TRADE_COST_MODEL_ENABLED:
        return {'enabled': bool(TRADE_COST_MODEL_ENABLED), 'market': market_code, 'total_cost': 0.0, 'currency_symbol': get_currency_info(market_code).get('symbol', '₹')}

    if market_code == 'IN':
        brokerage_buy = _brokerage_amount(buy_turnover, 'IN')
        brokerage_sell = _brokerage_amount(sell_turnover, 'IN')
        brokerage = brokerage_buy + brokerage_sell
        stt = _pct_amount(sell_turnover, INDIA_INTRADAY_STT_SELL_PCT)
        exchange = _pct_amount(turnover, INDIA_INTRADAY_EXCHANGE_TXN_PCT)
        sebi = _pct_amount(turnover, INDIA_INTRADAY_SEBI_PCT)
        stamp = _pct_amount(buy_turnover, INDIA_INTRADAY_STAMP_DUTY_BUY_PCT)
        gst = _pct_amount(brokerage + exchange + sebi, INDIA_INTRADAY_GST_PCT)
        slippage = turnover * float(INTRADAY_COST_SLIPPAGE_BPS or 0) / 10000.0
        calculated = brokerage + stt + exchange + sebi + stamp + gst + slippage
        total = max(calculated, INDIA_INTRADAY_MIN_ROUND_TRIP_COST_INR if INDIA_INTRADAY_MIN_ROUND_TRIP_COST_INR > 0 else 0)
        return {
            'enabled': True,
            'market': market_code,
            'currency_symbol': '₹',
            'buy_turnover': round(buy_turnover, 2),
            'sell_turnover': round(sell_turnover, 2),
            'turnover': round(turnover, 2),
            'brokerage': round(brokerage, 2),
            'stt': round(stt, 2),
            'exchange_txn': round(exchange, 2),
            'sebi': round(sebi, 2),
            'stamp_duty': round(stamp, 2),
            'gst': round(gst, 2),
            'slippage_reserve': round(slippage, 2),
            'minimum_round_trip_cost': round(INDIA_INTRADAY_MIN_ROUND_TRIP_COST_INR, 2),
            'total_cost': round(total, 2),
        }

    commission = max(0.0, US_INTRADAY_COMMISSION_PER_ORDER_USD * 2)
    bps_cost = turnover * float(US_INTRADAY_COST_BPS or 0) / 10000.0
    slippage = turnover * float(INTRADAY_COST_SLIPPAGE_BPS or 0) / 10000.0
    calculated = commission + bps_cost + slippage
    total = max(calculated, US_INTRADAY_MIN_ROUND_TRIP_COST_USD if US_INTRADAY_MIN_ROUND_TRIP_COST_USD > 0 else 0)
    return {
        'enabled': True,
        'market': market_code,
        'currency_symbol': '$',
        'buy_turnover': round(buy_turnover, 2),
        'sell_turnover': round(sell_turnover, 2),
        'turnover': round(turnover, 2),
        'commission': round(commission, 2),
        'bps_cost': round(bps_cost, 2),
        'slippage_reserve': round(slippage, 2),
        'minimum_round_trip_cost': round(US_INTRADAY_MIN_ROUND_TRIP_COST_USD, 2),
        'total_cost': round(total, 2),
    }


def calculate_net_trade_pnl(entry_price: float, exit_price: float, quantity: int, market: str = 'IN') -> Dict[str, Any]:
    entry = float(entry_price or 0)
    exit_px = float(exit_price or entry or 0)
    qty = int(quantity or 0)
    gross = round((exit_px - entry) * qty, 2)
    cost = estimate_round_trip_cost(entry, exit_px, qty, market)
    total_cost = float(cost.get('total_cost') or 0)
    net = round(gross - total_cost, 2)
    buy_value = entry * qty
    price_move_pct = round(((exit_px - entry) / entry) * 100, 4) if entry else 0.0
    net_pct = round((net / buy_value) * 100, 4) if buy_value else 0.0
    gross_pct = round((gross / buy_value) * 100, 4) if buy_value else 0.0
    return {
        'gross_pnl': gross,
        'gross_pnl_pct': gross_pct,
        'cost_amount': round(total_cost, 2),
        'net_pnl': net,
        'net_pnl_pct': net_pct,
        'price_move_pct': price_move_pct,
        'cost_breakdown': cost,
    }


def intraday_cost_gate(entry: float, target: float, quantity: int, market: str, settings: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    gross_target_profit = round((float(target or 0) - float(entry or 0)) * int(quantity or 0), 2)
    cost = estimate_round_trip_cost(entry, target, quantity, market)
    total_cost = float(cost.get('total_cost') or 0)
    expected_net = round(gross_target_profit - total_cost, 2)
    ratio = round((gross_target_profit / total_cost), 2) if total_cost > 0 else 999.0
    settings = settings or {}
    market_code = str(market or 'IN').upper()
    if market_code == 'IN':
        min_net = float(settings.get('min_expected_net_profit_inr') if settings.get('min_expected_net_profit_inr') is not None else get_min_expected_net_profit(market))
    else:
        min_net = float(settings.get('min_expected_net_profit_usd') if settings.get('min_expected_net_profit_usd') is not None else get_min_expected_net_profit(market))
    min_ratio = float(settings.get('min_gross_profit_to_cost_ratio') if settings.get('min_gross_profit_to_cost_ratio') is not None else INTRADAY_MIN_GROSS_PROFIT_TO_COST_RATIO or 0)
    ok = True
    reasons = []
    if TRADE_COST_MODEL_ENABLED and expected_net < min_net:
        ok = False
        reasons.append(f"expected net target profit {cost.get('currency_symbol','₹')}{expected_net:.2f} is below minimum {cost.get('currency_symbol','₹')}{min_net:.2f}")
    if TRADE_COST_MODEL_ENABLED and ratio < min_ratio:
        ok = False
        reasons.append(f"gross target profit/cost ratio {ratio}:1 is below minimum {min_ratio}:1")
    return {
        'ok': ok,
        'reasons': reasons,
        'gross_target_profit': gross_target_profit,
        'expected_round_trip_cost': round(total_cost, 2),
        'expected_net_profit_at_target': expected_net,
        'profit_to_cost_ratio': ratio,
        'min_expected_net_profit': min_net,
        'min_profit_to_cost_ratio': min_ratio,
        'cost_breakdown': cost,
    }


def explain_intraday_plan_skip(candidate: Dict[str, Any], trading_settings: Dict[str, Any], intraday_settings: Dict[str, Any]) -> str:
    entry = float(candidate.get('last') or 0)
    if entry <= 0:
        return 'No valid latest price for intraday paper order.'
    market = candidate.get('market') or intraday_settings.get('market') or 'IN'
    max_stop_pct = float(intraday_settings.get('max_stop_loss_pct') or INTRADAY_MAX_STOP_LOSS_PCT)
    min_stop_pct = min(max_stop_pct, max(0.0, float(INTRADAY_MIN_STOP_LOSS_PCT or 0)))
    stop = entry * (1 - max_stop_pct / 100)
    if min_stop_pct > 0:
        stop = min(stop, entry * (1 - min_stop_pct / 100))
    risk_per_share = max(entry - stop, 0.01)
    quick_target = entry * (1 + float(intraday_settings.get('quick_target_pct') or INTRADAY_QUICK_TARGET_PCT) / 100)
    rr_target = entry + risk_per_share * float(intraday_settings.get('min_risk_reward') or INTRADAY_MIN_RISK_REWARD)
    target = max(quick_target, rr_target)
    rr = (target - entry) / risk_per_share if risk_per_share > 0 else 0
    if rr < float(intraday_settings.get('min_risk_reward') or 0):
        return f"Intraday reward/risk {rr:.2f}:1 is below minimum {intraday_settings.get('min_risk_reward')}:1."
    sizing = compute_integer_position_size(entry, stop, trading_settings, market)
    if sizing['quantity'] <= 0:
        return sizing['skip_reason']
    size_reasons = apply_intraday_size_efficiency_gate(candidate, sizing, intraday_settings)
    if size_reasons:
        return 'Size filter skipped trade: ' + '; '.join(size_reasons)
    cost_gate = intraday_cost_gate(entry, target, int(sizing.get('quantity') or 0), market, intraday_settings)
    if not cost_gate.get('ok'):
        sym = get_currency_info(market, candidate.get('ticker')).get('symbol', '₹')
        return 'Cost filter skipped trade: ' + '; '.join(cost_gate.get('reasons') or []) + f". Gross target {sym}{cost_gate.get('gross_target_profit')}, estimated costs {sym}{cost_gate.get('expected_round_trip_cost')}."
    return 'Intraday trade plan failed final safety checks.'


def build_intraday_plan(candidate: Dict[str, Any], trading_settings: Dict[str, Any], intraday_settings: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    entry = float(candidate.get('last') or 0)
    if entry <= 0:
        return None
    market = candidate.get('market') or intraday_settings.get('market') or 'IN'
    max_stop_pct = float(intraday_settings.get('max_stop_loss_pct') or INTRADAY_MAX_STOP_LOSS_PCT)
    stop_by_pct = entry * (1 - max_stop_pct / 100)
    min_stop_pct = min(max_stop_pct, max(0.0, float(INTRADAY_MIN_STOP_LOSS_PCT or 0)))
    opening_low = float(candidate.get('opening_low') or stop_by_pct)
    vwap = float(candidate.get('vwap') or stop_by_pct)
    # Keep the intraday stop controlled, but never so tight that a one-tick/noise
    # move creates fake breakeven stops. The old engine sometimes rounded stop to
    # entry, then exited tiny gross wins/losses that could not cover costs.
    structural_stop = min(vwap * 0.998, opening_low * 0.998) if vwap and opening_low else stop_by_pct
    stop = max(stop_by_pct, structural_stop)
    if min_stop_pct > 0:
        stop = min(stop, entry * (1 - min_stop_pct / 100))
    if stop >= entry:
        stop = stop_by_pct
    risk_per_share = max(entry - stop, 0.01)
    quick_target = entry * (1 + float(intraday_settings.get('quick_target_pct') or INTRADAY_QUICK_TARGET_PCT) / 100)
    rr_target = entry + risk_per_share * float(intraday_settings.get('min_risk_reward') or INTRADAY_MIN_RISK_REWARD)
    target = max(quick_target, rr_target)
    rr = (target - entry) / risk_per_share if risk_per_share > 0 else 0
    if rr < float(intraday_settings.get('min_risk_reward') or 0):
        return None
    sizing = compute_integer_position_size(entry, stop, trading_settings, market)
    quantity = int(sizing.get('quantity') or 0)
    if quantity <= 0:
        return None
    size_reasons = apply_intraday_size_efficiency_gate(candidate, sizing, intraday_settings)
    if size_reasons:
        candidate.setdefault('blocked_reasons', []).extend(size_reasons)
        candidate['blocked_reason'] = 'size_filter_no_edge: ' + '; '.join(size_reasons)
        return None
    cost_gate = intraday_cost_gate(entry, target, quantity, market, intraday_settings)
    candidate['cost_gate'] = cost_gate
    if not cost_gate.get('ok'):
        candidate.setdefault('blocked_reasons', []).extend(cost_gate.get('reasons') or [])
        candidate['blocked_reason'] = 'cost_filter_no_edge: ' + '; '.join(cost_gate.get('reasons') or [])
        return None
    currency = get_currency_info(market, candidate.get('ticker'))
    return {
        'market': market,
        'horizon': 'intraday',
        'ticker': candidate['ticker'],
        'company': candidate.get('company') or candidate['ticker'],
        'side': 'BUY',
        'quantity': quantity,
        'order_type': 'LIMIT',
        'limit_price': round(entry, 2),
        'stop_price': round(stop, 2),
        'target_price': round(target, 2),
        'estimated_value': round(quantity * entry, 2),
        'currency_symbol': currency['symbol'],
        'rationale': f"Intraday bullish candidate: score {candidate.get('score')}, change {candidate.get('change_pct')}%, trend {candidate.get('recent_trend')}, VWAP confirmation {candidate.get('above_vwap')}, opening breakout {candidate.get('breakout')}.",
        'risk_notes': f"Quick intraday entry with max stop about {max_stop_pct}% and target about {float(intraday_settings.get('quick_target_pct') or 0)}%+. Uses {currency['symbol']}{sizing.get('capital')} simulation capital for {market}. Forced exit before close is enabled from the Intraday page.",
        'order_payload': {
            'engine': 'intraday',
            'learning_bucket': f"{market}:intraday:intraday",
            'candidate': candidate,
            'entry': round(entry, 2),
            'target': round(target, 2),
            'stop': round(stop, 2),
            'risk_reward': round(rr, 2),
            'sizing': sizing,
            'cost_gate': cost_gate,
            'expected_round_trip_cost': cost_gate.get('expected_round_trip_cost'),
            'expected_net_profit_at_target': cost_gate.get('expected_net_profit_at_target'),
            'gross_target_profit': cost_gate.get('gross_target_profit'),
            'profit_to_cost_ratio': cost_gate.get('profit_to_cost_ratio'),
            'profit_book_pct': max(float(intraday_settings.get('profit_book_pct') or 0), INTRADAY_PROFIT_BOOK_PCT),
            'force_exit_before_close_minutes': int(intraday_settings.get('force_exit_before_close_minutes') or INTRADAY_FORCE_EXIT_BEFORE_CLOSE_MINUTES),
        }
    }


def _compact_intraday_plan_for_claude(item: Dict[str, Any]) -> Dict[str, Any]:
    plan = item.get('plan') or {}
    candidate = item.get('candidate') or {}
    payload = plan.get('order_payload') or {}
    cost_gate = payload.get('cost_gate') or candidate.get('cost_gate') or {}
    return {
        'ticker': plan.get('ticker') or candidate.get('ticker'),
        'market': plan.get('market') or candidate.get('market'),
        'score': candidate.get('score'),
        'move_from_open_pct': candidate.get('change_pct'),
        'last_price': candidate.get('last'),
        'vwap': candidate.get('vwap'),
        'above_vwap': candidate.get('above_vwap'),
        'opening_breakout': candidate.get('breakout'),
        'volume_multiplier': candidate.get('volume_multiplier'),
        'recent_trend': candidate.get('recent_trend'),
        'data_source': candidate.get('data_source_label') or candidate.get('data_provider'),
        'entry': plan.get('limit_price'),
        'target': plan.get('target_price'),
        'stop': plan.get('stop_price'),
        'quantity': plan.get('quantity'),
        'estimated_value': plan.get('estimated_value'),
        'risk_reward': payload.get('risk_reward'),
        'gross_target_profit': payload.get('gross_target_profit'),
        'expected_round_trip_cost': payload.get('expected_round_trip_cost'),
        'expected_net_profit_at_target': payload.get('expected_net_profit_at_target'),
        'profit_to_cost_ratio': payload.get('profit_to_cost_ratio'),
        'score_explanation': candidate.get('score_explanation'),
    }


def call_claude_intraday_controller(planned_items: List[Dict[str, Any]], intraday_settings: Dict[str, Any], trading_settings: Dict[str, Any], session: Dict[str, Any], goal_status: Dict[str, Any], trigger: str) -> Dict[str, Any]:
    """Ask Claude to choose/approve from already-safe intraday plans.

    Claude gets bounded control only after deterministic hard gates have produced
    valid plans. The response never places orders directly; it only marks plans
    ALLOW/BLOCK/WATCH and can suggest target/stop tweaks that are rechecked by
    the deterministic engine before any order is persisted.
    """
    mode = normalize_claude_intraday_mode(intraday_settings.get('claude_control_mode'))
    result = {
        'mode': mode,
        'enabled': mode != 'off',
        'used': False,
        'model': None,
        'error': None,
        'decisions': {},
        'raw_decisions': [],
        'note': None,
    }
    if mode == 'off' or not planned_items:
        return result
    max_reviews = min(max(1, int(intraday_settings.get('claude_max_reviews_per_run') or CLAUDE_INTRADAY_MAX_REVIEWS_PER_RUN)), 10, len(planned_items))
    compact = [_compact_intraday_plan_for_claude(x) for x in planned_items[:max_reviews]]
    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        result['error'] = 'ANTHROPIC_API_KEY not configured; Claude control cannot run.'
        result['note'] = 'Claude unavailable. In approval/full-control modes no new orders are allowed.'
        return result
    estimated_input_tokens = 3200 + len(compact) * 850
    estimated_output_tokens = min(CLAUDE_INTRADAY_MAX_TOKENS, 900 + len(compact) * 220)
    estimated_cost = calculate_claude_cost(estimated_input_tokens, estimated_output_tokens)
    budget = get_ai_budget_status(estimated_cost.get('cost_inr', 0))
    if not budget.get('allowed'):
        result['error'] = 'Claude budget guard blocked intraday control: ' + ', '.join(budget.get('reasons') or [])
        result['note'] = 'Claude unavailable due to AI budget guard. In approval/full-control modes no new orders are allowed.'
        return result
    today = datetime.datetime.now(ZoneInfo('Asia/Kolkata')).strftime('%A, %d %B %Y, %I:%M %p IST')
    min_conf = int(intraday_settings.get('claude_min_confidence') or CLAUDE_INTRADAY_MIN_CONFIDENCE)
    target_instruction = str(intraday_settings.get('claude_target_instruction') or CLAUDE_INTRADAY_TARGET_INSTRUCTION)[:1200]
    market = intraday_settings.get('market') or 'IN'
    prompt = f"""You are a bounded intraday trading controller inside a paper-trading risk engine.
Today/time: {today}
Market: {market}
Mode requested: {mode}
Trigger: {trigger}
User objective: {target_instruction}

IMPORTANT SAFETY AND CONTROL RULES:
- You are not allowed to guarantee profit.
- Prefer NO TRADE over weak trade.
- Only choose from the provided candidate plans; do not invent new tickers.
- The engine has already calculated entry, target, stop, quantity, expected total costs, and expected net profit after costs.
- You may suggest a tighter stop or more realistic target, but the engine will reject anything that breaks hard gates.
- Do not approve a trade if momentum is fading, target profit barely covers costs, data looks weak/stale, R/R is poor, or the setup is already chased.
- Minimum Claude confidence required by the engine: {min_conf}.
- Hard gates still enforced by engine after your response: fresh data, market hours, max trades, max open positions, mandatory stop-loss, cost coverage, minimum expected net profit, gross/cost ratio, daily/weekly loss guard.

Session: {json.dumps(session, default=_json_safe)}
Goal/risk guard: {json.dumps(goal_status, default=_json_safe)}
Intraday settings: {json.dumps({k: intraday_settings.get(k) for k in ['min_score','min_risk_reward','quick_target_pct','max_stop_loss_pct','min_expected_net_profit_inr','min_expected_net_profit_usd','min_gross_profit_to_cost_ratio','max_trades_per_day']}, default=_json_safe)}

Return ONLY valid JSON with this shape:
{{
  "summary": "one sentence",
  "decisions": [
    {{
      "rank": 1,
      "ticker": "SYMBOL",
      "decision": "ALLOW" | "BLOCK" | "WATCH",
      "confidence": 0-100,
      "reason": "short practical reason",
      "risk_flags": ["flag1"],
      "suggested_target_price": null or number,
      "suggested_stop_price": null or number,
      "max_hold_minutes": null or integer
    }}
  ]
}}

CANDIDATE_PLANS_JSON:
{json.dumps(compact, separators=(',', ':'), default=_json_safe)}
"""
    try:
        client = anthropic.Anthropic(api_key=api_key)
        model_name = os.environ.get('ANTHROPIC_MODEL', 'claude-sonnet-5')
        response = client.messages.create(
            model=model_name,
            max_tokens=CLAUDE_INTRADAY_MAX_TOKENS,
            messages=[{'role': 'user', 'content': prompt}],
        )
        usage = getattr(response, 'usage', None)
        if usage:
            log_api_cost(
                'anthropic_intraday_controller', market, 'intraday', model_name,
                int(getattr(usage, 'input_tokens', 0) or 0),
                int(getattr(usage, 'output_tokens', 0) or 0),
                notes='Claude bounded intraday control review',
                metadata={'candidate_count': len(compact), 'mode': mode, 'trigger': trigger}
            )
        text_blocks = [b.text for b in response.content if getattr(b, 'type', None) == 'text']
        if not text_blocks:
            raise RuntimeError(f'Claude returned no text content (stop_reason={getattr(response, "stop_reason", None)}).')
        text = re.sub(r'```json|```', '', text_blocks[0].strip()).strip()
        try:
            parsed = json.loads(text)
        except Exception:
            match = re.search(r'\{[\s\S]*\}', text)
            if not match:
                raise
            parsed = json.loads(match.group())
        raw_decisions = parsed.get('decisions') if isinstance(parsed, dict) else parsed
        if not isinstance(raw_decisions, list):
            raw_decisions = []
        decisions = {}
        allowed_tickers = {str(x.get('ticker') or '').upper() for x in compact}
        for i, d in enumerate(raw_decisions, start=1):
            if not isinstance(d, dict):
                continue
            ticker = str(d.get('ticker') or '').upper().strip()
            if not ticker or ticker not in allowed_tickers:
                continue
            decision = str(d.get('decision') or 'WATCH').upper().strip()
            if decision not in ('ALLOW', 'BLOCK', 'WATCH'):
                decision = 'WATCH'
            try:
                confidence = max(0, min(100, int(float(d.get('confidence') or 0))))
            except Exception:
                confidence = 0
            decisions[ticker] = {
                'rank': int(d.get('rank') or i) if str(d.get('rank') or '').replace('.','',1).isdigit() else i,
                'ticker': ticker,
                'decision': decision,
                'confidence': confidence,
                'reason': str(d.get('reason') or '')[:500],
                'risk_flags': d.get('risk_flags') if isinstance(d.get('risk_flags'), list) else [],
                'suggested_target_price': _safe_float(d.get('suggested_target_price')),
                'suggested_stop_price': _safe_float(d.get('suggested_stop_price')),
                'max_hold_minutes': _safe_int(d.get('max_hold_minutes')) if d.get('max_hold_minutes') is not None else None,
            }
        result.update({
            'used': True,
            'model': model_name,
            'decisions': decisions,
            'raw_decisions': raw_decisions,
            'note': (parsed.get('summary') if isinstance(parsed, dict) else None) or f'Claude reviewed {len(compact)} intraday plan(s).',
        })
        return result
    except Exception as e:
        result['error'] = str(e)[:300]
        result['note'] = 'Claude intraday control failed. In approval/full-control modes no new orders are allowed.'
        return result


def apply_claude_intraday_decision_to_plan(plan: Dict[str, Any], decision: Dict[str, Any], trading_settings: Dict[str, Any], intraday_settings: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Apply Claude's target/stop suggestions, then re-run hard sizing/cost gates."""
    if not decision:
        return plan, None
    entry = float(plan.get('limit_price') or 0)
    market = plan.get('market') or intraday_settings.get('market') or 'IN'
    if entry <= 0:
        return None, 'claude_control_invalid_entry'
    target = float(plan.get('target_price') or 0)
    stop = float(plan.get('stop_price') or 0)
    suggested_target = decision.get('suggested_target_price')
    suggested_stop = decision.get('suggested_stop_price')
    if suggested_stop and float(suggested_stop) > 0:
        suggested_stop = float(suggested_stop)
        max_stop_pct = float(intraday_settings.get('max_stop_loss_pct') or INTRADAY_MAX_STOP_LOSS_PCT)
        widest_allowed_stop = entry * (1 - max_stop_pct / 100)
        # Stop can be tightened but not widened beyond the configured maximum loss.
        stop = max(suggested_stop, widest_allowed_stop)
    if suggested_target and float(suggested_target) > 0:
        suggested_target = float(suggested_target)
        if suggested_target > entry:
            target = suggested_target
    if stop <= 0 or stop >= entry:
        return None, 'claude_control_invalid_stop_after_suggestion'
    if target <= entry:
        return None, 'claude_control_invalid_target_after_suggestion'
    risk_per_share = max(entry - stop, 0.01)
    rr = (target - entry) / risk_per_share if risk_per_share > 0 else 0
    min_rr = float(intraday_settings.get('min_risk_reward') or INTRADAY_MIN_RISK_REWARD)
    if rr < min_rr:
        return None, f'claude_control_rr_failed: {round(rr,2)} needs {min_rr}+'
    sizing = compute_integer_position_size(entry, stop, trading_settings, market)
    quantity = int(sizing.get('quantity') or 0)
    if quantity <= 0:
        return None, sizing.get('skip_reason') or 'claude_control_sizing_failed'
    candidate = (plan.get('order_payload') or {}).get('candidate') or {}
    size_reasons = apply_intraday_size_efficiency_gate(candidate, sizing, intraday_settings)
    if size_reasons:
        return None, 'claude_control_size_filter_failed: ' + '; '.join(size_reasons)
    cost_gate = intraday_cost_gate(entry, target, quantity, market, intraday_settings)
    if not cost_gate.get('ok'):
        return None, 'claude_control_cost_gate_failed: ' + '; '.join(cost_gate.get('reasons') or [])
    plan['quantity'] = quantity
    plan['stop_price'] = round(stop, 2)
    plan['target_price'] = round(target, 2)
    plan['estimated_value'] = round(quantity * entry, 2)
    plan.setdefault('order_payload', {})
    plan['order_payload'].update({
        'claude_control': decision,
        'target': round(target, 2),
        'stop': round(stop, 2),
        'risk_reward': round(rr, 2),
        'sizing': sizing,
        'cost_gate': cost_gate,
        'expected_round_trip_cost': cost_gate.get('expected_round_trip_cost'),
        'expected_net_profit_at_target': cost_gate.get('expected_net_profit_at_target'),
        'gross_target_profit': cost_gate.get('gross_target_profit'),
        'profit_to_cost_ratio': cost_gate.get('profit_to_cost_ratio'),
    })
    plan['rationale'] = (plan.get('rationale') or '') + f" Claude control: {decision.get('decision')} {decision.get('confidence')}% — {decision.get('reason')}"
    return plan, None


def prepare_intraday_plans_for_execution(scan: Dict[str, Any], trading_settings: Dict[str, Any], intraday_settings: Dict[str, Any], open_tickers: set, review_limit: int) -> List[Dict[str, Any]]:
    planned: List[Dict[str, Any]] = []
    for candidate in scan.get('eligible', []):
        if len(planned) >= review_limit:
            break
        if candidate.get('ticker') in open_tickers:
            candidate.setdefault('blocked_reasons', []).append('already_open_position')
            continue
        plan = build_intraday_plan(candidate, trading_settings, intraday_settings)
        if not plan:
            candidate.setdefault('blocked_reason', explain_intraday_plan_skip(candidate, trading_settings, intraday_settings))
            if candidate.get('blocked_reason') and candidate.get('blocked_reason') not in candidate.setdefault('blocked_reasons', []):
                candidate['blocked_reasons'].append(candidate.get('blocked_reason'))
            continue
        planned.append({'candidate': candidate, 'plan': plan})
    return planned


def execute_intraday_plans(planned_items: List[Dict[str, Any]], trading_settings: Dict[str, Any], intraday_settings: Dict[str, Any], trigger: str, goal_status: Dict[str, Any], remaining_trades: int, slots: int) -> Tuple[List[Dict[str, Any]], int, int]:
    orders = []
    for item in planned_items:
        if slots <= 0 or remaining_trades <= 0:
            break
        candidate = item.get('candidate') or {}
        plan = item.get('plan') or {}
        if not plan:
            continue
        plan['risk_notes'] = (plan.get('risk_notes') or '') + f" Weekly guard: net ₹{goal_status.get('net_after_api_inr', 0)} / target ₹{goal_status.get('target_profit_inr', 0)}; trigger={trigger}; mode={trading_settings.get('mode')}; Claude mode={normalize_claude_intraday_mode(intraday_settings.get('claude_control_mode'))}."
        try:
            order = persist_trade_order(plan, trading_settings, source='paper-auto-intraday' if trigger.startswith('paper-auto') else 'intraday-engine')
            orders.append(order)
            candidate['order_status'] = order.get('status') if isinstance(order, dict) else 'ORDER_RECORDED'
            candidate['order_id'] = order.get('id') if isinstance(order, dict) else None
            remaining_trades -= 1
            slots -= 1
        except Exception as order_error:
            candidate.setdefault('blocked_reasons', []).append('order_persist_failed: ' + str(order_error)[:180])
    return orders, remaining_trades, slots


def _intraday_json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _intraday_json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_intraday_json_safe(v) for v in value]
    return value


def log_intraday_run(result: Dict[str, Any], started_at: datetime.datetime, finished_at: Optional[datetime.datetime] = None) -> None:
    """Persist every manual/auto intraday engine check so the UI can show when the system tried, what it checked, and why it did or did not trade."""
    conn = get_db_connection()
    if conn is None:
        return
    finished_at = finished_at or utc_now_naive()
    session = result.get('session') or {}
    settings = result.get('settings') or {}
    trading_settings = result.get('trading_settings') or {}
    candidates = result.get('candidates') or []
    watch = result.get('watch') or []
    orders = result.get('orders') or []
    exits = result.get('exits') or {}
    source_info = result.get('data_source') or result.get('source') or get_market_data_source_info(settings.get('market') or session.get('market'), 'intraday_engine_check')
    exit_count = len(exits.get('closed') or []) if isinstance(exits, dict) else 0
    scanned_count = int(result.get('scanned_count') or result.get('scan', {}).get('scanned_count') or len(candidates) + len(watch) or 0)
    eligible_count = int(result.get('eligible_count') or len(candidates) or 0)
    watch_count = int(result.get('watch_count') or len(watch) or 0)
    order_count = int(result.get('order_count') or len(orders) or 0)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO intraday_run_log
                        (started_at, finished_at, trigger, market, mode, session_state, message,
                         scanned_count, eligible_count, watch_count, order_count, exit_count,
                         can_enter, data_provider, data_source, data_source_type, claude_cost_inr, result_json)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    started_at, finished_at, result.get('trigger'), settings.get('market') or session.get('market'),
                    trading_settings.get('mode') or settings.get('execution_mode'), session.get('state'), result.get('message'),
                    scanned_count, eligible_count, watch_count, order_count, exit_count,
                    bool(session.get('can_enter')), source_info.get('actual_provider') or source_info.get('provider'), source_info.get('source_label') or source_info.get('source_name'), source_info.get('source_type'), 0,
                    psycopg2.extras.Json(_intraday_json_safe({
                        'message': result.get('message'),
                        'data_source': source_info,
                        'session': session,
                        'trigger': result.get('trigger'),
                        'enabled': result.get('enabled'),
                        'scanned_count': scanned_count,
                        'eligible_count': eligible_count,
                        'watch_count': watch_count,
                        'order_count': order_count,
                        'selection_metadata': result.get('selection_metadata'),
                        'exit_count': exit_count,
                        'candidates': candidates[:25],
                        'watch': watch[:25],
                        'orders': orders[:25],
                        'errors': (result.get('errors') or [])[:25],
                    }))
                ))
    except Exception as e:
        print(f"[WARN] Could not log intraday run: {e}")
    finally:
        conn.close()


def get_recent_intraday_runs(limit: int = 20) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    if conn is None:
        return []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, started_at, finished_at, trigger, market, mode, session_state, message,
                       scanned_count, eligible_count, watch_count, order_count, exit_count, can_enter,
                       data_provider, data_source, data_source_type, claude_cost_inr, result_json
                FROM intraday_run_log
                ORDER BY finished_at DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
    except Exception as e:
        print(f"[WARN] Could not load intraday run log: {e}")
        rows = []
    finally:
        conn.close()
    def clean(row):
        out = {}
        for k, v in dict(row).items():
            if hasattr(v, 'as_tuple'):
                out[k] = float(v)
            elif isinstance(v, (datetime.datetime, datetime.date)):
                out[k] = v.isoformat()
            else:
                out[k] = v
        return out
    return [clean(r) for r in rows]



def _clean_db_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in dict(row).items():
        if hasattr(v, 'as_tuple'):
            out[k] = float(v)
        elif isinstance(v, (datetime.datetime, datetime.date)):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


def run_trading_self_scan(market: str, horizon: str, settings: Dict[str, Any]) -> Dict[str, Any]:
    """Build a fresh deterministic candidate list for Trading Automation.

    This is the independent mode requested for the main Trading Automation
    engine. It uses the same public, explainable professional-analysis funnel as
    the scanner, but keeps it inside automation and does not require an existing
    locked First Scan. It does not call Claude by default; ranking is
    deterministic, then strict-learning and risk controls still decide whether
    any order is allowed.
    """
    scan_started = utc_now_naive()
    market, horizon = validate_market_horizon(market, horizon)
    configured_universe = load_market_universe(market)
    limit = min(len(configured_universe), max(20, int(settings.get('self_scan_universe_limit') or TRADING_SELF_SCAN_UNIVERSE_LIMIT)))
    universe = configured_universe[:limit]
    source_info = get_market_data_source_info(market, 'trading_self_scan')
    snapshots = download_universe_snapshots(universe)
    liquid = liquidity_filter(snapshots, market)
    momentum = apply_momentum_stage(liquid)
    technical = apply_technical_stage(momentum)
    deep = enrich_deep_candidates(technical)
    learning_context = get_learning_context(market, horizon)
    deep = apply_learning_adjustments(deep, learning_context)
    market_context = get_market_context(market)
    deep = apply_strategy_memory_adjustments(deep, market, horizon, market_context)

    min_score = float(settings.get('self_scan_min_score') or TRADING_SELF_SCAN_MIN_SCORE)
    expert_ready = [c for c in deep if float(c.get('final_pre_ai_score') or c.get('preliminary_score') or 0) >= min_score]
    if not expert_ready:
        expert_ready = deep[:FINAL_PICK_LIMIT]
    picks = deterministic_rank_candidates(expert_ready, market, horizon)
    strict_result = apply_strict_learning_to_picks(picks, expert_ready, market, horizon, market_context)
    picks = strict_result.get('picks', picks)

    completed_at = utc_now_naive()
    counts = {
        'configured_universe': len(configured_universe),
        'self_scan_universe': len(universe),
        'price_snapshots': len(snapshots),
        'liquidity_pass': len(liquid),
        'momentum_pass': len(momentum),
        'technical_pass': len(technical),
        'deep_analysis': len(deep),
        'expert_ready': len(expert_ready),
        'final_picks': len(picks),
    }
    methodology = get_methodology(counts)
    methodology['engine'] = 'trading_automation_self_scan'
    methodology['selection_mode'] = 'self_scan'
    methodology['stages'].append({
        'stage': 'Automation self-scan mode',
        'method': 'Runs inside Trading Automation when enabled. It ranks stocks from the configured NSE/US universe using liquidity, momentum, technicals, fundamentals, news sentiment, market regime, learning memory, risk/reward and opening confirmation. It does not need a locked scanner prediction.'
    })
    return {
        'picks': picks,
        'market': market,
        'horizon': horizon,
        'timestamp': completed_at.isoformat(),
        'started_at': scan_started.isoformat(),
        'completed_at': completed_at.isoformat(),
        'cached': False,
        'locked': False,
        'self_scan': True,
        'selection_mode': 'self_scan',
        'total_scanned': len(universe),
        'funnel_counts': counts,
        'market_context': market_context,
        'methodology': methodology,
        'model': 'deterministic-professional-self-scan',
        'data_source': source_info,
        'strict_learning_mode': strict_result.get('summary'),
        'no_trade_recommendation': bool((strict_result.get('summary') or {}).get('no_trade_recommendation')),
        'message': 'Trading Automation self-scan built fresh expert-style candidates without requiring First Scan prediction data.'
    }


def resolve_trading_candidate_source(market: str, horizon: str, settings: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    selection_mode = settings.get('selection_mode') or 'locked_scan'
    meta: Dict[str, Any] = {'selection_mode': selection_mode, 'locked_scan_available': False, 'used_source': None}
    cached = None
    if selection_mode in ('locked_scan', 'auto'):
        cached = get_locked_cached_scan(market, horizon) or get_cached_scan(market, horizon)
        if cached:
            meta.update({'locked_scan_available': True, 'used_source': 'locked_scan', 'order_source': 'locked-scan-confirmed'})
            cached['selection_mode'] = 'locked_scan'
            return cached, meta
    if selection_mode in ('self_scan', 'auto'):
        try:
            cached = run_trading_self_scan(market, horizon, settings)
            meta.update({'locked_scan_available': False, 'used_source': 'self_scan', 'order_source': 'self-scan-confirmed'})
            return cached, meta
        except Exception as e:
            meta.update({'self_scan_error': str(e)[:240]})
            if selection_mode == 'auto':
                return None, meta
            raise
    return None, meta


def log_trading_automation_run(result: Dict[str, Any], started_at: datetime.datetime, market: str, horizon: str, finished_at: Optional[datetime.datetime] = None) -> None:
    """Persist every Trading Automation check so the UI survives refreshes, restarts, and multi-worker requests."""
    conn = get_db_connection()
    if conn is None:
        return
    finished_at = finished_at or utc_now_naive()
    settings = result.get('settings') or {}
    goal_status = result.get('goal_status') or {}
    opening_status = result.get('opening_status') or get_market_session_state(market, settings.get('opening_wait_minutes'))
    entry_checks = result.get('entry_checks') or []
    orders = result.get('orders') or []
    exits = result.get('exits') or {}
    data_source = result.get('data_source') or get_market_data_source_info(market, 'trading_automation_check')
    approved_count = len([c for c in entry_checks if c.get('allowed')])
    blocked_count = len([c for c in entry_checks if not c.get('allowed')])
    exit_count = len(exits.get('closed') or []) if isinstance(exits, dict) else 0
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO trading_automation_run_log
                        (started_at, finished_at, trigger, market, horizon, mode, message,
                         locked_scan_available, entry_check_count, approved_count, blocked_count,
                         order_count, exit_count, can_open_new_trades, session_state,
                         data_provider, data_source, data_source_type, result_json)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    started_at, finished_at, result.get('trigger'), market, horizon, settings.get('mode'), result.get('message') or result.get('error'),
                    bool(result.get('locked_scan_available')), len(entry_checks), approved_count, blocked_count,
                    len(orders), exit_count, bool(goal_status.get('can_open_new_trades')), opening_status.get('state'),
                    data_source.get('actual_provider') or data_source.get('provider'), data_source.get('source_label') or data_source.get('source_name'), data_source.get('source_type'),
                    psycopg2.extras.Json(_intraday_json_safe({
                        'message': result.get('message'),
                        'error': result.get('error'),
                        'trigger': result.get('trigger'),
                        'market': market,
                        'horizon': horizon,
                        'mode': settings.get('mode'),
                        'data_source': data_source,
                        'opening_status': opening_status,
                        'goal_status': goal_status,
                        'locked_scan_available': bool(result.get('locked_scan_available')),
                        'entry_checks': entry_checks[:25],
                        'orders': orders[:25],
                        'exits': exits,
                    }))
                ))
    except Exception as e:
        print(f"[WARN] Could not log trading automation run: {e}")
    finally:
        conn.close()


def get_recent_trading_automation_runs(limit: int = 20) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    if conn is None:
        return []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, started_at, finished_at, trigger, market, horizon, mode, message,
                       locked_scan_available, entry_check_count, approved_count, blocked_count,
                       order_count, exit_count, can_open_new_trades, session_state,
                       data_provider, data_source, data_source_type, result_json
                FROM trading_automation_run_log
                ORDER BY finished_at DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
    except Exception as e:
        print(f"[WARN] Could not load trading automation run log: {e}")
        rows = []
    finally:
        conn.close()
    return [_clean_db_row(r) for r in rows]


def finalize_trading_automation_result(result: Dict[str, Any], started_at: datetime.datetime, market: str, horizon: str) -> Dict[str, Any]:
    finished_at = utc_now_naive()
    settings = result.get('settings') or {}
    if not result.get('trigger'):
        result['trigger'] = 'manual'
    if not result.get('opening_status'):
        result['opening_status'] = get_market_session_state(market, settings.get('opening_wait_minutes'))
    if not result.get('data_source'):
        result['data_source'] = get_market_data_source_info(market, 'trading_automation_check')
    result['data_provider'] = (result.get('data_source') or {}).get('actual_provider') or (result.get('data_source') or {}).get('provider')
    result['data_source_label'] = (result.get('data_source') or {}).get('source_label') or (result.get('data_source') or {}).get('source_name')
    result['checked_at'] = finished_at.isoformat()
    result['entry_check_count'] = len(result.get('entry_checks') or [])
    result['order_count'] = len(result.get('orders') or [])
    exits = result.get('exits') or {}
    result['exit_count'] = len(exits.get('closed') or []) if isinstance(exits, dict) else 0
    log_trading_automation_run(result, started_at, market, horizon, finished_at)
    return result


def log_paper_auto_run(result: Dict[str, Any], started_at: datetime.datetime, finished_at: Optional[datetime.datetime] = None) -> None:
    """Persist every full paper scheduler cycle, including cycles where markets were closed and engines were skipped."""
    conn = get_db_connection()
    if conn is None:
        return
    finished_at = finished_at or utc_now_naive()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO paper_auto_run_log
                        (started_at, finished_at, trigger, message, daily_week_count, intraday_count,
                         order_count, exit_count, skipped_count, result_json)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    started_at, finished_at, result.get('trigger'), result.get('message'),
                    len(result.get('daily_week') or []), len(result.get('intraday') or []),
                    int(result.get('orders_created') or 0), int(result.get('exits_closed') or 0), len(result.get('skipped') or []),
                    psycopg2.extras.Json(_intraday_json_safe({
                        'message': result.get('message'),
                        'trigger': result.get('trigger'),
                        'started_at': result.get('started_at'),
                        'finished_at': result.get('finished_at'),
                        'orders_created': result.get('orders_created', 0),
                        'exits_closed': result.get('exits_closed', 0),
                        'skipped': (result.get('skipped') or [])[:25],
                        'daily_week_count': len(result.get('daily_week') or []),
                        'intraday_count': len(result.get('intraday') or []),
                    }))
                ))
    except Exception as e:
        print(f"[WARN] Could not log paper auto run: {e}")
    finally:
        conn.close()


def get_recent_paper_auto_runs(limit: int = 20) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    if conn is None:
        return []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, started_at, finished_at, trigger, message, daily_week_count, intraday_count,
                       order_count, exit_count, skipped_count, result_json
                FROM paper_auto_run_log
                ORDER BY finished_at DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
    except Exception as e:
        print(f"[WARN] Could not load paper auto run log: {e}")
        rows = []
    finally:
        conn.close()
    return [_clean_db_row(r) for r in rows]


def finalize_intraday_result(result: Dict[str, Any], started_at: datetime.datetime) -> Dict[str, Any]:
    finished_at = utc_now_naive()
    candidates = result.get('candidates') or []
    watch = result.get('watch') or []
    orders = result.get('orders') or []
    if not result.get('data_source'):
        market = ((result.get('settings') or {}).get('market') or (result.get('session') or {}).get('market') or '')
        result['data_source'] = get_market_data_source_info(market, 'intraday_engine_check')
    result['data_provider'] = (result.get('data_source') or {}).get('actual_provider') or (result.get('data_source') or {}).get('provider')
    result['data_source_label'] = (result.get('data_source') or {}).get('source_label') or (result.get('data_source') or {}).get('source_name')
    result['checked_at'] = finished_at.isoformat()
    result['scanned_count'] = int(result.get('scanned_count') or len(candidates) + len(watch) or 0)
    result['eligible_count'] = int(result.get('eligible_count') or len(candidates) or 0)
    result['watch_count'] = int(result.get('watch_count') or len(watch) or 0)
    result['order_count'] = int(result.get('order_count') or len(orders) or 0)
    log_intraday_run(result, started_at, finished_at)
    return result

def run_intraday_engine(trigger: str = 'manual', market_override: Optional[str] = None, settings_override: Optional[Dict[str, Any]] = None, trading_settings_override: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not INTRADAY_RUN_LOCK.acquire(blocking=False):
        return {'ok': True, 'locked': True, 'orders': [], 'message': 'Intraday engine is already running; duplicate trigger skipped.'}
    started_at = utc_now_naive()
    try:
        intraday_settings = get_intraday_settings()
        if market_override in DEFAULT_UNIVERSES:
            intraday_settings['market'] = market_override
        if settings_override:
            intraday_settings.update(settings_override)
        base_trading_settings = dict(trading_settings_override or get_trading_settings())
        trading_settings = get_intraday_effective_trading_settings(base_trading_settings, intraday_settings)
        market = intraday_settings.get('market') or 'IN'
        data_source = get_market_data_source_info(market, 'intraday_engine_check')
        goal_status = get_weekly_goal_status(trading_settings)
        exits = evaluate_open_positions_for_exits(trading_settings, market=market, goal_status=goal_status)
        session = get_intraday_session_state(intraday_settings)

        if not intraday_settings.get('enabled'):
            return finalize_intraday_result({'ok': True, 'enabled': False, 'trigger': trigger, 'settings': intraday_settings, 'trading_settings': trading_settings, 'session': session, 'goal_status': goal_status, 'exits': exits, 'candidates': [], 'watch': [], 'orders': [], 'auto_state': get_intraday_auto_state(), 'message': 'Intraday engine is disabled.'}, started_at)
        if not TRADING_ENABLED:
            return finalize_intraday_result({'ok': True, 'enabled': True, 'trigger': trigger, 'settings': intraday_settings, 'trading_settings': trading_settings, 'session': session, 'goal_status': goal_status, 'exits': exits, 'candidates': [], 'watch': [], 'orders': [], 'auto_state': get_intraday_auto_state(), 'message': 'TRADING_ENABLED=false; intraday engine did not open entries.'}, started_at)
        if not session.get('can_enter'):
            return finalize_intraday_result({'ok': True, 'enabled': True, 'trigger': trigger, 'settings': intraday_settings, 'trading_settings': trading_settings, 'session': session, 'goal_status': goal_status, 'exits': exits, 'candidates': [], 'watch': [], 'orders': [], 'auto_state': get_intraday_auto_state(), 'message': get_intraday_entry_block_message(session)}, started_at)
        if not goal_status.get('can_open_new_trades'):
            return finalize_intraday_result({'ok': True, 'enabled': True, 'trigger': trigger, 'settings': intraday_settings, 'trading_settings': trading_settings, 'session': session, 'goal_status': goal_status, 'exits': exits, 'candidates': [], 'watch': [], 'orders': [], 'auto_state': get_intraday_auto_state(), 'message': 'Weekly target/loss guard is active: ' + str(goal_status.get('halt_reason'))}, started_at)

        today_count = get_intraday_trade_count_today(intraday_settings, trading_settings.get('mode'))
        remaining_trades = max(int(intraday_settings.get('max_trades_per_day') or 0) - today_count, 0)
        if remaining_trades <= 0:
            scan = scan_intraday_candidates(intraday_settings)
            return finalize_intraday_result({'ok': True, 'enabled': True, 'trigger': trigger, 'settings': intraday_settings, 'trading_settings': trading_settings, 'session': session, 'goal_status': goal_status, 'exits': exits, 'candidates': scan.get('eligible', []), 'watch': scan.get('watch', []), 'orders': [], 'errors': scan.get('errors', []), 'selection_metadata': scan.get('selection_metadata'), 'shortlist_count': scan.get('shortlist_count'), 'data_source': scan.get('data_source') or data_source, 'data_provider': scan.get('data_provider'), 'data_source_label': scan.get('data_source_label'), 'scanned_count': scan.get('scanned_count', 0), 'eligible_count': scan.get('eligible_count', 0), 'watch_count': len(scan.get('watch', [])), 'auto_state': get_intraday_auto_state(), 'message': 'Daily intraday trade limit reached; scan shown for monitoring only.'}, started_at)

        scan = scan_intraday_candidates(intraday_settings)
        open_positions = get_open_positions(market=market, mode=trading_settings.get('mode'))
        open_tickers = {p['ticker'] for p in open_positions}
        orders = []
        slots = max(int(trading_settings.get('max_open_positions') or 0) - len(open_tickers), 0)
        claude_mode = normalize_claude_intraday_mode(intraday_settings.get('claude_control_mode'))
        # Paper full control is intentionally not allowed to become direct live broker control.
        # In live/assisted mode it becomes approval-required review; hard engine gates still execute last.
        if claude_mode == 'paper_full_control' and trading_settings.get('mode') != 'paper':
            claude_mode = 'approval_required'
        intraday_settings['claude_control_mode_effective'] = claude_mode
        review_limit = max(slots, int(intraday_settings.get('claude_max_reviews_per_run') or CLAUDE_INTRADAY_MAX_REVIEWS_PER_RUN), 1)
        planned_items = prepare_intraday_plans_for_execution(scan, trading_settings, intraday_settings, open_tickers, review_limit)
        claude_result = {'enabled': False, 'mode': claude_mode, 'used': False, 'decisions': {}, 'note': 'Claude intraday control is off.'}

        if claude_mode == 'off':
            orders, remaining_trades, slots = execute_intraday_plans(planned_items, trading_settings, intraday_settings, trigger, goal_status, remaining_trades, slots)
        else:
            claude_result = call_claude_intraday_controller(planned_items, intraday_settings, trading_settings, session, goal_status, trigger)
            decisions = claude_result.get('decisions') or {}
            min_conf = int(intraday_settings.get('claude_min_confidence') or CLAUDE_INTRADAY_MIN_CONFIDENCE)
            approved_items: List[Dict[str, Any]] = []
            # Claude chooses priority. Unknown/unreviewed plans are blocked in approval/full-control modes.
            def _decision_sort_key(item: Dict[str, Any]) -> Tuple[int, int]:
                ticker = str((item.get('plan') or {}).get('ticker') or '').upper()
                d = decisions.get(ticker) or {}
                return (int(d.get('rank') or 999), -int(d.get('confidence') or 0))
            for item in sorted(planned_items, key=_decision_sort_key):
                candidate = item.get('candidate') or {}
                plan = item.get('plan') or {}
                ticker = str(plan.get('ticker') or candidate.get('ticker') or '').upper()
                decision = decisions.get(ticker)
                candidate['claude_control_mode'] = claude_mode
                if decision:
                    candidate['claude_decision'] = decision
                    candidate.setdefault('blocked_reasons', []).append(f"claude_{decision.get('decision','WATCH').lower()}: {decision.get('confidence')}% - {decision.get('reason','')}")
                elif claude_result.get('error'):
                    candidate.setdefault('blocked_reasons', []).append('claude_control_unavailable: ' + str(claude_result.get('error'))[:160])
                elif claude_mode in ('approval_required', 'paper_full_control'):
                    candidate.setdefault('blocked_reasons', []).append('claude_no_decision: blocked because Claude control requires explicit approval')

                if claude_mode == 'review_only':
                    # Review-only adds Claude analysis but does not block deterministic execution.
                    if decision:
                        adjusted_plan, adjust_reason = apply_claude_intraday_decision_to_plan(plan, decision, trading_settings, intraday_settings)
                        if adjusted_plan:
                            item['plan'] = adjusted_plan
                        elif adjust_reason:
                            candidate.setdefault('blocked_reasons', []).append(adjust_reason)
                    approved_items.append(item)
                    continue

                if not decision or decision.get('decision') != 'ALLOW' or int(decision.get('confidence') or 0) < min_conf:
                    if decision:
                        candidate['blocked_reason'] = f"claude_control_blocked: {decision.get('decision')} {decision.get('confidence')}%; needs ALLOW {min_conf}%+"
                    else:
                        candidate['blocked_reason'] = 'claude_control_blocked: no valid Claude approval'
                    continue
                adjusted_plan, adjust_reason = apply_claude_intraday_decision_to_plan(plan, decision, trading_settings, intraday_settings)
                if not adjusted_plan:
                    candidate['blocked_reason'] = adjust_reason or 'claude_control_adjustment_failed'
                    candidate.setdefault('blocked_reasons', []).append(candidate['blocked_reason'])
                    continue
                item['plan'] = adjusted_plan
                approved_items.append(item)
            orders, remaining_trades, slots = execute_intraday_plans(approved_items, trading_settings, intraday_settings, trigger, goal_status, remaining_trades, slots)
            if claude_result.get('error'):
                scan.setdefault('errors', []).append(claude_result.get('error'))
        return finalize_intraday_result({
            'ok': True,
            'enabled': True,
            'trigger': trigger,
            'settings': intraday_settings,
            'trading_settings': trading_settings,
            'session': session,
            'goal_status': goal_status,
            'exits': exits,
            'candidates': scan.get('eligible', []),
            'watch': scan.get('watch', []),
            'errors': scan.get('errors', []),
            'orders': orders,
            'selection_metadata': scan.get('selection_metadata'),
            'shortlist_count': scan.get('shortlist_count'),
            'data_source': scan.get('data_source') or data_source,
            'data_provider': scan.get('data_provider'),
            'data_source_label': scan.get('data_source_label'),
            'scanned_count': scan.get('scanned_count', 0),
            'eligible_count': scan.get('eligible_count', 0),
            'watch_count': len(scan.get('watch', [])),
            'auto_state': get_intraday_auto_state(),
            'claude_control': claude_result,
            'message': f"Intraday engine checked {scan.get('scanned_count', 0)} stocks using {((scan.get('data_source') or {}).get('source_label') or 'market data')}, found {scan.get('eligible_count', 0)} eligible bullish setups, created {len(orders)} order(s). Mode={trading_settings.get('mode')}. Claude control={claude_result.get('mode')}; used={bool(claude_result.get('used'))}.",
        }, started_at)
    finally:
        INTRADAY_RUN_LOCK.release()



def _safe_ratio(num: float, den: float, default: Optional[float] = None) -> Optional[float]:
    try:
        den = float(den or 0)
        if den == 0:
            return default
        return round(float(num or 0) / den, 3)
    except Exception:
        return default


def _summarize_closed_trade_rows(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    closed = []
    for r in rows:
        if str(r.get('status') or '').upper() != 'CLOSED':
            continue
        qty = _safe_int(r.get('quantity'), 0)
        entry = _safe_float(r.get('entry_price'), 0) or 0
        exit_px = _safe_float(r.get('exit_price'))
        market = r.get('market') or 'IN'
        gross = _safe_float(r.get('gross_pnl_amount'))
        cost = _safe_float(r.get('cost_amount'))
        net = _safe_float(r.get('net_pnl_amount'))
        if exit_px is not None and (gross is None or cost is None or net is None):
            calc = calculate_net_trade_pnl(entry, exit_px, qty, market)
            gross = calc.get('gross_pnl') if gross is None else gross
            cost = calc.get('cost_amount') if cost is None else cost
            net = calc.get('net_pnl') if net is None else net
        if net is None:
            net = _safe_float(r.get('pnl_amount'), 0) or 0
        if gross is None:
            gross = net
        if cost is None:
            cost = max(float(gross or 0) - float(net or 0), 0)
        position_value = entry * qty
        closed.append({
            'ticker': r.get('ticker'),
            'market': market,
            'net': float(net or 0),
            'gross': float(gross or 0),
            'cost': float(cost or 0),
            'position_value': float(position_value or 0),
            'exit_reason': r.get('exit_reason') or 'UNKNOWN',
            'expected_round_trip_cost': _safe_float(r.get('expected_round_trip_cost')),
            'expected_net_profit_at_target': _safe_float(r.get('expected_net_profit_at_target')),
        })
    total = len(closed)
    wins = [x for x in closed if x['net'] > 0]
    losses = [x for x in closed if x['net'] < 0]
    gross_total = round(sum(x['gross'] for x in closed), 2)
    cost_total = round(sum(x['cost'] for x in closed), 2)
    net_total = round(sum(x['net'] for x in closed), 2)
    win_sum = sum(x['net'] for x in wins)
    loss_sum = sum(x['net'] for x in losses)
    exit_groups: Dict[str, Dict[str, Any]] = {}
    for x in closed:
        g = exit_groups.setdefault(x['exit_reason'], {'count': 0, 'net_pnl': 0.0, 'gross_pnl': 0.0, 'costs': 0.0})
        g['count'] += 1
        g['net_pnl'] += x['net']
        g['gross_pnl'] += x['gross']
        g['costs'] += x['cost']
    by_exit_reason = []
    for reason, g in exit_groups.items():
        by_exit_reason.append({
            'reason': reason,
            'count': g['count'],
            'net_pnl': round(g['net_pnl'], 2),
            'gross_pnl': round(g['gross_pnl'], 2),
            'costs': round(g['costs'], 2),
            'avg_net': round(g['net_pnl'] / g['count'], 2) if g['count'] else 0,
        })
    by_exit_reason.sort(key=lambda x: x['net_pnl'])
    avg_expected_net = [x['expected_net_profit_at_target'] for x in closed if x.get('expected_net_profit_at_target') is not None]
    avg_expected_cost = [x['expected_round_trip_cost'] for x in closed if x.get('expected_round_trip_cost') is not None]
    return {
        'closed_trades': total,
        'winning_trades': len(wins),
        'losing_trades': len(losses),
        'win_rate_pct': round((len(wins) / total) * 100, 1) if total else None,
        'gross_pnl': gross_total,
        'total_costs': cost_total,
        'net_pnl': net_total,
        'avg_net_per_trade': round(net_total / total, 2) if total else None,
        'avg_gross_per_trade': round(gross_total / total, 2) if total else None,
        'avg_cost_per_trade': round(cost_total / total, 2) if total else None,
        'avg_net_win': round(win_sum / len(wins), 2) if wins else None,
        'avg_net_loss': round(loss_sum / len(losses), 2) if losses else None,
        'profit_factor': _safe_ratio(win_sum, abs(loss_sum), None),
        'cost_drag_pct_of_gross': round((cost_total / abs(gross_total)) * 100, 1) if gross_total else None,
        'avg_expected_net_at_target': round(sum(avg_expected_net) / len(avg_expected_net), 2) if avg_expected_net else None,
        'avg_expected_round_trip_cost': round(sum(avg_expected_cost) / len(avg_expected_cost), 2) if avg_expected_cost else None,
        'by_exit_reason': by_exit_reason[:8],
        'warning': None if (total == 0 or (net_total > 0 and (win_sum > abs(loss_sum)))) else 'Net expectancy is not positive yet. Reduce overtrading, avoid tiny exits, and increase expected net-profit gate.',
    }


def get_intraday_expectancy_dashboard(market: Optional[str] = None, limit: int = 500) -> Dict[str, Any]:
    conn = get_db_connection()
    settings = get_intraday_settings()
    base_policy = {
        'cost_model_enabled': bool(TRADE_COST_MODEL_ENABLED),
        'india_min_expected_net_profit': float(settings.get('min_expected_net_profit_inr') or INTRADAY_MIN_EXPECTED_NET_PROFIT_INR),
        'us_min_expected_net_profit': float(settings.get('min_expected_net_profit_usd') or INTRADAY_MIN_EXPECTED_NET_PROFIT_USD),
        'min_gross_profit_to_cost_ratio': float(settings.get('min_gross_profit_to_cost_ratio') or INTRADAY_MIN_GROSS_PROFIT_TO_COST_RATIO),
        'rule': 'A new intraday trade must have target gross profit greater than estimated round-trip costs, meet the min net-profit buffer, and meet the profit-to-cost ratio before an order is created.',
        'included_costs': 'Estimated brokerage/commission, STT where applicable, exchange/SEBI/stamp/GST where configured, spread/slippage reserve, and minimum round-trip cost.',
    }
    if conn is None:
        return {'overall': _summarize_closed_trade_rows([]), 'today': _summarize_closed_trade_rows([]), 'policy': base_policy, 'note': 'DATABASE_URL not set, expectancy dashboard cannot read closed trades.'}
    try:
        filters = ["horizon='intraday'"]
        params: List[Any] = []
        if market and market != 'all':
            filters.append('market=%s')
            params.append(market)
        where_sql = 'WHERE ' + ' AND '.join(filters)
        limit = max(50, min(int(limit or 500), 2000))
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"""
                SELECT id, market, ticker, quantity, entry_price, exit_price, status, pnl_amount, pnl_pct,
                       gross_pnl_amount, cost_amount, net_pnl_amount, expected_round_trip_cost,
                       expected_net_profit_at_target, exit_reason, opened_at, closed_at
                FROM trade_positions
                {where_sql}
                ORDER BY COALESCE(closed_at, opened_at) DESC
                LIMIT %s
            """, params + [limit])
            rows = [dict(r) for r in cur.fetchall()]
            tz = ZoneInfo('Asia/Kolkata')
            now_local = datetime.datetime.now(tz)
            day_start = datetime.datetime.combine(now_local.date(), datetime.time(0, 0), tzinfo=tz).astimezone(datetime.timezone.utc).replace(tzinfo=None)
            day_rows = [r for r in rows if r.get('opened_at') and r.get('opened_at') >= day_start]
        return {
            'overall': _summarize_closed_trade_rows(rows),
            'today': _summarize_closed_trade_rows(day_rows),
            'policy': base_policy,
            'note': 'All expectancy values use net P&L after estimated trade costs. Gross wins that do not cover costs are counted as losses.',
        }
    except Exception as e:
        return {'overall': _summarize_closed_trade_rows([]), 'today': _summarize_closed_trade_rows([]), 'policy': base_policy, 'error': str(e)[:300]}
    finally:
        conn.close()

def get_intraday_dashboard() -> Dict[str, Any]:
    intraday_settings = get_intraday_settings()
    base_trading_settings = get_trading_settings()
    trading_settings = get_intraday_effective_trading_settings(base_trading_settings, intraday_settings)
    market = intraday_settings.get('market') or 'IN'
    goal_status = get_weekly_goal_status(trading_settings)
    session = get_intraday_session_state(intraday_settings)
    exits = evaluate_open_positions_for_exits(trading_settings, market=market, goal_status=goal_status)
    conn = get_db_connection()
    orders, positions = [], []
    summary = {'open_positions': 0, 'realised_pnl': 0, 'closed_positions': 0}
    if conn is not None:
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM trade_orders WHERE source IN ('intraday-engine','paper-auto-intraday') ORDER BY created_at DESC LIMIT 30")
                orders = cur.fetchall()
                cur.execute("SELECT * FROM trade_positions WHERE horizon='intraday' ORDER BY opened_at DESC LIMIT 30")
                positions = cur.fetchall()
                cur.execute("""
                    SELECT COUNT(*) FILTER (WHERE status='OPEN') AS open_positions,
                           COALESCE(SUM(pnl_amount) FILTER (WHERE status='CLOSED'),0) AS realised_pnl,
                           COUNT(*) FILTER (WHERE status='CLOSED') AS closed_positions
                    FROM trade_positions WHERE horizon='intraday'
                """)
                summary = cur.fetchone() or summary
        finally:
            conn.close()
    def clean(row):
        out = {}
        for k, v in dict(row).items():
            if hasattr(v, 'as_tuple'):
                out[k] = float(v)
            elif isinstance(v, (datetime.datetime, datetime.date)):
                out[k] = v.isoformat()
            else:
                out[k] = v
        return out
    return {
        'settings': intraday_settings,
        'trading_settings': trading_settings,
        'session': session,
        'data_source': get_market_data_source_info(market, 'intraday_dashboard'),
        'goal_status': goal_status,
        'exits': exits,
        'orders': [clean(r) for r in orders],
        'positions': [clean(r) for r in positions],
        'summary': clean(summary),
        'expectancy': get_intraday_expectancy_dashboard(market=market),
        'auto_state': get_intraday_auto_state(),
        'recent_auto_runs': get_recent_intraday_runs(20),
        'policy': 'Separate intraday page. It scans bullish 5-minute setups and executes deterministic paper/assisted/live orders only if risk, weekly goal, max-loss and cost guards pass. Claude is never called for intraday entry/exit. Auto mode records each engine check and stops after the session close check.',
    }

def get_simulation_capital_for_market(settings: Dict[str, Any], market: str) -> float:
    """Return paper/assisted sizing capital in the market's native currency.

    India uses INR capital. US/global paper sizing uses USD capital. This avoids
    treating ₹5,000 as if it were $5,000 when the selected stock is American.
    """
    if str(market).upper() == 'IN':
        return float(settings.get('capital_inr') or TRADING_DEFAULT_CAPITAL_INR)
    return float(settings.get('capital_usd') or TRADING_DEFAULT_CAPITAL_USD)


def compute_integer_position_size(entry: float, stop: float, settings: Dict[str, Any], market: str) -> Dict[str, Any]:
    capital = get_simulation_capital_for_market(settings, market)
    max_position_value = capital * float(settings.get('max_position_pct') or TRADING_MAX_POSITION_PCT) / 100
    risk_amount = capital * float(settings.get('risk_per_trade_pct') or TRADING_RISK_PER_TRADE_PCT) / 100
    per_share_risk = max(entry - stop, 0.01)
    qty_by_risk = math.floor(risk_amount / per_share_risk)
    qty_by_value = math.floor(max_position_value / entry) if entry > 0 else 0
    quantity = max(0, min(qty_by_risk, qty_by_value))
    reason = ''
    if quantity <= 0:
        currency = get_currency_info(market).get('symbol', '$')
        if entry > capital:
            reason = f"Selected stock price {currency}{entry:.2f} is higher than total simulation capital {currency}{capital:.2f}; paper trade skipped."
        elif entry > max_position_value:
            reason = f"Selected stock price {currency}{entry:.2f} is higher than allowed position size {currency}{max_position_value:.2f}; increase capital or max position %, otherwise skip."
        elif qty_by_risk <= 0:
            reason = f"Risk per share is too large for risk budget; trade skipped to protect capital."
        else:
            reason = 'Position size calculated as zero; trade skipped to avoid fake fractional/over-capital paper fill.'
    return {
        'quantity': quantity,
        'capital': round(capital, 2),
        'max_position_value': round(max_position_value, 2),
        'risk_amount': round(risk_amount, 2),
        'risk_per_share': round(per_share_risk, 4),
        'qty_by_risk': int(qty_by_risk),
        'qty_by_value': int(qty_by_value),
        'skip_reason': reason,
    }


def explain_trade_plan_skip(pick: Dict[str, Any], settings: Dict[str, Any]) -> str:
    signal = 'WATCH' if pick.get('signal') == 'HOLD' else pick.get('signal')
    if signal != 'BUY':
        return f"Signal is {signal or 'unknown'}, not BUY."
    confidence = int(pick.get('confidence') or 0)
    rr = parse_rr(pick.get('risk_reward'))
    if confidence < settings['min_confidence']:
        return f"Confidence {confidence}% is below automation minimum {settings['min_confidence']}%."
    if rr < settings['min_risk_reward']:
        return f"Reward/risk {rr}:1 is below automation minimum {settings['min_risk_reward']}:1."
    ticker = str(pick.get('ticker') or '')
    market_name = 'IN' if ticker.upper().endswith(('.NS', '.BO')) else pick.get('market') or 'US'
    entry = float(pick.get('entry_price') or pick.get('current_price') or 0)
    stop = float(pick.get('stop_price') or 0)
    target = float(pick.get('target_price') or 0)
    if entry <= 0 or stop <= 0 or target <= 0 or stop >= entry or target <= entry:
        return 'Entry/target/stop prices are not valid enough to simulate safely.'
    sizing = compute_integer_position_size(entry, stop, settings, market_name)
    if sizing['quantity'] <= 0:
        return sizing['skip_reason']
    return 'Trade plan could not be created safely.'

def build_trade_plan_from_pick(pick: Dict[str, Any], settings: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    signal = 'WATCH' if pick.get('signal') == 'HOLD' else pick.get('signal')
    if signal != 'BUY':
        return None
    confidence = int(pick.get('confidence') or 0)
    rr = parse_rr(pick.get('risk_reward'))
    if confidence < settings['min_confidence'] or rr < settings['min_risk_reward']:
        return None
    ticker = str(pick.get('ticker') or '')
    market_name = 'IN' if ticker.upper().endswith(('.NS', '.BO')) else pick.get('market') or 'US'
    entry = float(pick.get('entry_price') or pick.get('current_price') or 0)
    stop = float(pick.get('stop_price') or 0)
    target = float(pick.get('target_price') or 0)
    if entry <= 0 or stop <= 0 or target <= 0 or stop >= entry or target <= entry:
        return None
    sizing = compute_integer_position_size(entry, stop, settings, market_name)
    quantity = int(sizing.get('quantity') or 0)
    if quantity <= 0:
        return None
    currency = get_currency_info(market_name, ticker)
    estimated_value = round(quantity * entry, 2)
    engine = 'trading_automation'
    return {
        'market': market_name,
        'horizon': pick.get('horizon') or 'day',
        'ticker': ticker,
        'company': pick.get('company'),
        'side': 'BUY',
        'quantity': quantity,
        'order_type': 'LIMIT',
        'limit_price': round(entry, 2),
        'stop_price': round(stop, 2),
        'target_price': round(target, 2),
        'estimated_value': estimated_value,
        'currency_symbol': currency['symbol'],
        'rationale': f"BUY automation candidate: confidence {confidence}%, R/R {rr}:1. Protective stop and target are attached as plan metadata.",
        'risk_notes': f"Risk-capped by {settings['risk_per_trade_pct']}% per trade and {settings['max_position_pct']}% max position size. Uses {currency['symbol']}{sizing.get('capital')} simulation capital for {market_name}. No guaranteed return.",
        'order_payload': {
            'engine': engine,
            'learning_bucket': f"{market_name}:{pick.get('horizon') or 'day'}:{engine}",
            'entry': round(entry, 2), 'target': round(target, 2), 'stop': round(stop, 2),
            'confidence': confidence, 'risk_reward': rr, 'reasoning': pick.get('reasoning'),
            'sizing': sizing,
        }
    }

def broker_post_order(order_payload: Dict[str, Any]) -> Dict[str, Any]:
    if not BROKER_ORDER_WEBHOOK_URL:
        return {'ok': False, 'error': 'BROKER_ORDER_WEBHOOK_URL not configured'}
    data = json.dumps(order_payload, default=_json_safe).encode('utf-8')
    req = urllib.request.Request(
        BROKER_ORDER_WEBHOOK_URL,
        data=data,
        headers={'Content-Type': 'application/json', 'X-Broker-Secret': BROKER_ORDER_WEBHOOK_SECRET},
        method='POST'
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read().decode('utf-8', errors='replace')
            try:
                parsed = json.loads(raw) if raw else {}
            except Exception:
                parsed = {'raw': raw}
            return {'ok': 200 <= resp.status < 300, 'status_code': resp.status, 'response': parsed}
    except urllib.error.HTTPError as e:
        return {'ok': False, 'status_code': e.code, 'error': e.read().decode('utf-8', errors='replace')}
    except Exception as e:
        return {'ok': False, 'error': str(e)}


def persist_trade_order(plan: Dict[str, Any], settings: Dict[str, Any], source: str = 'scan', prediction_id: Optional[int] = None) -> Dict[str, Any]:
    mode = settings['mode']
    engine = str((plan.get('order_payload') or {}).get('engine') or ('intraday' if plan.get('horizon') == 'intraday' else 'trading_automation'))[:40]
    plan.setdefault('order_payload', {})['engine'] = engine
    status = 'PROPOSED'
    broker_response: Dict[str, Any] = {}
    if not settings.get('enabled'):
        status = 'DISABLED'
    elif mode == 'paper':
        status = 'PAPER_EXECUTED'
    elif mode == 'assisted' or settings.get('require_confirmation'):
        status = 'CONFIRMATION_REQUIRED'
    elif mode == 'live':
        if LIVE_TRADING_ENABLED and BROKER_ORDER_WEBHOOK_URL:
            broker_response = broker_post_order({**plan, 'mode': 'live', 'broker': settings['broker']})
            status = 'SUBMITTED' if broker_response.get('ok') else 'REJECTED'
        else:
            status = 'LIVE_BLOCKED'
    idem = f"{mode}:{source}:{plan['market']}:{plan['horizon']}:{plan['ticker']}:{plan['side']}:{get_prediction_lock_key(plan.get('horizon') or 'day')}"
    conn = get_db_connection()
    if conn is None:
        return {'status': status, 'order': plan, 'broker_response': broker_response}
    try:
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    INSERT INTO trade_orders
                        (prediction_id, source, mode, market, horizon, ticker, company, side, quantity, order_type,
                         limit_price, stop_price, target_price, estimated_value, currency_symbol, status, rationale,
                         risk_notes, idempotency_key, broker, broker_response, order_payload, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,NOW())
                    ON CONFLICT (idempotency_key) WHERE (idempotency_key IS NOT NULL) DO UPDATE SET
                        updated_at=NOW(),
                        status=EXCLUDED.status,
                        broker_response=EXCLUDED.broker_response,
                        order_payload=EXCLUDED.order_payload
                    RETURNING *
                """, (
                    prediction_id, source, mode, plan['market'], plan.get('horizon'), plan['ticker'], plan.get('company'), plan['side'],
                    plan['quantity'], plan['order_type'], plan['limit_price'], plan.get('stop_price'), plan.get('target_price'),
                    plan.get('estimated_value'), plan.get('currency_symbol'), status, plan.get('rationale'), plan.get('risk_notes'),
                    idem, settings.get('broker'), json.dumps(broker_response, default=_json_safe), json.dumps(plan.get('order_payload') or {}, default=_json_safe)
                ))
                order = cur.fetchone()
                if status == 'PAPER_EXECUTED' and plan['side'] == 'BUY':
                    cur.execute("""
                        INSERT INTO trade_positions
                            (mode, market, horizon, ticker, company, quantity, entry_price, stop_price, target_price,
                             current_price, status, currency_symbol, source_order_id, metadata_json, engine,
                             expected_round_trip_cost, expected_net_profit_at_target)
                        SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'OPEN',%s,%s,%s::jsonb,%s,%s,%s
                        WHERE NOT EXISTS (
                            SELECT 1 FROM trade_positions WHERE status='OPEN' AND mode=%s AND ticker=%s AND market=%s
                        )
                    """, (
                        mode, plan['market'], plan.get('horizon'), plan['ticker'], plan.get('company'), plan['quantity'],
                        plan['limit_price'], plan.get('stop_price'), plan.get('target_price'), plan['limit_price'],
                        plan.get('currency_symbol'), order['id'], json.dumps(plan.get('order_payload') or {}, default=_json_safe), engine,
                        (plan.get('order_payload') or {}).get('expected_round_trip_cost'),
                        (plan.get('order_payload') or {}).get('expected_net_profit_at_target'),
                        mode, plan['ticker'], plan['market']
                    ))
                return {k: (float(v) if hasattr(v, 'as_tuple') else v) for k, v in dict(order).items()}
    finally:
        conn.close()


def close_trade_position(conn, pos: Dict[str, Any], exit_price: float, reason: str, extra: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    entry = float(pos['entry_price'])
    qty = int(pos['quantity'])
    market = pos.get('market') or 'IN'
    pnl_calc = calculate_net_trade_pnl(entry, exit_price, qty, market)
    gross_pnl = pnl_calc['gross_pnl']
    gross_pct = pnl_calc['gross_pnl_pct']
    cost_amount = pnl_calc['cost_amount']
    net_pnl = pnl_calc['net_pnl']
    net_pct = pnl_calc['net_pnl_pct']
    meta_extra = dict(extra or {})
    meta_extra['pnl_accounting'] = {
        'gross_pnl_amount': gross_pnl,
        'gross_pnl_pct': gross_pct,
        'cost_amount': cost_amount,
        'net_pnl_amount': net_pnl,
        'net_pnl_pct': net_pct,
        'price_move_pct': pnl_calc['price_move_pct'],
        'cost_breakdown': pnl_calc['cost_breakdown'],
        'note': 'pnl_amount and pnl_pct are stored as NET after estimated trading costs.',
    }
    with conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                UPDATE trade_positions
                SET status='CLOSED', closed_at=NOW(), current_price=%s, exit_price=%s,
                    exit_reason=%s, pnl_amount=%s, pnl_pct=%s,
                    gross_pnl_amount=%s, cost_amount=%s, net_pnl_amount=%s, pnl_cost_json=%s::jsonb,
                    metadata_json = COALESCE(metadata_json, '{}'::jsonb) || %s::jsonb
                WHERE id=%s AND status='OPEN'
                RETURNING *
            """, (exit_price, exit_price, reason, net_pnl, net_pct, gross_pnl, cost_amount, net_pnl, json.dumps(pnl_calc['cost_breakdown'], default=_json_safe), json.dumps(meta_extra, default=_json_safe), pos['id']))
            row = cur.fetchone()
            return {k: (float(v) if hasattr(v, 'as_tuple') else v) for k, v in dict(row).items()} if row else None


def parse_jsonish(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def fetch_position_daily_risk_snapshot(ticker: str, market: Optional[str] = None) -> Dict[str, Any]:
    """Daily/weekly post-entry evidence. It is used to ask: is the setup still valid?"""
    source_info = get_market_data_source_info(market or 'IN', 'post_entry_daily_risk_recheck')
    try:
        hist = yf.Ticker(ticker).history(period='6mo', interval='1d', timeout=YF_TIMEOUT)
        hist = normalize_history_frame(hist)
        if hist is None or hist.empty or len(hist) < 35:
            return {'ok': False, 'ticker': ticker, 'reason': 'not_enough_daily_history', 'data_source': source_info}
        close = hist['Close'].dropna().astype(float)
        if close.empty:
            return {'ok': False, 'ticker': ticker, 'reason': 'no_daily_close_data', 'data_source': source_info}
        snap = build_stock_snapshot(ticker, hist)
        if not snap:
            return {'ok': False, 'ticker': ticker, 'reason': 'daily_snapshot_failed', 'data_source': source_info}
        recent = [float(x) for x in close.tail(5).tolist()]
        daily_trend = 'flat'
        if len(recent) >= 3:
            if recent[-1] < recent[-2] < recent[-3]:
                daily_trend = 'falling'
            elif recent[-1] > recent[-2] > recent[-3]:
                daily_trend = 'rising'
        try:
            current = float(snap.get('current_price') or 0)
            support = float(snap.get('support_20') or current * 0.97)
            resistance = float(snap.get('resistance_20') or current * 1.04)
            snap['rr_proxy'] = round(((resistance-current)/max(current,0.01)) / max(((current-support)/max(current,0.01)), 0.01), 2)
        except Exception:
            snap['rr_proxy'] = None
        snap['ok'] = True
        snap['daily_recent_trend'] = daily_trend
        snap['daily_recent_closes'] = [round(x, 2) for x in recent]
        snap['atr_pct'] = compute_atr_pct_from_history(hist)
        snap['regime_info'] = classify_regime_from_history(hist)
        snap['data_source'] = source_info
        return snap
    except Exception as e:
        return {'ok': False, 'ticker': ticker, 'reason': str(e), 'data_source': source_info}


def get_strategy_stat_for_pattern(market: str, horizon: str, pattern_key: str, regime: Optional[str] = None) -> Optional[Dict[str, Any]]:
    conn = get_db_connection()
    if conn is None or not pattern_key:
        return None
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if regime:
                cur.execute("""
                    SELECT regime, pattern_key, trades, win_rate_pct, expectancy_pct, confidence_adjustment, notes
                    FROM strategy_signal_stats
                    WHERE market=%s AND horizon=%s AND pattern_key=%s AND (regime=%s OR regime='all')
                    ORDER BY CASE WHEN regime=%s THEN 0 ELSE 1 END, trades DESC, updated_at DESC
                    LIMIT 1
                """, (market, horizon, pattern_key, regime, regime))
            else:
                cur.execute("""
                    SELECT regime, pattern_key, trades, win_rate_pct, expectancy_pct, confidence_adjustment, notes
                    FROM strategy_signal_stats
                    WHERE market=%s AND horizon=%s AND pattern_key=%s
                    ORDER BY trades DESC, updated_at DESC
                    LIMIT 1
                """, (market, horizon, pattern_key))
            row = cur.fetchone()
            return dict(row) if row else None
    except Exception as e:
        print(f"[WARN] Strategy stat lookup failed for {pattern_key}: {e}")
        return None
    finally:
        conn.close()


def evaluate_post_entry_risk_recheck(pos: Dict[str, Any], settings: Dict[str, Any], snap: Dict[str, Any],
                                     market_context: Optional[Dict[str, Any]] = None,
                                     goal_status: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Re-check an open position after entry.

    Goal: protect capital/profit without panic-selling. Full exits require multiple
    actual weakening signals or serious macro/market risk. Otherwise we only hold
    or tighten the stop.
    """
    if not POST_ENTRY_RISK_RECHECK_ENABLED:
        return {'enabled': False, 'action': 'HOLD', 'reason': 'post_entry_recheck_disabled'}
    entry = _num(pos.get('entry_price'), 0.0) or 0.0
    last = _num(snap.get('last'), _num(pos.get('current_price'), entry)) or entry
    stop = _num(pos.get('stop_price'), 0.0) or 0.0
    target = _num(pos.get('target_price'), 0.0) or 0.0
    if entry <= 0 or last <= 0:
        return {'enabled': True, 'action': 'HOLD', 'reason': 'missing_entry_or_price'}
    market = pos.get('market') or 'IN'
    horizon = str(pos.get('horizon') or 'day').lower()
    ticker = str(pos.get('ticker') or '').upper()
    current_pnl_pct = round(((last - entry) / entry) * 100, 2)
    target_progress_pct = round(((last - entry) / max(target - entry, 0.01)) * 100, 2) if target > entry else 0.0
    daily = fetch_position_daily_risk_snapshot(ticker, market)
    combined = dict(daily if daily.get('ok') else {})
    combined.update({
        'ticker': ticker,
        'last': last,
        'current_price': last,
        'volume_multiplier': snap.get('volume_multiplier'),
        'intraday_recent_trend': snap.get('recent_trend'),
        'intraday_vwap': snap.get('vwap'),
        'intraday_above_vwap': bool(snap.get('vwap') and last > float(snap.get('vwap'))),
    })
    regime = (combined.get('regime_info') or {}).get('regime') or (market_context or {}).get('regime')
    pattern_key = combined.get('strategy_pattern_key') or backtest_pattern_key({**combined, 'rr_proxy': combined.get('rr_proxy')})
    stat = get_strategy_stat_for_pattern(market, horizon if horizon in ('day','week') else 'day', pattern_key, regime=regime)

    risk_score = 0.0
    weak_signals: List[str] = []
    protective_signals: List[str] = []

    def add(points: float, label: str):
        nonlocal risk_score
        risk_score += points
        weak_signals.append(label)

    # Price action weakening. Intraday uses VWAP/opening momentum; day/week use daily trend and MAs.
    if snap.get('recent_trend') == 'falling':
        add(18, 'recent intraday momentum is falling')
    if snap.get('vwap') and last < float(snap.get('vwap')):
        add(18, 'price is below VWAP')
    if combined.get('daily_recent_trend') == 'falling':
        add(14, 'daily closes are weakening')
    if _num(combined.get('price_vs_ma20_pct'), 0) < -0.5:
        add(14, 'price is below 20-day trend')
    if _num(combined.get('macd_histogram'), 0) <= 0:
        add(10, 'MACD momentum is weak/negative')
    if _num(combined.get('rsi_14'), 50) < 45:
        add(10, 'RSI has weakened')
    vol_ratio = _num(combined.get('volume_ratio_vs_20d_avg'), _num(snap.get('volume_multiplier'), 1.0))
    if vol_ratio is not None and vol_ratio < 0.65 and current_pnl_pct < 0:
        add(8, 'volume confirmation is weak while trade is losing')

    # Historical learning: if the current live setup now resembles a pattern that failed, increase caution.
    if stat and int(stat.get('trades') or 0) >= STRICT_PATTERN_RELIABLE_MIN_TRADES:
        win = float(stat.get('win_rate_pct') or 0)
        exp = float(stat.get('expectancy_pct') or 0)
        if win < STRICT_PER_STOCK_MIN_WIN_RATE or exp < 0:
            add(16, f'backtest memory says similar pattern was weak ({round(win,1)}% worked, avg {round(exp,3)}%)')
        else:
            protective_signals.append(f'backtest memory still acceptable ({round(win,1)}% worked, avg {round(exp,3)}%)')
    elif stat:
        protective_signals.append('pattern memory has low sample size; not used alone for exit')

    trend_direction = ((market_context or {}).get('market_trend') or {}).get('direction') if isinstance(market_context, dict) else None
    if trend_direction == 'bearish':
        add(13, 'broad market trend turned bearish')
    macro = (market_context or {}).get('macro_event_risk') if isinstance(market_context, dict) else None
    macro_block = False
    if isinstance(macro, dict) and macro.get('enabled'):
        mscore = _num(macro.get('risk_score'), 0) or 0
        mlevel = macro.get('risk_level')
        if mlevel == 'high_risk' or mscore >= MACRO_RISK_BLOCK_THRESHOLD:
            macro_block = True
            add(30, f'major macro/news risk is high ({round(mscore,1)})')
        elif mlevel == 'caution' or mscore >= MACRO_RISK_CAUTION_THRESHOLD:
            add(10, f'macro/news risk caution ({round(mscore,1)})')

    # Loss pressure only matters with weakening signals. We do not exit solely because a position is slightly red.
    if current_pnl_pct <= POST_ENTRY_LOSS_EXIT_MIN_PNL_PCT:
        add(10, f'position is losing {current_pnl_pct}%')
    elif current_pnl_pct > 0:
        protective_signals.append(f'position is still profitable ({current_pnl_pct}%)')
    if goal_status and goal_status.get('target_reached') and current_pnl_pct > 0:
        protective_signals.append('weekly target milestone reached; protect gains but do not treat it as a hard profit ceiling')

    weak_count = len(weak_signals)
    action = 'HOLD'
    exit_reason = None
    proposed_stop = None
    plain_reason = 'Trade is still valid; no confirmed weakening cluster.'

    losing_with_confirmed_weakness = current_pnl_pct <= POST_ENTRY_LOSS_EXIT_MIN_PNL_PCT and weak_count >= POST_ENTRY_MIN_WEAK_SIGNALS and risk_score >= POST_ENTRY_EXIT_SCORE_THRESHOLD
    profitable_but_serious_risk = current_pnl_pct > 0 and weak_count >= POST_ENTRY_MIN_WEAK_SIGNALS and risk_score >= POST_ENTRY_PROFIT_EXIT_SCORE_THRESHOLD and (macro_block or snap.get('recent_trend') == 'falling' or (snap.get('vwap') and last < float(snap.get('vwap'))))

    if losing_with_confirmed_weakness:
        action = 'EXIT_NOW'
        exit_reason = 'POST_ENTRY_CAPITAL_PROTECTION_EXIT'
        plain_reason = 'Exited because the trade was losing and multiple weakening/risk signals appeared; capital protection took priority.'
    elif profitable_but_serious_risk:
        action = 'EXIT_NOW'
        exit_reason = 'POST_ENTRY_PROFIT_PROTECTION_EXIT'
        plain_reason = 'Exited a profitable trade because serious weakening/risk signals appeared; profit protection took priority.'
    elif POST_ENTRY_ALLOW_TIGHTEN_STOP and risk_score >= POST_ENTRY_TIGHTEN_SCORE_THRESHOLD and weak_count >= 2:
        action = 'TIGHTEN_STOP'
        buffer = max(0.05, POST_ENTRY_TIGHTEN_BUFFER_PCT) / 100.0
        if current_pnl_pct > 0:
            proposed_stop = max(stop or 0, entry, last * (1 - buffer))
            plain_reason = 'Weakness appeared, but not enough for exit. Stop was tightened to protect profit while allowing upside.'
        else:
            proposed_stop = max(stop or 0, last * (1 - buffer))
            plain_reason = 'Weakness appeared, but not enough for immediate exit. Stop was tightened to reduce possible loss.'
        proposed_stop = round(min(proposed_stop, last * 0.999), 2)
    return {
        'enabled': True,
        'action': action,
        'exit_reason': exit_reason,
        'plain_reason': plain_reason,
        'ticker': ticker,
        'horizon': horizon,
        'current_pnl_pct': current_pnl_pct,
        'target_progress_pct': target_progress_pct,
        'risk_score': round(risk_score, 1),
        'weak_signal_count': weak_count,
        'weak_signals': weak_signals,
        'protective_signals': protective_signals,
        'pattern_key': pattern_key,
        'pattern_stat': {k: (float(v) if hasattr(v, 'as_tuple') else v) for k, v in dict(stat).items()} if stat else None,
        'daily_risk_snapshot_ok': bool(daily.get('ok')),
        'daily_risk_reason': daily.get('reason'),
        'proposed_stop_price': proposed_stop,
        'policy': 'Do not exit just because price is temporarily red. Exit requires multiple weakening/risk signals or serious macro risk; otherwise tighten stop or hold.',
    }

def evaluate_profit_protection_exit(pos: Dict[str, Any], settings: Dict[str, Any], snap: Dict[str, Any], goal_status: Optional[Dict[str, Any]] = None, market_context: Optional[Dict[str, Any]] = None) -> Tuple[Optional[str], Dict[str, Any]]:
    entry = float(pos.get('entry_price') or 0)
    target = float(pos.get('target_price') or 0)
    stop = float(pos.get('stop_price') or 0)
    last = float(snap.get('last') or pos.get('current_price') or entry)
    high = float(snap.get('high') or last)
    old_peak = float(pos.get('peak_price') or entry)
    peak = max(old_peak, high, last)
    best_pnl_pct = round(((peak - entry) / entry) * 100, 2) if entry else 0
    current_pnl_pct = round(((last - entry) / entry) * 100, 2) if entry else 0
    qty = int(pos.get('quantity') or 0)
    market_code = pos.get('market') or 'IN'
    current_pnl_calc = calculate_net_trade_pnl(entry, last, qty, market_code) if qty and entry else {'net_pnl': 0, 'cost_amount': 0, 'gross_pnl': 0, 'net_pnl_pct': 0}
    current_net_pnl = float(current_pnl_calc.get('net_pnl') or 0)
    min_net_to_protect = get_min_net_profit_to_protect(market_code)
    target_distance = max(target - entry, 0.01)
    progress_pct = round(((last - entry) / target_distance) * 100, 2) if target > entry else 0
    peak_progress_pct = round(((peak - entry) / target_distance) * 100, 2) if target > entry else 0
    trailing_stop_price = float(pos.get('trailing_stop_price') or stop or 0)

    reason = None
    if target and last >= target:
        reason = 'TARGET_HIT'
    elif stop and last <= stop:
        reason = 'STOP_LOSS_HIT'
    elif settings.get('profit_protection_enabled', True) and current_pnl_pct > 0 and current_net_pnl >= min_net_to_protect:
        activation = float(settings.get('trailing_stop_activation_pct') or TRAILING_STOP_ACTIVATION_PCT)
        giveback = float(settings.get('trailing_stop_giveback_pct') or TRAILING_STOP_GIVEBACK_PCT)
        if peak_progress_pct >= activation:
            protected_from_peak = peak - ((peak - entry) * giveback / 100)
            trailing_stop_price = round(max(trailing_stop_price, protected_from_peak, stop), 2)
            if last <= trailing_stop_price:
                reason = 'TRAILING_PROFIT_PROTECT'
        protect_progress = float(settings.get('profit_protect_progress_pct') or PROFIT_PROTECT_PROGRESS_PCT)
        if not reason and progress_pct >= protect_progress:
            if snap.get('recent_trend') == 'falling' and snap.get('vwap') and last < float(snap['vwap']):
                reason = 'PROFIT_PROTECTION_MOMENTUM_FADE'
        if not reason and goal_status and goal_status.get('target_reached') and current_pnl_pct > 0:
            if bool(settings.get('stop_after_weekly_target')):
                reason = 'WEEKLY_GOAL_PROFIT_LOCK'
            else:
                # Weekly target is a milestone, not a ceiling. Keep winners running, but
                # the post-entry risk recheck may tighten stops if weakening appears.
                pass
    if not reason and str(pos.get('horizon') or '').lower() == 'intraday':
        payload = pos.get('metadata_json') or {}
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                payload = {}
        profit_book_pct = max(float(payload.get('profit_book_pct') or 0), INTRADAY_PROFIT_BOOK_PCT)
        if current_pnl_pct >= profit_book_pct and current_net_pnl >= min_net_to_protect:
            if snap.get('recent_trend') == 'falling' or (snap.get('vwap') and last < float(snap['vwap'])):
                reason = 'INTRADAY_QUICK_PROFIT_BOOK'
        if not reason:
            market = pos.get('market') or 'IN'
            sched = get_market_schedule(market)
            tz = ZoneInfo(sched['tz'])
            local_now = datetime.datetime.now(datetime.timezone.utc).astimezone(tz)
            close_local = datetime.datetime.combine(local_now.date(), sched['close'], tzinfo=tz)
            exit_minutes = int(payload.get('force_exit_before_close_minutes') or INTRADAY_FORCE_EXIT_BEFORE_CLOSE_MINUTES)
            if local_now >= close_local - datetime.timedelta(minutes=exit_minutes):
                reason = 'INTRADAY_FORCE_EXIT_BEFORE_CLOSE'
    post_entry = None
    if not reason:
        post_entry = evaluate_post_entry_risk_recheck(pos, settings, snap, market_context=market_context, goal_status=goal_status)
        if post_entry.get('action') == 'EXIT_NOW' and post_entry.get('exit_reason'):
            reason = post_entry.get('exit_reason')
    if not reason and is_position_horizon_due(pos, settings):
        reason = 'HORIZON_END_EXIT'
    meta = {
        'last': round(last, 2),
        'peak': round(peak, 2),
        'best_pnl_pct': best_pnl_pct,
        'current_pnl_pct': current_pnl_pct,
        'target_progress_pct': progress_pct,
        'peak_target_progress_pct': peak_progress_pct,
        'trailing_stop_price': trailing_stop_price,
        'current_net_pnl': round(current_net_pnl, 2),
        'current_estimated_cost': round(float(current_pnl_calc.get('cost_amount') or 0), 2),
        'current_gross_pnl': round(float(current_pnl_calc.get('gross_pnl') or 0), 2),
        'min_net_profit_to_protect': round(min_net_to_protect, 2),
        'recent_trend': snap.get('recent_trend'),
        'vwap': round(float(snap['vwap']), 2) if snap.get('vwap') else None,
        'post_entry_risk_recheck': post_entry,
        'post_entry_action': (post_entry or {}).get('action') if isinstance(post_entry, dict) else None,
        'post_entry_plain_reason': (post_entry or {}).get('plain_reason') if isinstance(post_entry, dict) else None,
        'proposed_stop_price': (post_entry or {}).get('proposed_stop_price') if isinstance(post_entry, dict) else None,
        'weekly_target_policy': 'weekly target is a milestone, not a hard ceiling' if goal_status and goal_status.get('target_reached') else None,
    }
    return reason, meta


def evaluate_open_positions_for_exits(settings: Dict[str, Any], market: Optional[str] = None, goal_status: Optional[Dict[str, Any]] = None, market_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    positions = get_open_positions(market=market, mode=settings.get('mode'))
    closed = []
    updated = []
    errors = []
    conn = get_db_connection()
    if conn is None:
        return {'closed': closed, 'updated': updated, 'errors': ['DATABASE_URL not set']}
    try:
        context_cache = market_context
        if context_cache is None and market:
            try:
                context_cache = get_market_context(market)
            except Exception:
                context_cache = None
        for pos in positions:
            try:
                snap = fetch_intraday_snapshot(pos['ticker'], pos.get('market'))
                last = snap.get('last') if snap.get('ok') else fetch_last_price(pos['ticker'])
                if last is None:
                    continue
                if not snap.get('ok'):
                    snap = {'ok': True, 'last': last, 'high': last, 'low': last, 'recent_trend': 'unknown'}
                reason, meta = evaluate_profit_protection_exit(pos, settings, snap, goal_status=goal_status, market_context=context_cache)
                exit_price = float(snap.get('last') or last)
                if reason:
                    row = close_trade_position(conn, pos, round(exit_price, 2), reason, {'exit_engine': meta})
                    if row:
                        closed.append(row)
                    continue
                with conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE trade_positions
                            SET current_price=%s, peak_price=%s, best_pnl_pct=%s, trailing_stop_price=%s,
                                stop_price = GREATEST(COALESCE(stop_price,0), COALESCE(%s,0)),
                                metadata_json = COALESCE(metadata_json, '{}'::jsonb) || %s::jsonb
                            WHERE id=%s AND status='OPEN'
                        """, (round(exit_price, 2), meta.get('peak'), meta.get('best_pnl_pct'), meta.get('trailing_stop_price'), meta.get('proposed_stop_price'), json.dumps({'exit_engine': meta}, default=_json_safe), pos['id']))
                updated.append({'id': pos['id'], 'ticker': pos['ticker'], 'current_price': round(exit_price, 2), 'exit_engine': meta})
            except Exception as e:
                errors.append(f"{pos.get('ticker')}: {e}")
    finally:
        conn.close()
    return {'closed': closed, 'updated': updated, 'errors': errors}


def run_trading_automation(market: str, horizon: str, settings_override: Optional[Dict[str, Any]] = None, trigger: str = 'manual', order_source: str = 'locked-scan-confirmed') -> Dict[str, Any]:
    started_at = utc_now_naive()
    market, horizon = validate_market_horizon(market, horizon)
    settings = dict(settings_override or get_trading_settings())
    goal_status = get_weekly_goal_status(settings)
    exits = evaluate_open_positions_for_exits(settings, market=market, goal_status=goal_status)
    if not TRADING_ENABLED or not settings.get('enabled'):
        return finalize_trading_automation_result({'ok': True, 'enabled': False, 'trigger': trigger, 'message': 'Trading automation is disabled.', 'settings': settings, 'goal_status': goal_status, 'exits': exits, 'orders': [], 'entry_checks': [], 'locked_scan_available': False}, started_at, market, horizon)
    if not goal_status.get('can_open_new_trades'):
        return finalize_trading_automation_result({
            'ok': True,
            'enabled': True,
            'trigger': trigger,
            'settings': settings,
            'goal_status': goal_status,
            'exits': exits,
            'orders': [],
            'entry_checks': [],
            'locked_scan_available': False,
            'opening_status': get_market_session_state(market, settings.get('opening_wait_minutes')),
            'message': 'No new entries opened because the weekly portfolio goal/loss guard is active: ' + str(goal_status.get('halt_reason'))
        }, started_at, market, horizon)
    source_meta: Dict[str, Any] = {'selection_mode': settings.get('selection_mode') or 'locked_scan'}
    try:
        cached, source_meta = resolve_trading_candidate_source(market, horizon, settings)
    except Exception as e:
        return finalize_trading_automation_result({
            'ok': False,
            'trigger': trigger,
            'error': 'Trading Automation self-scan failed: ' + str(e)[:240],
            'message': 'Trading Automation self-scan failed: ' + str(e)[:240],
            'settings': settings,
            'goal_status': goal_status,
            'exits': exits,
            'orders': [],
            'entry_checks': [],
            'locked_scan_available': False,
            'selection_mode': source_meta.get('selection_mode') or settings.get('selection_mode') or 'locked_scan',
            'candidate_source': source_meta,
        }, started_at, market, horizon)
    if not cached:
        mode_msg = 'No locked scan is available. Run First Scan first.'
        if (settings.get('selection_mode') or 'locked_scan') == 'auto':
            mode_msg = 'No locked scan is available and auto self-scan could not produce candidates.'
        elif (settings.get('selection_mode') or 'locked_scan') == 'self_scan':
            mode_msg = 'Self-scan did not produce candidates from the configured universe.'
        return finalize_trading_automation_result({'ok': False, 'trigger': trigger, 'error': mode_msg, 'message': mode_msg, 'settings': settings, 'goal_status': goal_status, 'exits': exits, 'orders': [], 'entry_checks': [], 'locked_scan_available': False, 'selection_mode': settings.get('selection_mode') or 'locked_scan', 'candidate_source': source_meta}, started_at, market, horizon)
    open_positions = get_open_positions(market=market, mode=settings.get('mode'))
    open_tickers = {p['ticker'] for p in open_positions}
    slots = max(settings['max_open_positions'] - len(open_tickers), 0)
    orders = []
    entry_checks = []
    trade_history_memory = get_closed_trade_history_strength(market, horizon, engine='trading')
    market_context = get_market_context(market)
    for pick in cached.get('picks', []):
        if slots <= 0:
            break
        pick = dict(pick)
        pick['market'] = market
        pick['horizon'] = horizon
        if pick.get('ticker') in open_tickers:
            continue
        plan = build_trade_plan_from_pick(pick, settings)
        if not plan:
            entry_checks.append({
                'ticker': pick.get('ticker'),
                'company': pick.get('company'),
                'allowed': False,
                'reason': explain_trade_plan_skip(pick, settings),
                'source': trigger,
            })
            continue
        strict_trade = strict_learning_review({**pick, **plan}, market, horizon, engine='trading_automation', market_context=market_context, trade_memory=trade_history_memory)
        if strict_trade.get('enabled') and not strict_trade.get('allowed'):
            entry_checks.append({
                'ticker': plan['ticker'],
                'company': plan.get('company'),
                'allowed': False,
                'reason': 'Strict Learning Mode blocked automation entry: ' + '; '.join((strict_trade.get('hard_blocks') or strict_trade.get('cautions') or [])[:2]),
                'strict_learning': strict_trade,
            })
            continue
        plan['order_payload']['strict_learning'] = strict_trade
        plan['risk_notes'] = (plan.get('risk_notes') or '') + f" Strict Learning Mode: {strict_trade.get('verdict', 'checked')}."
        confirmation = evaluate_opening_confirmation(plan, settings)
        entry_checks.append({
            'ticker': plan['ticker'],
            'company': plan.get('company'),
            'allowed': confirmation.get('allowed'),
            'reason': confirmation.get('reason'),
            'session': confirmation.get('session'),
            'snapshot': confirmation.get('snapshot'),
            'confirmed_rr': confirmation.get('confirmed_rr'),
            'strict_learning': plan.get('order_payload', {}).get('strict_learning'),
        })
        if not confirmation.get('allowed'):
            continue
        if confirmation.get('confirmed_entry'):
            confirmed_entry = float(confirmation['confirmed_entry'])
            plan['limit_price'] = round(confirmed_entry, 2)
            plan['estimated_value'] = round(int(plan['quantity']) * confirmed_entry, 2)
            plan['order_payload']['confirmed_entry'] = round(confirmed_entry, 2)
            plan['order_payload']['opening_confirmation'] = confirmation
            plan['risk_notes'] = (plan.get('risk_notes') or '') + f" Opening confirmation passed at {plan['currency_symbol']}{confirmed_entry:.2f}; R/R after open {confirmation.get('confirmed_rr')}:1."
        plan['risk_notes'] = (plan.get('risk_notes') or '') + f" Weekly goal guard: net P/L after API costs ₹{goal_status.get('net_after_api_inr', 0)} / target ₹{goal_status.get('target_profit_inr', 0)}; API cost this week ₹{goal_status.get('api_cost_inr', 0)}."
        order = persist_trade_order(plan, settings, source=source_meta.get('order_source') or order_source)
        orders.append(order)
        slots -= 1
    return finalize_trading_automation_result({
        'ok': True,
        'enabled': True,
        'settings': settings,
        'mode': settings.get('mode'),
        'live_trading_enabled': LIVE_TRADING_ENABLED,
        'broker_configured': bool(BROKER_ORDER_WEBHOOK_URL),
        'execution_policy': get_execution_policy(),
        'ai_budget': get_ai_budget_status(),
        'goal_status': goal_status,
        'opening_status': get_market_session_state(market, settings.get('opening_wait_minutes')),
        'exits': exits,
        'market_context': market_context,
        'macro_event_risk': (market_context or {}).get('macro_event_risk') if isinstance(market_context, dict) else None,
        'entry_checks': entry_checks,
        'orders': orders,
        'trigger': trigger,
        'order_source': source_meta.get('order_source') or order_source,
        'locked_scan_available': bool(source_meta.get('locked_scan_available')),
        'selection_mode': source_meta.get('selection_mode') or settings.get('selection_mode') or 'locked_scan',
        'candidate_source': source_meta,
        'data_source': cached.get('data_source') or get_market_data_source_info(market, 'trading_automation_check'),
        'source_scan': {
            'self_scan': bool(cached.get('self_scan')),
            'model': cached.get('model'),
            'funnel_counts': cached.get('funnel_counts'),
            'methodology': cached.get('methodology'),
            'data_source': cached.get('data_source'),
        },
        'learning_architecture': strict_learning_architecture(),
        'message': ('Trading automation used self-scan expert-mode candidates because selection_mode=' + str(source_meta.get('selection_mode')) + '. Opening confirmation, strict filters and profit-protection exits are deterministic; no Claude call is made for buy/sell execution.') if source_meta.get('used_source') == 'self_scan' else 'Trading automation used locked scan + trade-history learning only. Opening confirmation, strict filters and profit-protection exits are deterministic; no Claude call is made for buy/sell execution.'
    }, started_at, market, horizon)


def get_trading_dashboard(market: str, horizon: str) -> Dict[str, Any]:
    market, horizon = validate_market_horizon(market, horizon)
    settings = get_trading_settings()
    goal_status = get_weekly_goal_status(settings)
    exits = evaluate_open_positions_for_exits(settings, market=market, goal_status=goal_status)
    conn = get_db_connection()
    if conn is None:
        recent_auto_runs = []
        return {'settings': settings, 'goal_status': goal_status, 'opening_status': get_market_session_state(market, settings.get('opening_wait_minutes')), 'execution_policy': get_execution_policy(), 'ai_budget': get_ai_budget_status(), 'orders': [], 'positions': [], 'summary': {'open_positions': 0, 'paper_pnl': 0}, 'recent_auto_runs': recent_auto_runs, 'entry_checks': []}
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM trade_orders ORDER BY created_at DESC LIMIT 40")
            orders = cur.fetchall()
            cur.execute("SELECT * FROM trade_positions ORDER BY opened_at DESC LIMIT 40")
            positions = cur.fetchall()
            cur.execute("""
                SELECT COUNT(*) FILTER (WHERE status='OPEN') AS open_positions,
                       COALESCE(SUM(pnl_amount) FILTER (WHERE status='CLOSED'),0) AS realised_pnl,
                       COUNT(*) FILTER (WHERE status='CLOSED') AS closed_positions
                FROM trade_positions
            """)
            summary = cur.fetchone()
            cur.execute("""
                SELECT market, COALESCE(engine, CASE WHEN horizon='intraday' THEN 'intraday' ELSE 'trading_automation' END) AS engine,
                       COALESCE(currency_symbol, CASE WHEN market='IN' THEN '₹' ELSE '$' END) AS currency_symbol,
                       COUNT(*) FILTER (WHERE status='OPEN') AS open_positions,
                       COUNT(*) FILTER (WHERE status='CLOSED') AS closed_positions,
                       COALESCE(SUM(pnl_amount) FILTER (WHERE status='CLOSED'),0) AS realised_pnl
                FROM trade_positions
                GROUP BY market, engine, currency_symbol
                ORDER BY market, engine
            """)
            summary_by_market = cur.fetchall()
    finally:
        conn.close()
    def clean_row(row):
        out = {}
        for k, v in dict(row).items():
            if hasattr(v, 'as_tuple'):
                out[k] = float(v)
            elif isinstance(v, (datetime.datetime, datetime.date)):
                out[k] = v.isoformat()
            else:
                out[k] = v
        return out
    recent_auto_runs = get_recent_trading_automation_runs(20)
    latest_checks = (((recent_auto_runs[0] if recent_auto_runs else {}).get('result_json') or {}).get('entry_checks') or [])
    return {
        'settings': settings,
        'regulatory_notice': 'Live orders should be routed only through a SEBI/broker-compliant API/algo flow with broker approval, unique order/algo identifiers, and audit trails.',
        'live_trading_enabled': LIVE_TRADING_ENABLED,
        'broker_configured': bool(BROKER_ORDER_WEBHOOK_URL),
        'execution_policy': get_execution_policy(),
        'ai_budget': get_ai_budget_status(),
        'goal_status': goal_status,
        'opening_status': get_market_session_state(market, settings.get('opening_wait_minutes')),
        'exits': exits,
        'orders': [clean_row(r) for r in orders],
        'positions': [clean_row(r) for r in positions],
        'summary': clean_row(summary) if summary else {'open_positions': 0, 'realised_pnl': 0, 'closed_positions': 0},
        'summary_by_market_engine': [clean_row(r) for r in (summary_by_market or [])],
        'paper_auto_state': get_paper_auto_state(),
        'recent_auto_runs': recent_auto_runs,
        'entry_checks': latest_checks,
        'selection_policy': {
            'locked_scan': 'Original mode: use saved First Scan prediction only.',
            'self_scan': 'Independent mode: Trading Automation scans the configured NSE/US universe itself using deterministic professional-analysis filters, then applies opening confirmation and risk controls.',
            'auto': 'Use locked First Scan when available; otherwise fall back to self-scan.',
            'current': settings.get('selection_mode') or 'locked_scan',
        },
    }


def run_accuracy_check(market: Optional[str] = None, horizon: Optional[str] = None, triggered_by: str = 'manual') -> Dict[str, Any]:
    conn = get_db_connection()
    if conn is None:
        return {'error': 'Prediction tracking disabled - DATABASE_URL not set.', 'updated': 0, 'tickers_checked': 0, 'errors': []}

    updated = 0
    errors = []
    due_rows = []
    now_utc = utc_now_naive()
    filters = ["checked = FALSE", "check_date <= %s"]
    params: List[Any] = [now_utc]
    if market:
        filters.append("market = %s")
        params.append(market)
    if horizon:
        filters.append("horizon = %s")
        params.append(horizon)
    where_sql = " AND ".join(filters)

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"""
                SELECT id, scan_timestamp, market, horizon, ticker, signal, entry_price, target_price, stop_price,
                       predicted_gain_pct, confidence, reasoning, check_date, features_json
                FROM predictions
                WHERE {where_sql}
                ORDER BY check_date ASC
            """, params)
            due_rows = cur.fetchall()

        if not due_rows:
            return {
                'updated': 0,
                'tickers_checked': 0,
                'errors': [],
                'skipped': True,
                'reason': 'no_due_unchecked_predictions_after_exchange_close',
                'market': market,
                'horizon': horizon,
                'triggered_by': triggered_by,
                'verified_after_close': True,
            }

        for row in due_rows:
            ticker = row['ticker']
            try:
                scan_ts = row.get('scan_timestamp') or now_utc
                check_dt = row.get('check_date') or now_utc
                prices = fetch_prediction_price_window(ticker, row.get('market') or market or 'US', scan_ts, check_dt)
                if not prices:
                    errors.append(f"{ticker}: no price data")
                    continue
                scored = score_prediction_outcome(row, prices)
                row_updated = 0
                with conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE predictions
                            SET checked=TRUE, actual_price=%s, actual_high=%s, actual_low=%s, actual_gain_pct=%s,
                                outcome_correct=%s, outcome_label=%s, outcome_status=%s, failure_reason=%s,
                                lesson_summary=%s, checked_at=%s
                            WHERE id=%s AND checked=FALSE
                        """, (
                            scored['actual_price'], scored['actual_high'], scored['actual_low'], scored['actual_gain_pct'],
                            scored['outcome_correct'], scored['outcome_label'], scored['outcome_status'], scored['failure_reason'],
                            scored['lesson_summary'], now_utc, row['id']
                        ))
                        row_updated = cur.rowcount
                        updated += row_updated
                if row_updated:
                    save_prediction_lesson(row, scored)
            except Exception as e:
                errors.append(f"{ticker}: {e}")
                continue
    finally:
        conn.close()

    return {
        'updated': updated,
        'tickers_checked': len({r['ticker'] for r in due_rows}),
        'rows_due': len(due_rows),
        'errors': errors,
        'skipped': False,
        'market': market,
        'horizon': horizon,
        'triggered_by': triggered_by,
        'verified_after_close': True,
    }


def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def _jsonable_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in dict(row or {}).items():
        if hasattr(v, 'as_tuple'):
            out[k] = float(v)
        elif isinstance(v, (datetime.datetime, datetime.date)):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


def _ensure_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _engine_label(source: Optional[str], horizon: Optional[str]) -> str:
    source = (source or '').lower()
    horizon = (horizon or '').lower()
    if source == 'intraday-engine' or horizon == 'intraday':
        return 'Intraday Engine'
    if source in ('locked-scan-confirmed', 'scan') or horizon in ('day', 'week'):
        return 'Trading Automation'
    return source.replace('-', ' ').title() if source else 'Trading Automation'


def _extract_trade_metrics(metadata: Any, order_payload: Any = None) -> Dict[str, Any]:
    """Extract useful price/volume diagnostics stored when the trade was created/updated."""
    meta = _ensure_dict(metadata)
    order = _ensure_dict(order_payload)
    # Position metadata is usually the order payload. The joined order payload is
    # kept as fallback for older positions.
    source = meta or order
    candidate = _ensure_dict(source.get('candidate'))
    opening = _ensure_dict(source.get('opening_confirmation'))
    snapshot = _ensure_dict(opening.get('snapshot')) or _ensure_dict(source.get('snapshot'))
    if not candidate and snapshot:
        candidate = snapshot

    last_bar_volume = _safe_float(candidate.get('last_bar_volume'))
    avg_bar_volume = _safe_float(candidate.get('avg_bar_volume'))
    volume_multiplier = _safe_float(candidate.get('volume_multiplier'))
    if volume_multiplier is None and last_bar_volume is not None and avg_bar_volume:
        volume_multiplier = last_bar_volume / avg_bar_volume if avg_bar_volume else None

    return {
        'entry_score': _safe_float(candidate.get('score')),
        'entry_change_pct': _safe_float(candidate.get('change_pct')),
        'entry_vwap': _safe_float(candidate.get('vwap')),
        'entry_breakout': candidate.get('breakout'),
        'entry_trend': candidate.get('recent_trend') or candidate.get('trend'),
        'volume_multiplier': round(volume_multiplier, 2) if volume_multiplier is not None else None,
        'last_bar_volume': round(last_bar_volume, 0) if last_bar_volume is not None else None,
        'avg_bar_volume': round(avg_bar_volume, 0) if avg_bar_volume is not None else None,
        'best_pnl_pct': _safe_float(source.get('best_pnl_pct')),
        'target_progress_pct': _safe_float(source.get('target_progress_pct')),
        'peak_target_progress_pct': _safe_float(source.get('peak_target_progress_pct')),
        'trailing_stop_price': _safe_float(source.get('trailing_stop_price')),
    }


def _trade_position_to_history(row: Dict[str, Any]) -> Dict[str, Any]:
    entry = _safe_float(row.get('entry_price'), 0) or 0
    qty = _safe_int(row.get('quantity'), 0)
    exit_price = _safe_float(row.get('exit_price'))
    current_price = _safe_float(row.get('current_price')) or exit_price or entry
    status = row.get('status') or 'OPEN'
    market = row.get('market') or 'IN'
    sell_or_mark_price = exit_price if status == 'CLOSED' and exit_price is not None else current_price
    buy_value = round(entry * qty, 2)
    sell_value = round((sell_or_mark_price or 0) * qty, 2) if sell_or_mark_price is not None else None
    pnl_amount = _safe_float(row.get('pnl_amount'))
    pnl_pct = _safe_float(row.get('pnl_pct'))
    gross_pnl_amount = _safe_float(row.get('gross_pnl_amount'))
    cost_amount = _safe_float(row.get('cost_amount'))
    net_pnl_amount = _safe_float(row.get('net_pnl_amount'))
    pnl_accounting = _ensure_dict(_ensure_dict(row.get('metadata_json')).get('pnl_accounting'))
    if gross_pnl_amount is None:
        gross_pnl_amount = _safe_float(pnl_accounting.get('gross_pnl_amount'))
    if cost_amount is None:
        cost_amount = _safe_float(pnl_accounting.get('cost_amount'))
    if net_pnl_amount is None:
        net_pnl_amount = _safe_float(pnl_accounting.get('net_pnl_amount'))
    if status != 'CLOSED' and current_price and entry:
        live_calc = calculate_net_trade_pnl(entry, current_price, qty, market)
        gross_pnl_amount = live_calc.get('gross_pnl')
        cost_amount = live_calc.get('cost_amount')
        net_pnl_amount = live_calc.get('net_pnl')
        pnl_amount = net_pnl_amount
        pnl_pct = live_calc.get('net_pnl_pct')
    elif status == 'CLOSED':
        if net_pnl_amount is not None:
            pnl_amount = net_pnl_amount
        if pnl_pct is None and entry and qty and pnl_amount is not None:
            pnl_pct = round((float(pnl_amount) / (entry * qty)) * 100, 4)
    metrics = _extract_trade_metrics(row.get('metadata_json'), row.get('order_payload'))
    ticker = row.get('ticker')
    currency = row.get('currency_symbol') or get_currency_info(market, ticker)['symbol']
    return {
        'id': row.get('id'),
        'engine': _engine_label(row.get('source'), row.get('horizon')),
        'source': row.get('source'),
        'mode': row.get('mode'),
        'market': market,
        'horizon': row.get('horizon'),
        'ticker': ticker,
        'company': row.get('company'),
        'quantity': qty,
        'entry_price': round(entry, 2) if entry else None,
        'buy_value': buy_value,
        'target_price': _safe_float(row.get('target_price')),
        'stop_price': _safe_float(row.get('stop_price')),
        'current_price': round(current_price, 2) if current_price is not None else None,
        'exit_price': round(exit_price, 2) if exit_price is not None else None,
        'sell_value': sell_value,
        'pnl_amount': round(pnl_amount, 2) if pnl_amount is not None else None,
        'pnl_pct': round(pnl_pct, 2) if pnl_pct is not None else None,
        'gross_pnl_amount': round(gross_pnl_amount, 2) if gross_pnl_amount is not None else None,
        'cost_amount': round(cost_amount, 2) if cost_amount is not None else None,
        'net_pnl_amount': round(net_pnl_amount, 2) if net_pnl_amount is not None else None,
        'expected_round_trip_cost': _safe_float(row.get('expected_round_trip_cost')),
        'expected_net_profit_at_target': _safe_float(row.get('expected_net_profit_at_target')),
        'status': status,
        'exit_reason': row.get('exit_reason'),
        'opened_at': row.get('opened_at').isoformat() if row.get('opened_at') else None,
        'closed_at': row.get('closed_at').isoformat() if row.get('closed_at') else None,
        'order_status': row.get('order_status'),
        'source_order_id': row.get('source_order_id'),
        'currency_symbol': currency,
        **metrics,
    }


def get_trade_history_dashboard(source: str = 'all', market: Optional[str] = None, mode: Optional[str] = None, limit: int = 100) -> Dict[str, Any]:
    """Unified executed-trade history for Trading Automation + Intraday Engine."""
    settings = get_trading_settings()
    # Before reading history, update/close any open paper positions whose target,
    # stop, trailing protection, horizon end, or intraday forced-exit rule has hit.
    exits = evaluate_open_positions_for_exits(settings, market=market or None, goal_status=get_weekly_goal_status(settings))

    conn = get_db_connection()
    if conn is None:
        return {'error': 'Trade history disabled - DATABASE_URL not set.', 'history': [], 'summary': {}, 'exits': exits}

    filters = []
    params: List[Any] = []
    src = (source or 'all').lower()
    if src == 'intraday':
        filters.append("(p.horizon='intraday' OR o.source='intraday-engine')")
    elif src in ('trading', 'automation', 'main'):
        filters.append("(p.horizon IS DISTINCT FROM 'intraday' AND COALESCE(o.source,'') <> 'intraday-engine')")
    if market and market != 'all':
        filters.append('p.market=%s')
        params.append(market)
    if mode and mode != 'all':
        filters.append('p.mode=%s')
        params.append(mode)
    where_sql = ('WHERE ' + ' AND '.join(filters)) if filters else ''
    limit = max(1, min(int(limit or 100), 300))

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"""
                SELECT p.*, o.source, o.status AS order_status, o.created_at AS order_created_at,
                       o.order_payload, o.rationale, o.risk_notes
                FROM trade_positions p
                LEFT JOIN trade_orders o ON p.source_order_id = o.id
                {where_sql}
                ORDER BY COALESCE(p.closed_at, p.opened_at) DESC
                LIMIT %s
            """, params + [limit])
            rows = cur.fetchall()

            cur.execute(f"""
                SELECT
                    COUNT(*) AS total_positions,
                    COUNT(*) FILTER (WHERE p.status='OPEN') AS open_positions,
                    COUNT(*) FILTER (WHERE p.status='CLOSED') AS closed_positions,
                    COUNT(*) FILTER (WHERE p.status='CLOSED' AND p.pnl_amount > 0) AS winning_trades,
                    COUNT(*) FILTER (WHERE p.status='CLOSED' AND p.pnl_amount < 0) AS losing_trades,
                    COALESCE(SUM(p.pnl_amount) FILTER (WHERE p.status='CLOSED'),0) AS realised_pnl,
                    COALESCE(AVG(p.pnl_pct) FILTER (WHERE p.status='CLOSED'),0) AS avg_pnl_pct,
                    COALESCE(SUM(p.quantity * p.entry_price),0) AS total_buy_value,
                    COALESCE(SUM(p.quantity * COALESCE(p.exit_price, p.current_price)),0) AS total_sell_or_mark_value,
                    COALESCE(SUM(p.pnl_amount) FILTER (WHERE p.status='CLOSED' AND (p.horizon='intraday' OR o.source='intraday-engine')),0) AS intraday_realised_pnl,
                    COALESCE(SUM(p.pnl_amount) FILTER (WHERE p.status='CLOSED' AND (p.horizon IS DISTINCT FROM 'intraday' AND COALESCE(o.source,'') <> 'intraday-engine')),0) AS trading_realised_pnl
                FROM trade_positions p
                LEFT JOIN trade_orders o ON p.source_order_id = o.id
                {where_sql}
            """, params)
            summary = cur.fetchone() or {}

            # Current India calendar day summary for the dashboard card. This is
            # display-only; exchange-specific history remains in each trade row.
            tz = ZoneInfo('Asia/Kolkata')
            now_local = datetime.datetime.now(tz)
            day_start = datetime.datetime.combine(now_local.date(), datetime.time(0, 0), tzinfo=tz).astimezone(datetime.timezone.utc).replace(tzinfo=None)
            day_end = day_start + datetime.timedelta(days=1)
            cur.execute(f"""
                SELECT COUNT(*) AS trades_today,
                       COUNT(*) FILTER (WHERE p.status='CLOSED') AS closed_today,
                       COALESCE(SUM(p.pnl_amount) FILTER (WHERE p.status='CLOSED'),0) AS realised_pnl_today
                FROM trade_positions p
                LEFT JOIN trade_orders o ON p.source_order_id = o.id
                {where_sql + (' AND ' if where_sql else 'WHERE ')} p.opened_at >= %s AND p.opened_at < %s
            """, params + [day_start, day_end])
            today = cur.fetchone() or {}
    finally:
        conn.close()

    history = [_trade_position_to_history(dict(r)) for r in rows]
    s = _jsonable_row(summary)
    closed = int(s.get('closed_positions') or 0)
    wins = int(s.get('winning_trades') or 0)
    s['win_rate_pct'] = round((wins / closed) * 100, 1) if closed else None
    return {
        'history': history,
        'summary': s,
        'today': _jsonable_row(today),
        'exits': exits,
        'filters': {'source': source, 'market': market or 'all', 'mode': mode or 'all', 'limit': limit},
        'policy': 'Unified trade history covers both Trading Automation and Intraday Engine. P/L is realised after position close; open trades show mark-to-market using latest checked price. Claude is never called for buy/sell/exit history updates.',
        'difference': {
            'trading_automation': 'Uses locked AI/day/week scan predictions, waits for opening confirmation, then manages target/stop/profit-protection/horizon exits.',
            'intraday_engine': 'Separate live 5-minute bullish scanner for quick same-day entries/exits. It does not depend on Claude rankings and exits before market close.'
        }
    }


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/scan', methods=['POST'])
def scan():
    # Fast path: return PostgreSQL precomputed results. No AI ranking or full
    # market scan happens here. If old predictions are due, the app performs a
    # small, idempotent accuracy check once and then marks them checked.
    body = request.get_json(silent=True) or {}
    market, horizon = validate_market_horizon(body.get('market', 'US'), body.get('horizon', 'day'))
    accuracy_result = None
    if ACCURACY_CHECK_ON_SCAN:
        accuracy_result = run_accuracy_check(market=market, horizon=horizon, triggered_by='user-scan')
    cached = get_cached_scan(market, horizon)
    if cached:
        has_current_lock = payload_matches_current_lock(cached, horizon)
        if cached.get('cache_status') == 'running' and not has_current_lock:
            return jsonify({
                'error': 'A first scan is already running for this market/horizon.',
                'market': market,
                'horizon': horizon,
                'can_start_first_scan': False,
                'scan_running': True,
                'status': get_scan_cache_status(market, horizon),
                'accuracy_check': accuracy_result,
            }), 503
        cached['accuracy_check'] = accuracy_result
        cached['can_start_first_scan'] = not has_current_lock and cached.get('cache_status') != 'running'
        cached['first_scan_label'] = "Run Today's First Scan" if horizon == 'day' else "Run This Week's First Scan"
        return jsonify(cached)

    status = get_scan_cache_status(market, horizon)
    is_running = status.get('status') == 'running' and not status.get('running_is_stale')
    return jsonify({
        'error': 'No locked prediction is available yet for this market/horizon.',
        'market': market,
        'horizon': horizon,
        'can_start_first_scan': not is_running,
        'scan_running': is_running,
        'first_scan_label': 'Run First Scan' if status.get('status') == 'empty' else ("Run Today's First Scan" if horizon == 'day' else "Run This Week's First Scan"),
        'expected_background_job': 'Click Run First Scan in the app, or use: python app.py background-scan',
        'status': status,
        'accuracy_check': accuracy_result,
    }), 503


@app.route('/api/start-first-scan', methods=['POST'])
def start_first_scan():
    # User-facing safe initializer. It can create the first/current-period locked
    # prediction, but it refuses to start if a current lock already exists or if
    # another scan is already running for the same market/horizon.
    body = request.get_json(silent=True) or {}
    market, horizon = validate_market_horizon(body.get('market', 'US'), body.get('horizon', 'day'))

    if ACCURACY_CHECK_ON_SCAN:
        run_accuracy_check(market=market, horizon=horizon, triggered_by='first-scan-request')

    locked = get_locked_cached_scan(market, horizon)
    if locked:
        locked['status'] = 'complete'
        locked['already_locked'] = True
        return jsonify({'ok': True, 'status': 'complete', 'message': 'A locked prediction already exists for this period.', 'payload': locked})

    can_start, reason, status = mark_first_scan_running(market, horizon)
    if not can_start:
        if reason == 'already_locked':
            cached = get_cached_scan(market, horizon)
            return jsonify({'ok': True, 'status': 'complete', 'message': 'A locked prediction already exists for this period.', 'payload': cached})
        if reason == 'already_running':
            return jsonify({'ok': True, 'status': 'running', 'message': 'A scan is already running for this market/horizon.', 'status_detail': status})
        return jsonify({'ok': False, 'status': 'error', 'error': status.get('error') or reason}), 503

    cost_estimate = estimate_next_scan_cost(market, horizon)
    start_first_scan_thread(market, horizon)
    budget = cost_estimate.get('ai_budget') or get_ai_budget_status()
    ai_message = 'Claude ranking will run only if the AI budget guard allows it; otherwise deterministic ranking will be used with ₹0 paid AI cost.'
    return jsonify({
        'ok': True,
        'status': 'running',
        'message': 'First scan started. The app will save and lock the prediction when the background analysis completes. ' + ai_message,
        'market': market,
        'horizon': horizon,
        'status_detail': status,
        'ai_budget': budget,
        'cost_estimate': cost_estimate,
        'execution_policy': get_execution_policy(),
    }), 202


@app.route('/api/scan-status', methods=['POST'])
def scan_status():
    body = request.get_json(silent=True) or {}
    market, horizon = validate_market_horizon(body.get('market', 'US'), body.get('horizon', 'day'))
    cached = get_cached_scan(market, horizon)
    if cached and payload_matches_current_lock(cached, horizon):
        return jsonify({'ok': True, 'status': 'complete', 'payload': cached})
    status = get_scan_cache_status(market, horizon)
    return jsonify({'ok': True, 'status': status.get('status', 'empty'), 'status_detail': status})


@app.route('/api/run-background-scan', methods=['POST'])
def run_background_scan_endpoint():
    secret = os.environ.get('BACKGROUND_SCAN_SECRET')
    if secret and request.headers.get('X-Scan-Secret') != secret:
        return jsonify({'error': 'Unauthorized'}), 401
    body = request.get_json(silent=True) or {}
    market = body.get('market', 'US')
    horizon = body.get('horizon', 'day')
    force = bool(body.get('force', False))
    try:
        payload = run_market_scan(market, horizon, force=force)
        return jsonify({'ok': True, 'summary': payload.get('funnel_counts'), 'market': market, 'horizon': horizon, 'locked': payload.get('locked'), 'lock_reused': payload.get('lock_reused', False)})
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'ok': False, 'error': str(e)}), 500


@app.route('/api/trading', methods=['GET'])
def trading_dashboard():
    try:
        market, horizon = validate_market_horizon(request.args.get('market', 'US'), request.args.get('horizon', 'day'))
        return jsonify(get_trading_dashboard(market, horizon))
    except Exception as e:
        print(f"[ERROR] Trading dashboard failed: {e}")
        return jsonify({
            'ok': False,
            'error': str(e)[:300],
            'message': 'Trading Automation dashboard failed to load, but the app is still running. Check logs for details.',
            'settings': get_trading_settings(),
            'orders': [],
            'positions': [],
            'recent_auto_runs': [],
            'entry_checks': [],
            'paper_auto_state': get_paper_auto_state(),
        }), 200


@app.route('/api/trading/settings', methods=['POST'])
def trading_settings_endpoint():
    body = request.get_json(silent=True) or {}
    settings = update_trading_settings(body)
    return jsonify({
        'ok': True,
        'settings': settings,
        'live_trading_enabled': LIVE_TRADING_ENABLED,
        'broker_configured': bool(BROKER_ORDER_WEBHOOK_URL),
        'notice': 'Live mode is downgraded to assisted unless LIVE_TRADING_ENABLED=true and a compliant broker webhook/API is configured.'
    })


@app.route('/api/trading/run', methods=['POST'])
def trading_run_endpoint():
    body = request.get_json(silent=True) or {}
    try:
        market, horizon = validate_market_horizon(body.get('market', 'US'), body.get('horizon', 'day'))
        result = run_trading_automation(market, horizon)
        # Keep HTTP 200 so the frontend does not look "crashed" when automation
        # simply finds no candidates or self-scan fails. The JSON still carries ok/error.
        return jsonify(result), 200
    except Exception as e:
        print(f"[ERROR] Trading automation run crashed: {e}")
        market = body.get('market', 'US')
        horizon = body.get('horizon', 'day')
        return jsonify({
            'ok': False,
            'error': str(e)[:300],
            'message': 'Trading Automation run failed safely. No order was placed. Check server logs for the traceback.',
            'market': market,
            'horizon': horizon,
            'orders': [],
            'entry_checks': [],
            'locked_scan_available': False,
            'settings': get_trading_settings(),
            'data_source': get_market_data_source_info(str(market), 'trading_automation_check'),
        }), 200


@app.route('/api/paper-auto', methods=['GET'])
def paper_auto_status_endpoint():
    return jsonify({'ok': True, 'auto_state': get_paper_auto_state(), 'recent_runs': get_recent_paper_auto_runs(20)})


@app.route('/api/paper-auto/run', methods=['POST'])
def paper_auto_run_endpoint():
    result = run_paper_auto_cycle(trigger='manual-paper-auto')
    return jsonify({'ok': True, 'result': result, 'auto_state': get_paper_auto_state()})


@app.route('/api/paper-auto/start', methods=['POST'])
def paper_auto_start_endpoint():
    result = start_paper_auto_worker()
    status = 400 if not result.get('ok') else 200
    return jsonify(result), status


@app.route('/api/paper-auto/stop', methods=['POST'])
def paper_auto_stop_endpoint():
    result = stop_paper_auto_worker()
    return jsonify(result)


@app.route('/api/intraday', methods=['GET'])
def intraday_dashboard_endpoint():
    result = get_intraday_dashboard()
    return jsonify(result)


@app.route('/api/intraday/settings', methods=['POST'])
def intraday_settings_endpoint():
    body = request.get_json(silent=True) or {}
    settings = save_intraday_settings(body)
    return jsonify({
        'ok': True,
        'settings': settings,
        'notice': 'Intraday settings saved. Claude can review/control paper intraday entries only when enabled; hard cost/risk gates still run before any order.'
    })


@app.route('/api/intraday/run', methods=['POST'])
def intraday_run_endpoint():
    body = request.get_json(silent=True) or {}
    trigger = str(body.get('trigger') or 'manual')[:40]
    try:
        result = run_intraday_engine(trigger=trigger)
        status = 500 if result.get('error') else 200
        return jsonify(result), status
    except Exception as e:
        print(f"[ERROR] Intraday run crashed: {e}")
        settings = get_intraday_settings()
        trading_settings = get_intraday_effective_trading_settings(get_trading_settings(), settings)
        market = settings.get('market') or 'IN'
        session = get_intraday_session_state(settings)
        started_at = utc_now_naive()
        result = finalize_intraday_result({
            'ok': False,
            'error': str(e)[:300],
            'trigger': trigger,
            'settings': settings,
            'trading_settings': trading_settings,
            'session': session,
            'candidates': [],
            'watch': [],
            'orders': [],
            'errors': [str(e)[:300]],
            'data_source': get_market_data_source_info(market, 'intraday_engine_check'),
            'message': 'Intraday run failed safely. No order was placed. Check the error shown in the latest cycle.',
        }, started_at)
        return jsonify(result), 200


@app.route('/api/intraday/auto/start', methods=['POST'])
def intraday_auto_start_endpoint():
    result = start_intraday_auto_worker()
    status = 400 if not result.get('ok') else 200
    return jsonify(result), status


@app.route('/api/intraday/auto/stop', methods=['POST'])
def intraday_auto_stop_endpoint():
    result = stop_intraday_auto_worker()
    return jsonify(result)


@app.route('/api/trade-history', methods=['GET'])
def trade_history_endpoint():
    source = request.args.get('source', 'all')
    market_filter = request.args.get('market')
    mode_filter = request.args.get('mode')
    try:
        limit = int(request.args.get('limit', 100))
    except Exception:
        limit = 100
    result = get_trade_history_dashboard(source=source, market=market_filter, mode=mode_filter, limit=limit)
    status = 500 if result.get('error') else 200
    return jsonify(result), status


@app.route('/api/check-accuracy', methods=['GET', 'POST'])
def check_accuracy():
    body = request.get_json(silent=True) or {}
    market = body.get('market') or request.args.get('market')
    horizon = body.get('horizon') or request.args.get('horizon')
    result = run_accuracy_check(market=market, horizon=horizon, triggered_by='manual-endpoint')
    status = 500 if result.get('error') else 200
    return jsonify(result), status


@app.route('/api/accuracy', methods=['GET'])
def accuracy():
    conn = get_db_connection()
    if conn is None:
        return jsonify({'error': 'Prediction tracking disabled - DATABASE_URL not set.'}), 500

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE checked) AS total_checked,
                    COUNT(*) FILTER (WHERE checked AND outcome_correct IS TRUE) AS total_correct,
                    COUNT(*) FILTER (WHERE checked AND outcome_correct IS FALSE) AS total_wrong,
                    COUNT(*) FILTER (WHERE checked AND outcome_status='neutral') AS total_neutral,
                    COUNT(*) FILTER (WHERE NOT checked) AS pending,
                    COUNT(*) FILTER (WHERE checked AND signal='BUY') AS buy_checked,
                    COUNT(*) FILTER (WHERE checked AND signal='BUY' AND outcome_correct IS TRUE) AS buy_correct,
                    AVG(actual_gain_pct) FILTER (WHERE checked) AS avg_actual_gain,
                    AVG(predicted_gain_pct) FILTER (WHERE checked) AS avg_predicted_gain,
                    AVG(ABS(actual_gain_pct - predicted_gain_pct)) FILTER (WHERE checked) AS avg_abs_error
                FROM predictions
            """)
            overall = cur.fetchone()

            cur.execute("""
                SELECT market, horizon,
                    COUNT(*) FILTER (WHERE checked) AS total_checked,
                    COUNT(*) FILTER (WHERE checked AND outcome_correct IS TRUE) AS total_correct,
                    COUNT(*) FILTER (WHERE checked AND outcome_correct IS FALSE) AS total_wrong,
                    COUNT(*) FILTER (WHERE checked AND outcome_status='neutral') AS total_neutral,
                    COUNT(*) FILTER (WHERE NOT checked) AS pending
                FROM predictions
                GROUP BY market, horizon
                ORDER BY market, horizon
            """)
            breakdown = cur.fetchall()

            cur.execute("""
                SELECT ticker, company, market, horizon, signal, entry_price,
                       predicted_gain_pct, target_price, stop_price, actual_price, actual_high, actual_low,
                       actual_gain_pct, outcome_correct, outcome_label, outcome_status, failure_reason,
                       lesson_summary, check_date, scan_timestamp, checked_at
                FROM predictions
                WHERE checked = TRUE
                ORDER BY checked_at DESC
                LIMIT 50
            """)
            recent = cur.fetchall()

            cur.execute("""
                SELECT market, horizon, ticker, signal, check_date, scan_timestamp
                FROM predictions
                WHERE checked = FALSE
                ORDER BY check_date ASC
                LIMIT 20
            """)
            pending_rows = cur.fetchall()
    finally:
        conn.close()

    total_checked = overall['total_checked'] or 0
    total_correct = overall['total_correct'] or 0
    total_scored_binary = (overall['total_correct'] or 0) + (overall['total_wrong'] or 0)
    buy_checked = overall['buy_checked'] or 0
    buy_correct = overall['buy_correct'] or 0
    win_rate = round((total_correct / total_scored_binary) * 100, 1) if total_scored_binary else None
    buy_win_rate = round((buy_correct / buy_checked) * 100, 1) if buy_checked else None

    return jsonify({
        'overall': {
            'total_checked': total_checked,
            'total_correct': total_correct,
            'total_wrong': overall['total_wrong'] or 0,
            'total_neutral': overall['total_neutral'] or 0,
            'pending': overall['pending'] or 0,
            'win_rate_pct': win_rate,
            'buy_checked': buy_checked,
            'buy_correct': buy_correct,
            'buy_win_rate_pct': buy_win_rate,
            'avg_actual_gain_pct': round(float(overall['avg_actual_gain']), 2) if overall['avg_actual_gain'] is not None else None,
            'avg_predicted_gain_pct': round(float(overall['avg_predicted_gain']), 2) if overall['avg_predicted_gain'] is not None else None,
            'avg_abs_error_pct': round(float(overall['avg_abs_error']), 2) if overall['avg_abs_error'] is not None else None,
        },
        'breakdown': [{
            'market': b['market'], 'horizon': b['horizon'],
            'total_checked': b['total_checked'], 'total_correct': b['total_correct'],
            'total_wrong': b['total_wrong'], 'total_neutral': b['total_neutral'], 'pending': b['pending'],
            'win_rate_pct': round((b['total_correct'] / (b['total_correct'] + b['total_wrong'])) * 100, 1) if (b['total_correct'] + b['total_wrong']) else None
        } for b in breakdown],
        'recent': [{
            'ticker': r['ticker'], 'company': r['company'], 'market': r['market'], 'horizon': r['horizon'],
            'signal': 'WATCH' if r['signal'] == 'HOLD' else r['signal'],
            'entry_price': float(r['entry_price']) if r['entry_price'] is not None else None,
            'target_price': float(r['target_price']) if r['target_price'] is not None else None,
            'stop_price': float(r['stop_price']) if r['stop_price'] is not None else None,
            'actual_price': float(r['actual_price']) if r['actual_price'] is not None else None,
            'actual_high': float(r['actual_high']) if r['actual_high'] is not None else None,
            'actual_low': float(r['actual_low']) if r['actual_low'] is not None else None,
            'predicted_gain_pct': float(r['predicted_gain_pct']) if r['predicted_gain_pct'] is not None else None,
            'actual_gain_pct': float(r['actual_gain_pct']) if r['actual_gain_pct'] is not None else None,
            'outcome_correct': r['outcome_correct'],
            'outcome_label': r['outcome_label'] or ('Correct' if r['outcome_correct'] else 'Wrong'),
            'outcome_status': r['outcome_status'],
            'failure_reason': r['failure_reason'],
            'lesson_summary': r['lesson_summary'],
            'check_date': r['check_date'].isoformat() if r['check_date'] else None,
            'checked_at': r['checked_at'].isoformat() if r['checked_at'] else None,
            'currency_symbol': get_currency_info(r['market'], r['ticker'])['symbol'],
        } for r in recent],
        'pending': [{
            'ticker': r['ticker'], 'market': r['market'], 'horizon': r['horizon'],
            'signal': 'WATCH' if r['signal'] == 'HOLD' else r['signal'],
            'check_date': r['check_date'].isoformat() if r['check_date'] else None,
            'scan_timestamp': r['scan_timestamp'].isoformat() if r['scan_timestamp'] else None,
        } for r in pending_rows]
    })


@app.route('/api/research', methods=['GET'])
def research_dashboard_endpoint():
    result = get_research_dashboard()
    status = 500 if result.get('error') else 200
    return jsonify(json.loads(json.dumps(result, default=_json_safe))), status


@app.route('/api/backtest/run/<int:run_id>', methods=['GET'])
def backtest_run_detail_endpoint(run_id: int):
    payload = load_backtest_run_payload(run_id)
    status = 404 if payload.get('error') == 'Backtest run not found.' else (500 if payload.get('error') else 200)
    return jsonify(json.loads(json.dumps(payload, default=_json_safe))), status


@app.route('/api/backtest/run', methods=['POST'])
def backtest_run_endpoint():
    body = request.get_json(silent=True) or {}
    market = body.get('market') or request.args.get('market') or 'IN'
    horizon = body.get('horizon') or request.args.get('horizon') or 'day'
    market, horizon = validate_market_horizon(market, horizon)
    try:
        requested_universe_limit = int(body.get('universe_limit') or request.args.get('universe_limit') or BACKTEST_UNIVERSE_LIMIT)
        safe_universe_limit = max(1, min(requested_universe_limit, BACKTEST_WEB_MAX_UNIVERSE_LIMIT))
        result = run_historical_backtest(
            market=market,
            horizon=horizon,
            start_date=body.get('start_date') or request.args.get('start_date'),
            end_date=body.get('end_date') or request.args.get('end_date'),
            universe_limit=safe_universe_limit,
            top_n=int(body.get('top_n') or request.args.get('top_n') or BACKTEST_TOP_N),
            rebalance_step_days=int(body.get('rebalance_step_days') or request.args.get('rebalance_step_days') or BACKTEST_REBALANCE_STEP_DAYS),
            learning_mode=str(body.get('learning_mode') or request.args.get('learning_mode') or 'raw'),
            # Must be explicitly opted into with commit_learning=true. Default is False so
            # ad-hoc/manual/research runs from the dashboard never silently mutate the shared
            # pattern-memory table that live 'learned' scans and future backtests read from.
            commit_learning=bool(body.get('commit_learning', request.args.get('commit_learning', False))),
        )
        if requested_universe_limit != safe_universe_limit:
            result.setdefault('warnings', []).append(f'Web backtest universe was capped at {safe_universe_limit} to avoid Render worker timeout. Use CLI or raise BACKTEST_WEB_MAX_UNIVERSE_LIMIT for heavier runs.')
        status = 500 if result.get('error') else 200
        return jsonify(json.loads(json.dumps(result, default=_json_safe))), status
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'ok': False, 'error': str(e)}), 500


@app.route('/api/backtest/compare-learning', methods=['POST'])
def backtest_compare_learning_endpoint():
    """Run the SAME window/settings twice -- once with learning ignored ('raw'),
    once with all accumulated pattern-memory/stock-history learning applied
    ('learned') -- and return both results side by side with an honest verdict.

    This directly answers 'did the system actually get better after learning,
    on the same test, or not' instead of relying on a single ambiguous number.
    """
    body = request.get_json(silent=True) or {}
    market = body.get('market') or 'IN'
    horizon = body.get('horizon') or 'day'
    market, horizon = validate_market_horizon(market, horizon)
    try:
        requested_universe_limit = int(body.get('universe_limit') or BACKTEST_UNIVERSE_LIMIT)
        safe_universe_limit = max(1, min(requested_universe_limit, BACKTEST_WEB_MAX_UNIVERSE_LIMIT))
        common = dict(
            market=market, horizon=horizon,
            start_date=body.get('start_date'), end_date=body.get('end_date'),
            universe_limit=safe_universe_limit,
            top_n=int(body.get('top_n') or BACKTEST_TOP_N),
            rebalance_step_days=int(body.get('rebalance_step_days') or BACKTEST_REBALANCE_STEP_DAYS),
        )
        # Both legs run with commit_learning=False. This is a diagnostic A/B, not a
        # training step: the raw leg must not write evidence that the learned leg
        # (running microseconds later) then reads back, and neither leg should move
        # the shared memory that other in-flight comparisons depend on. Review the
        # verdict first; only call /api/backtest/run with commit_learning=true once
        # you've decided a specific config's result should count as real learning.
        raw_result = run_historical_backtest(**common, learning_mode='raw', commit_learning=False)
        learned_result = run_historical_backtest(**common, learning_mode='learned', commit_learning=False)

        def _m(res, key):
            return (res or {}).get(key)

        pf_before = _m(raw_result, 'profit_factor')
        pf_after = _m(learned_result, 'profit_factor')
        crossed_profitable = (pf_before is not None and pf_after is not None and pf_before < 1.0 <= (pf_after or 0))
        got_worse = (pf_before is not None and pf_after is not None and pf_after < pf_before)

        if pf_after is None or pf_before is None:
            verdict = 'incomplete_data'
        elif crossed_profitable:
            verdict = 'learning_helped_and_crossed_breakeven'
        elif pf_after > pf_before:
            verdict = 'learning_helped_but_still_net_negative' if pf_after < 1.0 else 'learning_helped_and_stayed_profitable'
        elif got_worse:
            verdict = 'learning_made_it_worse_on_this_window'
        else:
            verdict = 'no_meaningful_change'

        comparison = {
            'ok': True,
            'market': market, 'horizon': horizon,
            'raw': {
                'total_trades': _m(raw_result, 'total_trades'),
                'win_rate_pct': _m(raw_result, 'win_rate_pct'),
                'expectancy_pct': _m(raw_result, 'expectancy_pct'),
                'sharpe': _m(raw_result, 'sharpe'),
                'sortino': _m(raw_result, 'sortino'),
                'profit_factor': pf_before,
                'max_drawdown_pct': _m(raw_result, 'max_drawdown_pct'),
                'run_id': _m(raw_result, 'run_id'),
            },
            'learned': {
                'total_trades': _m(learned_result, 'total_trades'),
                'win_rate_pct': _m(learned_result, 'win_rate_pct'),
                'expectancy_pct': _m(learned_result, 'expectancy_pct'),
                'sharpe': _m(learned_result, 'sharpe'),
                'sortino': _m(learned_result, 'sortino'),
                'profit_factor': pf_after,
                'max_drawdown_pct': _m(learned_result, 'max_drawdown_pct'),
                'run_id': _m(learned_result, 'run_id'),
            },
            'verdict': verdict,
            'plain_summary': (
                'Raw = the base strategy with no learning applied, on this exact window. '
                'Learned = the same exact window, but candidates are re-scored and filtered using every '
                'pattern-memory and per-stock lesson learned from past backtests so far. '
                'A rising profit factor here means learning is genuinely helping; a profit factor at or above '
                '1.0 means the (simulated) strategy would have been net profitable after realistic costs. '
                'No real trading strategy should be expected to reach a 100% win rate -- a smaller, consistent, '
                'positive expectancy across many trades is the realistic goal, not a perfect record.'
            ),
        }
        return jsonify(json.loads(json.dumps(comparison, default=_json_safe))), 200
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'ok': False, 'error': str(e)}), 500


# Full data export: every application table plus computed summaries.
EXPORT_TABLES = [
    'predictions',
    'stock_scan_cache',
    'stock_scan_runs',
    'prediction_lessons',
    'api_cost_log',
    'trading_settings',
    'intraday_settings',
    'trade_orders',
    'trade_positions',
    'intraday_run_log',
    'backtest_runs',
    'backtest_trades',
    'strategy_signal_stats',
]


def _dict_rows(rows):
    return [dict(r) for r in (rows or [])]


def _safe_export_json(obj):
    return json.dumps(obj, default=_json_safe, ensure_ascii=False, indent=2)


def _table_exists(cur, table_name: str) -> bool:
    cur.execute("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema='public' AND table_name=%s
        ) AS exists
    """, (table_name,))
    row = cur.fetchone()
    return bool(row['exists'] if isinstance(row, dict) else row[0])


def _table_columns(cur, table_name: str) -> List[str]:
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name=%s
        ORDER BY ordinal_position
    """, (table_name,))
    return [r['column_name'] for r in cur.fetchall()]


def _table_count(cur, table_name: str) -> int:
    if not _table_exists(cur, table_name):
        return 0
    cur.execute(f'SELECT COUNT(*) AS n FROM {table_name}')
    row = cur.fetchone()
    return int(row['n'] or 0)


def _select_order_clause(columns: List[str]) -> str:
    if 'id' in columns:
        return ' ORDER BY id ASC'
    if 'created_at' in columns:
        return ' ORDER BY created_at ASC'
    if 'scan_timestamp' in columns:
        return ' ORDER BY scan_timestamp ASC'
    if 'updated_at' in columns:
        return ' ORDER BY updated_at ASC'
    return ''


def _fetch_export_table(cur, table_name: str) -> Tuple[List[str], List[Dict[str, Any]]]:
    if not _table_exists(cur, table_name):
        return [], []
    columns = _table_columns(cur, table_name)
    cur.execute(f'SELECT * FROM {table_name}{_select_order_clause(columns)}')
    return columns, _dict_rows(cur.fetchall())


def _rows_to_csv_text(columns: List[str], rows: List[Dict[str, Any]]) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=columns, extrasaction='ignore')
    writer.writeheader()
    for row in rows:
        clean = {}
        for col in columns:
            val = row.get(col)
            if isinstance(val, (dict, list)):
                clean[col] = json.dumps(val, default=_json_safe, ensure_ascii=False)
            elif isinstance(val, (datetime.datetime, datetime.date)):
                clean[col] = val.isoformat()
            elif isinstance(val, Decimal):
                clean[col] = str(val)
            else:
                clean[col] = val
        writer.writerow(clean)
    return buf.getvalue()


def _safe_export_environment() -> Dict[str, Any]:
    # Only non-secret operational config is exported. API keys, database URLs,
    # webhook secrets and tokens are intentionally excluded.
    keys = [
        'MARKET_DATA_PROVIDER', 'GROWW_MARKET_DATA_ENABLED',
        'MARKET_SCAN_MARKETS', 'MARKET_SCAN_HORIZONS', 'SCAN_BATCH_SIZE',
        'MOMENTUM_STAGE_LIMIT', 'TECHNICAL_STAGE_LIMIT', 'DEEP_ANALYSIS_LIMIT',
        'FINAL_PICK_LIMIT', 'SCAN_CACHE_TTL_MINUTES', 'PREDICTION_LOCK_ENABLED',
        'ACCURACY_CHECK_ON_SCAN', 'MIN_BUY_CONFIDENCE', 'MIN_BUY_RISK_REWARD',
        'VERIFY_AFTER_CLOSE_DELAY_MINUTES', 'AI_MAX_PAID_CALLS_PER_DAY',
        'AI_MAX_PAID_CALLS_PER_WEEK', 'AI_MAX_COST_INR_PER_DAY',
        'AI_MAX_COST_INR_PER_WEEK', 'LEARNING_AI_ENABLED', 'TRADING_ENABLED',
        'AUTO_TRADING_MODE', 'LIVE_TRADING_ENABLED', 'BROKER_NAME',
        'TRADING_DEFAULT_CAPITAL_INR', 'TRADING_WEEKLY_PROFIT_TARGET_PCT',
        'TRADING_MAX_WEEKLY_LOSS_PCT', 'OPENING_CONFIRMATION_ENABLED',
        'OPENING_CONFIRMATION_WAIT_MINUTES', 'PROFIT_PROTECTION_ENABLED', 'TRADING_STOP_AFTER_WEEKLY_TARGET',
        'INTRADAY_ENGINE_ENABLED', 'INTRADAY_DEFAULT_MARKET',
        'INTRADAY_DEFAULT_MODE', 'INTRADAY_AUTO_ENABLED',
        'INTRADAY_AUTO_INTERVAL_SECONDS', 'INTRADAY_OPENING_WAIT_MINUTES',
        'INTRADAY_MAX_TRADES_PER_DAY', 'BACKTEST_DEFAULT_LOOKBACK_YEARS',
        'BACKTEST_UNIVERSE_LIMIT', 'BACKTEST_TOP_N', 'BACKTEST_REBALANCE_STEP_DAYS',
        'STRICT_LEARNING_ENABLED', 'STRICT_MIN_SCORE', 'STRICT_BUY_MIN_SCORE', 'STRICT_BACKTEST_MIN_SCORE',
        'STRICT_MIN_RISK_REWARD', 'STRICT_MIN_VOLUME_RATIO', 'STRICT_BACKTEST_MIN_RISK_REWARD',
        'STRICT_BACKTEST_MIN_VOLUME_RATIO', 'STRICT_BACKTEST_ALLOW_WATCH_ONLY', 'STRICT_BACKTEST_REJECT_RISK_OFF',
        'STRICT_AVOID_RISK_OFF',
        'STRICT_PATTERN_RELIABLE_MIN_TRADES', 'STRICT_PER_STOCK_MIN_TRADES',
        'STRICT_TRADE_HISTORY_MIN_TRADES', 'STRICT_INTRADAY_HISTORY_MIN_TRADES',
        'STRICT_NO_TRADE_IF_NO_BUY',
        'POST_ENTRY_RISK_RECHECK_ENABLED', 'POST_ENTRY_TIGHTEN_SCORE_THRESHOLD',
        'POST_ENTRY_EXIT_SCORE_THRESHOLD', 'POST_ENTRY_PROFIT_EXIT_SCORE_THRESHOLD',
        'POST_ENTRY_MIN_WEAK_SIGNALS', 'POST_ENTRY_TIGHTEN_BUFFER_PCT',
        'POST_ENTRY_LOSS_EXIT_MIN_PNL_PCT', 'POST_ENTRY_ALLOW_TIGHTEN_STOP',
        'QUALITY_STOCK_FILTER_ENABLED', 'QUALITY_MIN_PRICE', 'QUALITY_MAX_ZERO_VOLUME_DAYS',
        'QUALITY_MIN_RECENT_BARS', 'QUALITY_MIN_AVG_VOLUME_MULTIPLIER', 'QUALITY_MIN_TURNOVER_MULTIPLIER',
        'BACKTEST_DATA_INTEGRITY_ENABLED', 'BACKTEST_PRICE_MODE', 'BACKTEST_EXIT_AWARE_ENABLED', 'BACKTEST_RETURN_MODE', 'BACKTEST_MAX_SUSPICIOUS_CANDLES',
        'BACKTEST_MAX_DAILY_RANGE_PCT', 'BACKTEST_MAX_CLOSE_GAP_PCT', 'BACKTEST_MIN_DATA_QUALITY_SCORE',
        'BACKTEST_BLOCK_LEARNING_ON_DATA_WARNING', 'BACKTEST_DUPLICATE_PROTECTION', 'BACKTEST_ALLOW_DUPLICATE_LEARNING',
        'BACKTEST_FINGERPRINT_VERSION',
        'MACRO_RISK_ENABLED', 'MACRO_RISK_CAUTION_THRESHOLD', 'MACRO_RISK_BLOCK_THRESHOLD',
        'MACRO_RISK_MANUAL_LEVEL', 'MACRO_RISK_MANUAL_NOTE',
        'EXECUTION_SLIPPAGE_BPS', 'EXECUTION_SPREAD_BPS', 'EXECUTION_FEE_BPS',
    ]
    return {k: os.environ.get(k) for k in keys if os.environ.get(k) is not None}


def _fetch_export_summaries(cur) -> Dict[str, Any]:
    summaries: Dict[str, Any] = {
        'generated_at_utc': datetime.datetime.utcnow().isoformat() + 'Z',
        'note': 'Full application export. Secrets are excluded; database data is included.',
        'export_health': {'schema_safe': True, 'warnings': []},
    }

    def warn(section: str, error: Any) -> None:
        msg = f'{section}: {str(error)[:220]}'
        summaries['export_health'].setdefault('warnings', []).append(msg)
        summaries[f'{section}_error'] = msg

    def cols(table: str) -> List[str]:
        return _table_columns(cur, table) if _table_exists(cur, table) else []

    def has(table: str, *names: str) -> bool:
        c = set(cols(table))
        return all(n in c for n in names)

    # Prediction accuracy summaries.
    try:
        if _table_exists(cur, 'predictions'):
            cur.execute("""
                SELECT
                    COUNT(*) AS total_predictions,
                    COUNT(*) FILTER (WHERE checked = TRUE) AS checked_predictions,
                    COUNT(*) FILTER (WHERE checked = FALSE) AS pending_predictions,
                    COUNT(*) FILTER (WHERE outcome_correct = TRUE) AS correct_predictions,
                    COUNT(*) FILTER (WHERE outcome_correct = FALSE) AS wrong_predictions,
                    AVG(actual_gain_pct) FILTER (WHERE checked = TRUE) AS avg_actual_gain_pct,
                    AVG(predicted_gain_pct) FILTER (WHERE checked = TRUE) AS avg_predicted_gain_pct
                FROM predictions
            """)
            pred = dict(cur.fetchone())
            checked_for_rate = int(pred.get('correct_predictions') or 0) + int(pred.get('wrong_predictions') or 0)
            pred['win_rate_pct'] = round((float(pred.get('correct_predictions') or 0) / checked_for_rate) * 100, 2) if checked_for_rate else None
            summaries['prediction_accuracy_overall'] = pred

            cur.execute("""
                SELECT market, horizon, COALESCE(signal,'UNKNOWN') AS signal,
                       COUNT(*) AS total,
                       COUNT(*) FILTER (WHERE checked = TRUE) AS checked,
                       COUNT(*) FILTER (WHERE outcome_correct = TRUE) AS correct,
                       COUNT(*) FILTER (WHERE outcome_correct = FALSE) AS wrong,
                       AVG(actual_gain_pct) FILTER (WHERE checked = TRUE) AS avg_actual_gain_pct
                FROM predictions
                GROUP BY market, horizon, COALESCE(signal,'UNKNOWN')
                ORDER BY market, horizon, signal
            """)
            breakdown = []
            for row in cur.fetchall():
                d = dict(row)
                denom = int(d.get('correct') or 0) + int(d.get('wrong') or 0)
                d['win_rate_pct'] = round((float(d.get('correct') or 0) / denom) * 100, 2) if denom else None
                breakdown.append(d)
            summaries['prediction_accuracy_by_market_horizon_signal'] = breakdown
        else:
            summaries['prediction_accuracy_overall'] = {'total_predictions': 0, 'win_rate_pct': None}
            summaries['prediction_accuracy_by_market_horizon_signal'] = []
    except Exception as e:
        warn('prediction_summary', e)

    # Trade history summaries. Some older DBs do not have an engine column, so infer it.
    try:
        if _table_exists(cur, 'trade_positions'):
            tcols = set(cols('trade_positions'))
            if 'engine' in tcols:
                engine_expr = "COALESCE(engine, 'trading_automation')"
            elif 'metadata_json' in tcols:
                engine_expr = "COALESCE(metadata_json->>'engine', metadata_json->>'source', CASE WHEN horizon='intraday' THEN 'intraday' ELSE 'trading_automation' END)"
            else:
                engine_expr = "CASE WHEN horizon='intraday' THEN 'intraday' ELSE 'trading_automation' END"
            mode_expr = 'mode' if 'mode' in tcols else "'paper'"
            market_expr = 'market' if 'market' in tcols else "'UNKNOWN'"
            pnl_amount = 'pnl_amount' if 'pnl_amount' in tcols else '0'
            pnl_pct = 'pnl_pct' if 'pnl_pct' in tcols else 'NULL::numeric'
            status = 'status' if 'status' in tcols else "'UNKNOWN'"
            cur.execute(f"""
                WITH p AS (
                    SELECT {engine_expr} AS source_engine, {mode_expr} AS mode, {market_expr} AS market,
                           {status} AS status, {pnl_amount} AS pnl_amount, {pnl_pct} AS pnl_pct
                    FROM trade_positions
                )
                SELECT
                  COUNT(*) AS total_positions,
                  COUNT(*) FILTER (WHERE status='OPEN') AS open_positions,
                  COUNT(*) FILTER (WHERE status='CLOSED') AS closed_positions,
                  COALESCE(SUM(pnl_amount) FILTER (WHERE status='CLOSED'),0) AS realised_pnl,
                  COALESCE(SUM(pnl_amount) FILTER (WHERE source_engine='intraday' AND status='CLOSED'),0) AS intraday_realised_pnl,
                  COALESCE(SUM(pnl_amount) FILTER (WHERE source_engine <> 'intraday' AND status='CLOSED'),0) AS trading_realised_pnl,
                  COUNT(*) FILTER (WHERE status='CLOSED' AND pnl_amount > 0) AS winning_positions,
                  COUNT(*) FILTER (WHERE status='CLOSED' AND pnl_amount <= 0) AS losing_positions
                FROM p
            """)
            trade = dict(cur.fetchone())
            closed = int(trade.get('winning_positions') or 0) + int(trade.get('losing_positions') or 0)
            trade['closed_win_rate_pct'] = round((float(trade.get('winning_positions') or 0) / closed) * 100, 2) if closed else None
            trade['engine_column_present'] = 'engine' in tcols
            trade['engine_inference_used'] = 'engine' not in tcols
            summaries['trade_performance_overall'] = trade

            cur.execute(f"""
                WITH p AS (
                    SELECT {engine_expr} AS source_engine, {mode_expr} AS mode, {market_expr} AS market,
                           {status} AS status, {pnl_amount} AS pnl_amount, {pnl_pct} AS pnl_pct
                    FROM trade_positions
                )
                SELECT source_engine AS engine, mode, market,
                       COUNT(*) AS total_positions,
                       COUNT(*) FILTER (WHERE status='CLOSED') AS closed_positions,
                       COALESCE(SUM(pnl_amount) FILTER (WHERE status='CLOSED'),0) AS realised_pnl,
                       AVG(pnl_pct) FILTER (WHERE status='CLOSED') AS avg_pnl_pct,
                       COUNT(*) FILTER (WHERE status='CLOSED' AND pnl_amount > 0) AS wins,
                       COUNT(*) FILTER (WHERE status='CLOSED' AND pnl_amount <= 0) AS losses
                FROM p
                GROUP BY source_engine, mode, market
                ORDER BY source_engine, mode, market
            """)
            trade_breakdown = []
            for row in cur.fetchall():
                d = dict(row)
                denom = int(d.get('wins') or 0) + int(d.get('losses') or 0)
                d['win_rate_pct'] = round((float(d.get('wins') or 0) / denom) * 100, 2) if denom else None
                trade_breakdown.append(d)
            summaries['trade_performance_by_engine_mode_market'] = trade_breakdown
        else:
            summaries['trade_performance_overall'] = {'total_positions': 0, 'closed_win_rate_pct': None}
            summaries['trade_performance_by_engine_mode_market'] = []
    except Exception as e:
        warn('trade_summary', e)

    # API costs.
    try:
        if _table_exists(cur, 'api_cost_log'):
            cur.execute("""
                SELECT COUNT(*) AS paid_ai_calls,
                       COALESCE(SUM(input_tokens),0) AS input_tokens,
                       COALESCE(SUM(output_tokens),0) AS output_tokens,
                       COALESCE(SUM(cost_usd),0) AS cost_usd,
                       COALESCE(SUM(cost_inr),0) AS cost_inr
                FROM api_cost_log
            """)
            summaries['api_costs_overall'] = dict(cur.fetchone())
        else:
            summaries['api_costs_overall'] = {'paid_ai_calls': 0, 'cost_inr': 0}
    except Exception as e:
        warn('api_cost_summary', e)

    # Backtest runs and research lab summaries.
    try:
        if _table_exists(cur, 'backtest_runs'):
            btcols = cols('backtest_runs')
            select_cols = [c for c in [
                'id', 'market', 'horizon', 'created_at', 'start_date', 'end_date', 'total_trades',
                'win_rate_pct', 'expectancy_pct', 'sharpe', 'sortino', 'max_drawdown_pct', 'profit_factor',
                'universe_limit', 'top_n', 'rebalance_step_days', 'fingerprint_hash', 'learning_applied', 'config_json', 'metrics_json'
            ] if c in btcols]
            cur.execute(f"SELECT {', '.join(select_cols)} FROM backtest_runs ORDER BY created_at DESC LIMIT 100")
            summaries['latest_backtest_runs'] = _dict_rows(cur.fetchall())
        else:
            summaries['latest_backtest_runs'] = []
    except Exception as e:
        warn('latest_backtest_runs', e)

    try:
        if _table_exists(cur, 'backtest_trades'):
            cur.execute("""
                SELECT market, horizon, ticker,
                       COUNT(*) AS trades,
                       ROUND(AVG(CASE WHEN net_return_pct > 0 THEN 1 ELSE 0 END) * 100, 2) AS win_rate_pct,
                       AVG(net_return_pct) AS expectancy_pct,
                       AVG(net_return_pct) FILTER (WHERE net_return_pct > 0) AS avg_win_pct,
                       AVG(net_return_pct) FILTER (WHERE net_return_pct <= 0) AS avg_loss_pct,
                       MAX(net_return_pct) AS best_trade_pct,
                       MIN(net_return_pct) AS worst_trade_pct,
                       COUNT(*) FILTER (WHERE outcome='target_hit') AS target_hits,
                       COUNT(*) FILTER (WHERE outcome='stop_loss_hit') AS stop_hits,
                       MAX(signal_date) AS last_signal_date
                FROM backtest_trades
                GROUP BY market, horizon, ticker
                ORDER BY market, horizon, trades DESC, ticker
                LIMIT 1000
            """)
            summaries['per_stock_backtest_accuracy_top1000'] = _dict_rows(cur.fetchall())
        else:
            summaries['per_stock_backtest_accuracy_top1000'] = []
    except Exception as e:
        warn('per_stock_backtest_accuracy', e)

    try:
        if _table_exists(cur, 'strategy_signal_stats'):
            cur.execute("""
                SELECT market, horizon, regime, pattern_key, trades, win_rate_pct,
                       expectancy_pct, confidence_adjustment, notes, updated_at
                FROM strategy_signal_stats
                ORDER BY updated_at DESC, trades DESC
                LIMIT 1000
            """)
            summaries['strategy_memory_top1000'] = _dict_rows(cur.fetchall())
        else:
            summaries['strategy_memory_top1000'] = []
    except Exception as e:
        warn('strategy_memory', e)

    summaries['research_lab_complete_export_note'] = {
        'included': [
            'backtest_runs with config_json/metrics_json',
            'backtest_trades full historical simulated trades',
            'strategy_signal_stats learning rules',
            'prediction_lessons mistake-learning rows',
            'per-stock backtest accuracy summary',
            'duplicate-learning fingerprints in backtest_runs',
            'data-integrity guard and raw/adjusted price mode in metrics/config for new runs',
        ],
        'plain_summary': 'The ZIP export contains both raw tables and computed summaries, so Research Lab findings, tests, learning rules, skipped/quality notes, and correctness rates are preserved.'
    }
    summaries['safe_environment_config'] = _safe_export_environment()
    return summaries

def build_full_export_bundle() -> Dict[str, Any]:
    conn = get_db_connection()
    if conn is None:
        raise RuntimeError('DATABASE_URL is not set. Full export requires PostgreSQL.')
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            manifest = {
                'generated_at_utc': datetime.datetime.utcnow().isoformat() + 'Z',
                'app': 'AI Market Scanner',
                'export_scope': 'all_database_tables_plus_computed_summaries',
                'secret_policy': 'DATABASE_URL, API keys, broker tokens and webhook secrets are not exported.',
                'tables': [],
            }
            tables: Dict[str, Dict[str, Any]] = {}
            for table in EXPORT_TABLES:
                exists = _table_exists(cur, table)
                columns = _table_columns(cur, table) if exists else []
                count = _table_count(cur, table) if exists else 0
                manifest['tables'].append({'table': table, 'exists': exists, 'rows': count, 'columns': columns})
                if exists:
                    cols, rows = _fetch_export_table(cur, table)
                    tables[table] = {'columns': cols, 'rows': rows}
            summaries = _fetch_export_summaries(cur)
            manifest['summary_sections'] = list(summaries.keys())
            return {'manifest': manifest, 'summaries': summaries, 'tables': tables}
    finally:
        conn.close()


@app.route('/api/export/manifest', methods=['GET'])
def export_manifest_endpoint():
    conn = get_db_connection()
    if conn is None:
        return jsonify({'ok': False, 'error': 'DATABASE_URL is not set. Export requires PostgreSQL.'}), 500
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            tables = []
            for table in EXPORT_TABLES:
                exists = _table_exists(cur, table)
                tables.append({
                    'table': table,
                    'exists': exists,
                    'rows': _table_count(cur, table) if exists else 0,
                    'columns': _table_columns(cur, table) if exists else [],
                })
            summaries = _fetch_export_summaries(cur)
            return jsonify(json.loads(_safe_export_json({
                'ok': True,
                'generated_at_utc': datetime.datetime.utcnow().isoformat() + 'Z',
                'tables': tables,
                'summary_preview': {
                    'prediction_accuracy_overall': summaries.get('prediction_accuracy_overall'),
                    'trade_performance_overall': summaries.get('trade_performance_overall'),
                    'api_costs_overall': summaries.get('api_costs_overall'),
                    'latest_backtest_runs_count': len(summaries.get('latest_backtest_runs') or []),
                },
                'formats': ['zip', 'json'],
                'download_zip': '/api/export/all?format=zip',
                'download_json': '/api/export/all?format=json',
            })))
    finally:
        conn.close()


@app.route('/api/export/all', methods=['GET'])
def export_all_endpoint():
    fmt = (request.args.get('format') or 'zip').lower()
    bundle = build_full_export_bundle()
    stamp = datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    if fmt == 'json':
        data = _safe_export_json(bundle)
        return Response(
            data,
            mimetype='application/json',
            headers={'Content-Disposition': f'attachment; filename=marketpredictor_full_export_{stamp}.json'}
        )

    if fmt != 'zip':
        return jsonify({'ok': False, 'error': 'Unsupported format. Use format=zip or format=json.'}), 400

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr('manifest.json', _safe_export_json(bundle['manifest']))
        zf.writestr('summaries.json', _safe_export_json(bundle['summaries']))
        for table, payload in bundle['tables'].items():
            rows = payload.get('rows') or []
            cols = payload.get('columns') or []
            zf.writestr(f'tables/{table}.json', _safe_export_json(rows))
            zf.writestr(f'tables/{table}.csv', _rows_to_csv_text(cols, rows))
    buf.seek(0)
    return Response(
        buf.getvalue(),
        mimetype='application/zip',
        headers={'Content-Disposition': f'attachment; filename=marketpredictor_full_export_{stamp}.zip'}
    )


@app.route('/api/costs', methods=['GET'])
def api_costs():
    market = request.args.get('market', 'US')
    horizon = request.args.get('horizon', 'day')
    market, horizon = validate_market_horizon(market, horizon)
    estimate = estimate_next_scan_cost(market, horizon)
    conn = get_db_connection()
    if conn is None:
        return jsonify({'error': 'Cost tracking disabled - DATABASE_URL not set.', 'estimate': estimate}), 500
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    COUNT(*) AS total_calls,
                    COALESCE(SUM(input_tokens),0) AS input_tokens,
                    COALESCE(SUM(output_tokens),0) AS output_tokens,
                    COALESCE(SUM(cost_usd),0) AS cost_usd,
                    COALESCE(SUM(cost_inr),0) AS cost_inr
                FROM api_cost_log
            """)
            totals = cur.fetchone()
            cur.execute("""
                SELECT
                    DATE(created_at + interval '5 hours 30 minutes') AS cost_date_ist,
                    COUNT(*) AS calls,
                    COALESCE(SUM(input_tokens),0) AS input_tokens,
                    COALESCE(SUM(output_tokens),0) AS output_tokens,
                    COALESCE(SUM(cost_usd),0) AS cost_usd,
                    COALESCE(SUM(cost_inr),0) AS cost_inr,
                    STRING_AGG(DISTINCT call_type, ', ' ORDER BY call_type) AS call_types
                FROM api_cost_log
                GROUP BY DATE(created_at + interval '5 hours 30 minutes')
                ORDER BY cost_date_ist DESC
                LIMIT 30
            """)
            daily = cur.fetchall()
            cur.execute("""
                SELECT
                    call_type,
                    COUNT(*) AS calls,
                    COALESCE(SUM(input_tokens),0) AS input_tokens,
                    COALESCE(SUM(output_tokens),0) AS output_tokens,
                    COALESCE(SUM(cost_usd),0) AS cost_usd,
                    COALESCE(SUM(cost_inr),0) AS cost_inr,
                    MAX(created_at) AS last_call_at
                FROM api_cost_log
                GROUP BY call_type
                ORDER BY cost_inr DESC, calls DESC
                LIMIT 20
            """)
            by_call_type = cur.fetchall()
            cur.execute("""
                SELECT call_type, market, horizon, model, input_tokens, output_tokens, cost_usd, cost_inr, notes, created_at
                FROM api_cost_log
                ORDER BY created_at DESC
                LIMIT 30
            """)
            recent = cur.fetchall()
    finally:
        conn.close()

    budget = get_ai_budget_status((estimate.get('estimated_paid_cost_if_first_scan_runs') or {}).get('cost_inr', 0))
    return jsonify({
        'totals': {
            'total_calls': int(totals['total_calls'] or 0),
            'input_tokens': int(totals['input_tokens'] or 0),
            'output_tokens': int(totals['output_tokens'] or 0),
            'cost_usd': round(float(totals['cost_usd'] or 0), 6),
            'cost_inr': round(float(totals['cost_inr'] or 0), 4),
        },
        'estimate': estimate,
        'pricing': {
            'input_usd_per_million': ANTHROPIC_INPUT_USD_PER_MILLION,
            'output_usd_per_million': ANTHROPIC_OUTPUT_USD_PER_MILLION,
            'usd_inr_rate': USD_INR_RATE,
        },
        'ai_budget': budget,
        'execution_policy': get_execution_policy(),
        'daily': [{
            'date_ist': str(r['cost_date_ist']) if r.get('cost_date_ist') else None,
            'calls': int(r['calls'] or 0),
            'input_tokens': int(r['input_tokens'] or 0),
            'output_tokens': int(r['output_tokens'] or 0),
            'cost_usd': round(float(r['cost_usd'] or 0), 6),
            'cost_inr': round(float(r['cost_inr'] or 0), 4),
            'call_types': r.get('call_types') or '',
        } for r in daily],
        'by_call_type': [{
            'call_type': r['call_type'],
            'calls': int(r['calls'] or 0),
            'input_tokens': int(r['input_tokens'] or 0),
            'output_tokens': int(r['output_tokens'] or 0),
            'cost_usd': round(float(r['cost_usd'] or 0), 6),
            'cost_inr': round(float(r['cost_inr'] or 0), 4),
            'last_call_at': r['last_call_at'].isoformat() if r.get('last_call_at') else None,
        } for r in by_call_type],
        'recent': [{
            'call_type': r['call_type'], 'market': r['market'], 'horizon': r['horizon'], 'model': r['model'],
            'input_tokens': r['input_tokens'], 'output_tokens': r['output_tokens'],
            'cost_usd': float(r['cost_usd']), 'cost_inr': float(r['cost_inr']),
            'notes': r['notes'], 'created_at': r['created_at'].isoformat() if r['created_at'] else None,
        } for r in recent]
    })


def run_background_scan_cli(args):
    markets = os.environ.get('MARKET_SCAN_MARKETS', 'US,IN,Global').split(',')
    horizons = os.environ.get('MARKET_SCAN_HORIZONS', 'day,week').split(',')
    if args.markets:
        markets = args.markets
    if args.horizons:
        horizons = args.horizons

    failures = []
    for market in [m.strip() for m in markets if m.strip()]:
        for horizon in [h.strip() for h in horizons if h.strip()]:
            started = datetime.datetime.now()
            try:
                run_market_scan(market, horizon, force=getattr(args, 'force', False))
            except Exception as e:
                import traceback
                traceback.print_exc()
                counts = {'total_universe': len(load_market_universe(market))}
                insert_scan_run(market, horizon, 'failed', started, datetime.datetime.now(), counts, str(e))
                failures.append(f"{market}/{horizon}: {e}")
    if failures:
        print('[SCAN] Failures:', failures)
        return 1
    return 0



SERVER_AUTOSTART_DONE = False


def start_server_background_workers_once() -> None:
    """Start configured in-process workers when the app is imported by Gunicorn.

    Gunicorn imports app:app and does not call main(), so relying only on the
    __main__ block means PAPER_AUTO_ON_START / INTRADAY_SERVER_AUTO_ON_START do
    nothing in production. Keep Render on one worker when using this in-process
    scheduler so state and controls do not split across processes.
    """
    global SERVER_AUTOSTART_DONE
    if SERVER_AUTOSTART_DONE:
        return
    SERVER_AUTOSTART_DONE = True
    if INTRADAY_SERVER_AUTO_ON_START:
        try:
            settings = get_intraday_settings()
            if settings.get('enabled') and settings.get('auto_enabled'):
                start_intraday_auto_worker()
        except Exception as e:
            print(f"[WARN] Could not start intraday auto worker on boot: {e}")
    try:
        if PAPER_AUTO_ENABLED and PAPER_AUTO_ON_START:
            start_paper_auto_worker()
    except Exception as e:
        print(f"[WARN] Could not start paper auto scheduler on boot: {e}")


start_server_background_workers_once()

def main():
    parser = argparse.ArgumentParser(description='AI Market Scanner')
    sub = parser.add_subparsers(dest='command')
    scan_parser = sub.add_parser('background-scan')
    scan_parser.add_argument('--markets', nargs='*', help='Markets to scan, e.g. US IN Global')
    scan_parser.add_argument('--horizons', nargs='*', help='Horizons to scan, e.g. day week')
    scan_parser.add_argument('--force', action='store_true', help='Override the current-period prediction lock and refresh picks anyway')
    sub.add_parser('check-accuracy')
    backtest_parser = sub.add_parser('backtest')
    backtest_parser.add_argument('--market', default='IN')
    backtest_parser.add_argument('--horizon', default='day')
    backtest_parser.add_argument('--start-date')
    backtest_parser.add_argument('--end-date')
    backtest_parser.add_argument('--universe-limit', type=int, default=BACKTEST_UNIVERSE_LIMIT)
    backtest_parser.add_argument('--top-n', type=int, default=BACKTEST_TOP_N)
    backtest_parser.add_argument('--step-days', type=int, default=BACKTEST_REBALANCE_STEP_DAYS)
    backtest_parser.add_argument('--learning-mode', choices=['raw', 'learned'], default='raw')
    backtest_parser.add_argument('--commit-learning', action='store_true',
                                  help='Write this run into strategy_signal_stats. Omit for research/replication runs.')
    replay_parser = sub.add_parser('backtest-replay', help='Re-run a past backtest_runs.id with its EXACT stored config (true replication).')
    replay_parser.add_argument('run_id', type=int)
    replay_parser.add_argument('--commit-learning', action='store_true')
    args = parser.parse_args()

    if args.command == 'background-scan':
        raise SystemExit(run_background_scan_cli(args))
    if args.command == 'check-accuracy':
        result = run_accuracy_check()
        print(json.dumps(result, default=_json_safe))
        raise SystemExit(1 if result.get('error') else 0)
    if args.command == 'backtest-replay':
        payload = load_backtest_run_payload(args.run_id)
        if not payload.get('ok'):
            print(json.dumps({'ok': False, 'error': f'Could not load run {args.run_id}: {payload.get("error")}'}))
            raise SystemExit(1)
        cfg = ((payload.get('run') or {}).get('config_json')) or {}
        if isinstance(cfg, str):
            try:
                cfg = json.loads(cfg)
            except Exception:
                cfg = {}
        fp = cfg.get('fingerprint_payload') or {}
        # Pull the exact original inputs straight from the stored fingerprint, not from
        # whatever defaults happen to be set today, so top_n/universe_limit/dates can
        # never silently drift the way they did between run #9 and its later "reruns".
        result = run_historical_backtest(
            market=fp.get('market') or (payload.get('run') or {}).get('market'),
            horizon=fp.get('horizon') or (payload.get('run') or {}).get('horizon'),
            start_date=fp.get('start_date'),
            end_date=fp.get('end_date'),
            universe_limit=fp.get('universe_limit'),
            top_n=fp.get('top_n'),
            rebalance_step_days=fp.get('rebalance_step_days'),
            learning_mode=fp.get('learning_mode') or 'raw',
            commit_learning=bool(getattr(args, 'commit_learning', False)),
        )
        print(json.dumps({
            'replayed_run_id': args.run_id,
            'original_metrics': {
                'total_trades': (payload.get('run') or {}).get('total_trades'),
                'profit_factor': (payload.get('run') or {}).get('profit_factor'),
                'win_rate_pct': (payload.get('run') or {}).get('win_rate_pct'),
            },
            'new_metrics': {
                'total_trades': result.get('total_trades'),
                'profit_factor': result.get('profit_factor'),
                'win_rate_pct': result.get('win_rate_pct'),
            },
        }, default=_json_safe, indent=2))
        raise SystemExit(0)
    if args.command == 'backtest':
        result = run_historical_backtest(args.market, args.horizon, args.start_date, args.end_date, args.universe_limit, args.top_n, args.step_days,
                                          learning_mode=args.learning_mode, commit_learning=bool(args.commit_learning))
        print(json.dumps(result, default=_json_safe))
        raise SystemExit(1 if result.get('error') else 0)

    if INTRADAY_SERVER_AUTO_ON_START:
        try:
            settings = get_intraday_settings()
            if settings.get('enabled') and settings.get('auto_enabled'):
                start_intraday_auto_worker()
        except Exception as e:
            print(f"[WARN] Could not start intraday auto worker on boot: {e}")

    try:
        if PAPER_AUTO_ENABLED and PAPER_AUTO_ON_START:
            start_paper_auto_worker()
    except Exception as e:
        print(f"[WARN] Could not start paper auto scheduler on boot: {e}")

    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)


if __name__ == '__main__':
    main()
