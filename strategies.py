"""
Multi-strategy intraday engine.

ARCHITECTURE (replaces the previous "one giant strategy with dozens of global gates"):

    Market data
         |
         v
    FEATURE EXTRACTION            (app.py: extract_intraday_strategy_features)
         |
         v
    LAYER 1 - STRATEGY ENGINE     (this module: evaluate_all_strategies)
      10 independent strategies, each with its OWN entry conditions,
      its OWN confidence score, and its OWN target/stop profile.
         |
         v
    LAYER 2 - STRATEGY SELECTOR   (this module: select_strategy)
      Decides which ONE strategy to trust right now, using setup
      confidence + market-regime compatibility + that strategy's own
      out-of-sample historical edge.
         |
         v
    LAYER 3 - RISK/EXECUTION      (app.py: unchanged, strategy-independent)
      max trades/day, position sizing, capital, chase, signal-fill gap,
      cooldown, stale data, market hours, daily loss limit.

DESIGN RULES THIS MODULE FOLLOWS
--------------------------------
1. Pure functions, no I/O, no DB, no network, no global mutable state. Everything
   comes in through arguments. This is what makes it possible for Replay, Paper and
   Live to share one engine and provably behave identically -- the only thing that
   differs between them is where the feature dict came from and whether the fill is
   simulated.
2. Nothing here decides position size, how many trades you may take, or whether you
   can afford the trade. Those are Layer 3 concerns and deliberately live elsewhere.
   A strategy answers exactly one question: "is this a valid setup of my type right
   now, and how confident am I?"
3. Win rates are NEVER hard-coded. A strategy does not get to assert it wins 65% of
   the time; it has to earn that number from recorded out-of-sample trades, and until
   it has enough of them it is explicitly treated as unproven (see NEUTRAL_PRIOR and
   MIN_TRADES_FOR_EDGE below).
4. Adaptation is evidence-driven and deterministic. This module contains no AI calls
   and no self-modifying rules. It classifies what happened; changing a rule based on
   that evidence is a separate, human-approved step.
"""

from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Calibration constants
# ---------------------------------------------------------------------------

# A strategy needs at least this many recorded trades before its measured win rate
# is allowed to influence selection at all. Below this, 3 wins out of 4 is noise, not
# an edge -- and letting it act like an edge is how a system talks itself into
# over-trading whichever strategy got lucky first.
MIN_TRADES_FOR_EDGE = 30

# What an unproven strategy is assumed to be worth. Deliberately neutral (not
# optimistic): a brand-new strategy competes on setup quality and regime fit alone
# until it has a real track record.
NEUTRAL_PRIOR_EDGE = 50.0

# GRADUATED COLD-START EVIDENCE (report #2/#3): a strategy below MIN_TRADES_FOR_EDGE
# used to be flatly "unproven" -- true at 0 trades and at 29 trades alike -- and
# competed on setup quality with the same authority either way. This ladder blends
# the strategy's OWN measured edge toward the neutral prior in proportion to how
# little evidence actually backs it, so 29 trades of real (if noisy) signal counts
# for more than 2 trades of pure luck, while nothing is treated as "proven" until
# it clears MIN_TRADES_FOR_EDGE. Kept as its own copy (not shared with app.py's
# evidence_tier_for_trade_count) because this module is deliberately free of any
# dependency on app.py -- see this file's design rules above.
#   0-9 trades  -> 0.15 evidence weight (research only)
#   10-29       -> 0.50 evidence weight (reduced risk)
#   30+         -> fully proven (handled by the MIN_TRADES_FOR_EDGE branch below)
def _evidence_weight_below_min_trades(trades: int) -> float:
    if trades < 10:
        return 0.15
    return 0.50

# Selector weights. Setup quality dominates, but regime fit and proven edge both
# matter enough to break ties and to demote a strategy that keeps losing.
# These are the NO-EVIDENCE weights: used when nothing in the candidate set has a
# proven, context-conditioned expected value yet (a cold selector has nothing
# better to go on than setup quality + regime fit).
WEIGHT_CONFIDENCE = 0.50
WEIGHT_REGIME_FIT = 0.30
WEIGHT_HISTORICAL_EDGE = 0.20

# EVIDENCE-PRESENT weights. The moment at least one candidate strategy has a
# proven, context-conditioned expected net edge, expected value becomes the
# dominant term and raw confidence is demoted -- "how good does this setup look"
# must not outrank "what does this setup type actually pay, in this regime, at
# this time of day, after costs". Kept as a separate weight set (rather than
# permanently re-weighting the ones above) so a cold start behaves exactly as
# before instead of ranking everything off a 50.0 placeholder EV.
WEIGHT_CONFIDENCE_EV = 0.30
WEIGHT_REGIME_FIT_EV = 0.15
WEIGHT_HISTORICAL_EDGE_EV = 0.15
WEIGHT_EXPECTED_VALUE_EV = 0.40

# Maps an expected net edge in % per trade onto the same 0-100 scale the other
# selector terms use. 0.00% (breakeven) -> 50; +0.50% -> 80; -0.50% -> 20.
EV_SCORE_SLOPE = 60.0

# A strategy whose measured performance is this bad gets benched automatically
# (still evaluated and logged, never selected) once it has enough trades to judge.
AUTO_DISABLE_PROFIT_FACTOR = 0.85
AUTO_DISABLE_MIN_TRADES = 40

# ---------------------------------------------------------------------------
# FAST (same-day) ADAPTATION defaults -- see assess_session_adaptation()
# ---------------------------------------------------------------------------
# The slow layer (measured expectancy across days -> strategy health, auto-bench)
# cannot react inside a single session: a strategy's evidence snapshot is built
# from trades that closed BEFORE the day started, so five consecutive losses at
# 09:30-09:50 change nothing until tomorrow. These defaults drive the fast layer
# that does react within the day, and they are deliberately about REDUCING
# exposure, never about increasing it -- a strategy cannot win its way into
# bigger size intraday, only lose its way into smaller size or a timeout.
FAST_ADAPT_LOSS_STREAK = 2              # consecutive losses today -> timeout
FAST_ADAPT_TIMEOUT_MINUTES = 45         # length of that timeout
FAST_ADAPT_MAX_LOSSES_PER_DAY = 3       # losses today -> benched for the rest of the day
FAST_ADAPT_LOSS_PENALTY_PER_LOSS = 4.0  # score points removed per net-losing trade today
FAST_ADAPT_MAX_LOSS_PENALTY = 15.0
FAST_ADAPT_RISK_STEP = 0.5              # risk multiplier applied once a strategy is bleeding


# ---------------------------------------------------------------------------
# Market regime detection
# ---------------------------------------------------------------------------

REGIMES = ('strong_uptrend', 'mild_uptrend', 'choppy', 'downtrend', 'volatile')


def detect_market_regime(features: Dict[str, Any], market_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Classify the CURRENT market regime from index behaviour plus this session's
    own character. Regime is a property of the market, not of the individual stock,
    so it is computed once per scan tick and handed to every strategy.

    Falls back gracefully: with no index context at all it degrades to 'choppy',
    which is the honest answer (we don't know) and which favours strategies that
    don't depend on a trend existing.
    """
    ctx = market_context or {}
    # BUGFIX (regime/index parity): the only place either Live (get_market_context)
    # or Replay (build_historical_market_context_for_day /
    # build_historical_intraday_index_context) actually populate today's index move
    # is the nested 'intraday_index': {'change_pct': ...} dict -- neither ever wrote a
    # top-level 'index_change_pct' key. Reading only the top-level key meant this
    # function silently fell through to the "no index context" branch below on
    # EVERY tick, in both engines, and always classified the regime as 'choppy'
    # regardless of the real market move. Check both shapes so a caller who does
    # populate the flat key (now or in future) still works, and so the nested shape
    # both engines actually produce is no longer silently ignored.
    intraday_index = ctx.get('intraday_index') if isinstance(ctx.get('intraday_index'), dict) else {}
    index_change = _safe_float(ctx.get('index_change_pct'))
    if index_change is None:
        index_change = _safe_float(intraday_index.get('change_pct'))
    breadth = _safe_float(ctx.get('advance_decline_ratio'))
    if breadth is None:
        breadth = _safe_float(intraday_index.get('advance_decline_ratio'))
    index_vol = _safe_float(ctx.get('index_volatility_pct'))
    if index_vol is None:
        index_vol = _safe_float(intraday_index.get('index_volatility_pct'))
    session_range_pct = _safe_float(features.get('session_range_pct')) or 0.0

    reasons: List[str] = []

    if index_change is None:
        regime = 'choppy'
        reasons.append('no index context available; defaulting to choppy (assume no trend rather than guess one)')
        return {'regime': regime, 'reasons': reasons, 'index_change_pct': None, 'confidence': 30.0}

    # Volatile takes precedence: a wild tape invalidates trend-following assumptions
    # even when the index happens to be net-up on the day.
    if (index_vol is not None and index_vol > 1.5) or session_range_pct > 3.5:
        regime = 'volatile'
        reasons.append(f'wide ranges (index vol {index_vol}, session range {round(session_range_pct, 2)}%)')
    elif index_change >= 0.6 and (breadth is None or breadth >= 1.2):
        regime = 'strong_uptrend'
        reasons.append(f'index +{round(index_change, 2)}% with supportive breadth')
    elif index_change >= 0.15:
        regime = 'mild_uptrend'
        reasons.append(f'index +{round(index_change, 2)}%')
    elif index_change <= -0.4:
        regime = 'downtrend'
        reasons.append(f'index {round(index_change, 2)}%')
    else:
        regime = 'choppy'
        reasons.append(f'index flat ({round(index_change, 2)}%), no clear direction')

    if breadth is not None:
        reasons.append(f'advance/decline {round(breadth, 2)}')

    return {
        'regime': regime,
        'reasons': reasons,
        'index_change_pct': round(index_change, 3),
        'breadth': round(breadth, 2) if breadth is not None else None,
        'confidence': 70.0,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        f = float(v)
        if f != f:  # NaN
            return None
        return f
    except (TypeError, ValueError):
        return None


def _clamp(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, v))


# ---------------------------------------------------------------------------
# SETUP CONTEXT -- the conditioning variables a calibrated probability needs
# ---------------------------------------------------------------------------
# "P(win)" only means something conditioned on something. A single strategy-wide
# win rate answers "how often does EMA Trend win, ever", which is not the
# question being asked at entry time -- the question is "how often does EMA
# Trend win HERE: this regime, this part of the session, this volatility, with
# the stock already this extended and this strong/weak against the index".
#
# These buckets are deliberately COARSE. Fine-grained conditioning sounds more
# precise but splits the evidence into slices too small to ever reach
# significance, which is how a system ends up confidently quoting a probability
# derived from four trades. Three or four buckets per dimension keeps each slice
# reachable within a realistic number of recorded trades, and the hierarchical
# blend in app.py (estimate_calibrated_win_probability) shrinks any slice that is
# still thin back toward the strategy-level rate rather than trusting it.
#
# Every bucket is computed from features that already exist on every candidate
# (extract_intraday_strategy_features), so nothing here needs new market data.

def volatility_bucket(features: Optional[Dict[str, Any]]) -> Optional[str]:
    """Session range as a % of the open, bucketed. A setup that works in a quiet
    tape often does not survive a wide-range one (same entry, same stop, very
    different odds of being shaken out)."""
    r = _safe_float((features or {}).get('session_range_pct'))
    if r is None:
        return None
    if r < 1.0:
        return 'vol_low'
    if r < 2.0:
        return 'vol_normal'
    if r < 3.5:
        return 'vol_high'
    return 'vol_extreme'


def extension_bucket(features: Optional[Dict[str, Any]]) -> Optional[str]:
    """How far the stock has already travelled today. Buying the first 0.5% of a
    move and buying the fourth 0.5% of the same move are not the same trade, and
    a strategy-wide win rate silently averages the two together."""
    c = _safe_float((features or {}).get('change_pct'))
    if c is None:
        return None
    if c < -0.5:
        return 'ext_red'
    if c < 0.75:
        return 'ext_flat'
    if c < 2.0:
        return 'ext_moderate'
    return 'ext_stretched'


def relative_strength_bucket(features: Optional[Dict[str, Any]]) -> Optional[str]:
    """Stock versus index, so far today. Leading the tape and lagging it are
    different setups even when every other feature reads identically."""
    rs = _safe_float((features or {}).get('relative_strength_pct'))
    if rs is None:
        return None
    if rs < -0.5:
        return 'rs_lagging'
    if rs < 0.5:
        return 'rs_inline'
    if rs < 1.5:
        return 'rs_leading'
    return 'rs_strong_leader'


def build_setup_context(features: Optional[Dict[str, Any]], regime: Optional[str],
                        entry_dt: Any = None) -> Dict[str, Any]:
    """The full context key-set for one candidate at one moment.

    Returned as a plain dict of bucket labels so it can travel with the candidate,
    be stored on the trade record at entry, and later be matched against recorded
    trades in exactly the same shape -- which is what makes P(win | this setup,
    this regime, this time) computable at all instead of aspirational.
    """
    ctx: Dict[str, Any] = {
        'regime': regime,
        'time_bucket': _entry_time_bucket({'entry_time': entry_dt}) if entry_dt is not None else None,
        'volatility_bucket': volatility_bucket(features),
        'extension_bucket': extension_bucket(features),
        'relative_strength_bucket': relative_strength_bucket(features),
    }
    return ctx


class Signal:
    """One strategy's verdict on one stock at one moment."""

    __slots__ = ('strategy_id', 'name', 'eligible', 'confidence', 'reasons', 'blocks',
                 'target_pct', 'stop_pct', 'min_confidence')

    def __init__(self, strategy_id: str, name: str):
        self.strategy_id = strategy_id
        self.name = name
        self.eligible = False
        self.confidence = 0.0
        self.reasons: List[str] = []
        self.blocks: List[str] = []
        self.target_pct: Optional[float] = None
        self.stop_pct: Optional[float] = None
        self.min_confidence: float = 55.0

    def add(self, points: float, reason: str) -> None:
        self.confidence += points
        sign = '+' if points >= 0 else ''
        self.reasons.append(f'{reason} ({sign}{round(points, 1)})')

    def block(self, reason: str) -> None:
        self.blocks.append(reason)

    def finalize(self, target_pct: float, stop_pct: float, min_confidence: float = 55.0) -> 'Signal':
        self.confidence = _clamp(self.confidence)
        self.target_pct = target_pct
        self.stop_pct = stop_pct
        self.min_confidence = min_confidence
        if self.blocks:
            self.eligible = False
        elif self.confidence < min_confidence:
            self.eligible = False
            self.blocks.append(f'confidence {round(self.confidence, 1)} below this strategy\'s own minimum {min_confidence}')
        else:
            self.eligible = True
        return self

    def to_dict(self) -> Dict[str, Any]:
        return {
            'strategy_id': self.strategy_id,
            'name': self.name,
            'eligible': self.eligible,
            'confidence': round(self.confidence, 1),
            'reasons': self.reasons,
            'blocks': self.blocks,
            'target_pct': self.target_pct,
            'stop_pct': self.stop_pct,
            'min_confidence': self.min_confidence,
        }


# ---------------------------------------------------------------------------
# LAYER 1 -- the 10 strategies
# ---------------------------------------------------------------------------
# Each strategy owns its entry conditions AND its risk profile (target/stop as a
# % of entry). A tight scalp-style breakout and a wider pullback-continuation trade
# should NOT be forced to share one global target/stop, which is what the old
# single-strategy design did.

def _s_opening_breakout(f: Dict[str, Any], s: Dict[str, Any]) -> Signal:
    """Opening range breakout with follow-through confirmation.

    The failure mode this strategy is specifically built to avoid is the false
    breakout that Run 6 kept buying: price pokes above the opening range, the entry
    fires, and it immediately falls back. Hence the hard requirement that the
    breakout has actually HELD (sustained closes) plus real volume behind it.
    """
    sig = Signal('opening_breakout', 'Opening Breakout')
    breakout = bool(f.get('breakout'))
    failed = bool(f.get('failed_breakout'))
    vol_mult = _safe_float(f.get('volume_multiplier'))
    change = _safe_float(f.get('change_pct')) or 0.0
    minutes = _safe_float(f.get('minutes_since_open')) or 0.0
    opening_high = _safe_float(f.get('opening_high'))
    last = _safe_float(f.get('last'))

    if not breakout:
        sig.block('no confirmed opening-range breakout')
    if failed:
        sig.block('breakout already failed once today (broke out, then fell back below the range)')
    if minutes < 10:
        sig.block('opening range has not finished forming yet')
    # A breakout is only tradable while it is still fresh. Buying an opening-range
    # break at 14:00 is a different (worse) trade than buying it at 09:45.
    if minutes > 150:
        sig.block('opening-range breakout is stale this late in the session')
    if vol_mult is not None and vol_mult < 1.4:
        sig.block(f'breakout volume too thin ({round(vol_mult, 2)}x) -- breakouts without volume are the classic false-breakout setup')

    if breakout:
        sig.add(30, 'confirmed opening-range breakout with sustained closes')
    if vol_mult is not None and vol_mult >= 1.4:
        sig.add(min(22, (vol_mult - 1.0) * 16), f'volume expansion {round(vol_mult, 2)}x')
    if f.get('above_vwap'):
        sig.add(14, 'trading above VWAP')
    if f.get('recent_trend') == 'rising':
        sig.add(12, 'recent candles rising')
    if f.get('momentum_state') == 'accelerating':
        sig.add(10, 'momentum accelerating')
    elif f.get('momentum_state') == 'exhausted':
        sig.add(-25, 'momentum exhausted')
    # Proximity to the breakout level: entering right at the level is a good trade,
    # entering 1.5% above it means the move is already gone and the stop is far away.
    if opening_high and last and opening_high > 0:
        ext = ((last - opening_high) / opening_high) * 100
        if ext > 1.2:
            sig.add(-min(25, (ext - 1.2) * 12), f'already {round(ext, 2)}% beyond the breakout level -- extended entry')
        else:
            sig.add(8, f'entry still close to the breakout level ({round(ext, 2)}% above)')
    if change > 4.0:
        sig.add(-15, f'already up {round(change, 2)}% today')
    return sig.finalize(target_pct=0.85, stop_pct=0.5, min_confidence=58)


def _s_vwap_momentum(f: Dict[str, Any], s: Dict[str, Any]) -> Signal:
    """Trending above VWAP with EMA alignment -- the 'trend is intact, join it' trade."""
    sig = Signal('vwap_momentum', 'VWAP Momentum')
    above = bool(f.get('above_vwap'))
    dist = _safe_float(f.get('vwap_distance_pct'))
    ema9 = _safe_float(f.get('ema9'))
    ema21 = _safe_float(f.get('ema21'))
    vol_mult = _safe_float(f.get('volume_multiplier'))

    if not above:
        sig.block('price is below VWAP')
    if f.get('recent_trend') == 'falling':
        sig.block('recent candles falling')
    if dist is not None and dist > 2.0:
        sig.block(f'{round(dist, 2)}% above VWAP -- too extended, this is chasing not joining')
    if f.get('momentum_state') == 'exhausted':
        sig.block('momentum exhausted')

    if above:
        sig.add(22, 'holding above VWAP')
    if dist is not None and 0.1 <= dist <= 1.0:
        sig.add(18, f'constructive distance from VWAP ({round(dist, 2)}%) -- room left to run')
    if ema9 and ema21 and ema9 > ema21:
        sig.add(20, 'EMA9 above EMA21 (short-term trend aligned)')
    elif ema9 and ema21:
        sig.add(-12, 'EMA9 below EMA21 (trend not aligned)')
    if f.get('recent_trend') == 'rising':
        sig.add(14, 'recent candles rising')
    if vol_mult is not None and vol_mult >= 1.2:
        sig.add(min(14, (vol_mult - 1.0) * 12), f'volume support {round(vol_mult, 2)}x')
    if f.get('momentum_state') == 'accelerating':
        sig.add(12, 'momentum accelerating')
    return sig.finalize(target_pct=0.8, stop_pct=0.45, min_confidence=58)


def _s_pullback_continuation(f: Dict[str, Any], s: Dict[str, Any]) -> Signal:
    """Strong move, orderly retracement, then resumption. Buys the dip WITHIN an
    established uptrend -- explicitly not a falling knife: requires the trend to still
    be intact and momentum to be turning back up."""
    sig = Signal('pullback_continuation', 'Pullback Continuation')
    pullback = _safe_float(f.get('pullback_from_high_pct'))
    change = _safe_float(f.get('change_pct')) or 0.0
    ema21 = _safe_float(f.get('ema21'))
    last = _safe_float(f.get('last'))
    vwap = _safe_float(f.get('vwap'))

    if change < 0.5:
        sig.block('no meaningful prior up-move to pull back from')
    if pullback is None:
        sig.block('cannot measure pullback depth')
    else:
        if pullback < 0.25:
            sig.block('no actual pullback yet (still at highs) -- this is a breakout setup, not a pullback')
        if pullback > 1.8:
            sig.block(f'pullback too deep ({round(pullback, 2)}%) -- trend likely broken, not a dip')
    if f.get('recent_trend') == 'falling' and f.get('momentum_state') != 'accelerating':
        sig.block('still falling with no sign of resumption')
    if not f.get('above_vwap'):
        sig.block('pullback has broken below VWAP -- support lost')

    if pullback is not None and 0.25 <= pullback <= 1.8:
        sig.add(26, f'orderly {round(pullback, 2)}% pullback from session high')
    if change >= 1.0:
        sig.add(min(18, change * 6), f'strong prior move (+{round(change, 2)}%) to continue')
    if last and ema21 and last >= ema21:
        sig.add(16, 'holding above EMA21 support through the pullback')
    if last and vwap and last >= vwap:
        sig.add(14, 'holding above VWAP support through the pullback')
    if f.get('momentum_state') == 'accelerating':
        sig.add(18, 'momentum turning back up (resumption confirmed)')
    elif f.get('recent_trend') == 'rising':
        sig.add(12, 'candles turning back up')
    else:
        sig.add(-10, 'no resumption signal yet')
    return sig.finalize(target_pct=0.9, stop_pct=0.5, min_confidence=60)


def _s_ema_trend(f: Dict[str, Any], s: Dict[str, Any]) -> Signal:
    """Clean EMA stack. The most conservative trend-follower here: it wants structure,
    not excitement, and deliberately scores volume spikes low."""
    sig = Signal('ema_trend', 'EMA Trend')
    ema9 = _safe_float(f.get('ema9'))
    ema21 = _safe_float(f.get('ema21'))
    last = _safe_float(f.get('last'))

    if not (ema9 and ema21 and last):
        sig.block('EMA data unavailable (not enough bars yet)')
    else:
        if ema9 <= ema21:
            sig.block('EMA9 not above EMA21 -- no established short-term uptrend')
        if last < ema9:
            sig.block('price below EMA9 -- trend structure broken')
        spread = ((ema9 - ema21) / ema21) * 100 if ema21 else 0
        if spread > 1.5:
            sig.block(f'EMA spread {round(spread, 2)}% -- trend already overextended')
        else:
            sig.add(28, f'clean EMA stack (EMA9 {round(spread, 2)}% above EMA21)')
        if last >= ema9:
            sig.add(18, 'price holding above EMA9')
    if f.get('above_vwap'):
        sig.add(16, 'above VWAP')
    if f.get('recent_trend') == 'rising':
        sig.add(16, 'recent candles rising')
    if f.get('momentum_state') == 'exhausted':
        sig.add(-22, 'momentum exhausted')
    return sig.finalize(target_pct=0.75, stop_pct=0.45, min_confidence=58)


def _s_rsi_macd_momentum(f: Dict[str, Any], s: Dict[str, Any]) -> Signal:
    """Classic momentum-oscillator confirmation. Deliberately refuses overbought
    readings -- RSI > 75 intraday is usually the END of the move, not the start."""
    sig = Signal('rsi_macd_momentum', 'RSI/MACD Momentum')
    rsi = _safe_float(f.get('rsi'))
    macd_hist = _safe_float(f.get('macd_hist'))

    if rsi is None or macd_hist is None:
        sig.block('RSI/MACD unavailable (not enough intraday bars yet)')
    else:
        if rsi > 75:
            sig.block(f'RSI {rsi} overbought -- late to the move')
        if rsi < 50:
            sig.block(f'RSI {rsi} below 50 -- no momentum to ride')
        if macd_hist <= 0:
            sig.block('MACD histogram not positive -- momentum not confirmed')
        if 55 <= (rsi or 0) <= 70:
            sig.add(28, f'RSI {rsi} in the productive momentum band')
        elif rsi is not None and 50 <= rsi < 55:
            sig.add(12, f'RSI {rsi} just turning up')
        if macd_hist and macd_hist > 0:
            sig.add(24, 'MACD histogram positive (momentum confirmed)')
    if f.get('above_vwap'):
        sig.add(14, 'above VWAP')
    if f.get('recent_trend') == 'rising':
        sig.add(14, 'recent candles rising')
    vol_mult = _safe_float(f.get('volume_multiplier'))
    if vol_mult is not None and vol_mult >= 1.2:
        sig.add(min(12, (vol_mult - 1.0) * 10), f'volume support {round(vol_mult, 2)}x')
    return sig.finalize(target_pct=0.8, stop_pct=0.5, min_confidence=60)


def _s_volume_expansion(f: Dict[str, Any], s: Dict[str, Any]) -> Signal:
    """Institutional-footprint trade: unusual volume WITH price confirmation.
    Volume alone is not a signal -- heavy volume on a falling stock is distribution,
    so price direction is a hard requirement here."""
    sig = Signal('volume_expansion', 'Volume Expansion')
    vol_mult = _safe_float(f.get('volume_multiplier'))
    change = _safe_float(f.get('change_pct')) or 0.0

    if vol_mult is None:
        sig.block('volume data unavailable')
    elif vol_mult < 2.0:
        sig.block(f'volume {round(vol_mult, 2)}x below the 2.0x threshold this strategy requires')
    if change <= 0:
        sig.block('heavy volume without a positive price move is distribution, not accumulation')
    if f.get('recent_trend') == 'falling':
        sig.block('price falling on the volume surge')
    if not f.get('above_vwap'):
        sig.block('below VWAP despite the volume surge')

    if vol_mult is not None and vol_mult >= 2.0:
        sig.add(min(34, (vol_mult - 1.0) * 14), f'volume surge {round(vol_mult, 2)}x average')
    if change > 0:
        sig.add(min(18, change * 8), f'price confirming (+{round(change, 2)}%)')
    if f.get('above_vwap'):
        sig.add(14, 'above VWAP')
    if f.get('momentum_state') == 'accelerating':
        sig.add(14, 'momentum accelerating')
    elif f.get('momentum_state') == 'exhausted':
        sig.add(-24, 'momentum exhausted despite volume -- likely a climax/blow-off')
    return sig.finalize(target_pct=0.95, stop_pct=0.55, min_confidence=60)


def _s_relative_strength(f: Dict[str, Any], s: Dict[str, Any]) -> Signal:
    """Stock outperforming the index. Strongest when the market is flat or weak and
    the stock is up anyway -- that divergence is the actual signal."""
    sig = Signal('relative_strength', 'Relative Strength')
    rs = _safe_float(f.get('relative_strength_pct'))
    change = _safe_float(f.get('change_pct')) or 0.0

    if rs is None:
        sig.block('no index benchmark available to measure relative strength against')
    elif rs < 0.5:
        sig.block(f'only {round(rs, 2)}% ahead of the index -- not meaningful outperformance')
    if change <= 0:
        sig.block('stock is not actually up on the day')
    if f.get('recent_trend') == 'falling':
        sig.block('recent candles falling')
    if not f.get('above_vwap'):
        sig.block('below VWAP')

    if rs is not None and rs >= 0.5:
        sig.add(min(32, rs * 14), f'outperforming the index by {round(rs, 2)}%')
    if f.get('above_vwap'):
        sig.add(16, 'above VWAP')
    if f.get('recent_trend') == 'rising':
        sig.add(16, 'recent candles rising')
    vol_mult = _safe_float(f.get('volume_multiplier'))
    if vol_mult is not None and vol_mult >= 1.3:
        sig.add(min(14, (vol_mult - 1.0) * 11), f'volume confirmation {round(vol_mult, 2)}x')
    if f.get('momentum_state') == 'exhausted':
        sig.add(-20, 'momentum exhausted')
    return sig.finalize(target_pct=0.85, stop_pct=0.5, min_confidence=58)


def _s_range_breakout(f: Dict[str, Any], s: Dict[str, Any]) -> Signal:
    """Breakout from an intraday consolidation range (distinct from the OPENING range).
    Wants a genuinely tight coil first -- a 'breakout' from a wide sloppy range is noise."""
    sig = Signal('range_breakout', 'Intraday Range Break')
    coil = _safe_float(f.get('consolidation_range_pct'))
    last = _safe_float(f.get('last'))
    session_high = _safe_float(f.get('session_high'))
    minutes = _safe_float(f.get('minutes_since_open')) or 0.0
    vol_mult = _safe_float(f.get('volume_multiplier'))

    if minutes < 45:
        sig.block('too early in the session for a post-opening consolidation range to exist')
    if coil is None:
        sig.block('cannot measure the consolidation range')
    elif coil > 1.2:
        sig.block(f'range {round(coil, 2)}% is too wide/sloppy to count as a coil')
    if not (last and session_high) or last < session_high * 0.998:
        sig.block('price is not breaking out to new session highs')
    if vol_mult is not None and vol_mult < 1.3:
        sig.block(f'range break on only {round(vol_mult, 2)}x volume -- unconvincing')

    if coil is not None and coil <= 1.2:
        sig.add(26, f'tight {round(coil, 2)}% consolidation before the break')
    if last and session_high and last >= session_high * 0.998:
        sig.add(22, 'breaking to new session highs')
    if vol_mult is not None and vol_mult >= 1.3:
        sig.add(min(20, (vol_mult - 1.0) * 13), f'volume expansion {round(vol_mult, 2)}x')
    if f.get('above_vwap'):
        sig.add(14, 'above VWAP')
    if f.get('momentum_state') == 'accelerating':
        sig.add(12, 'momentum accelerating')
    return sig.finalize(target_pct=0.85, stop_pct=0.5, min_confidence=60)


def _s_vwap_reclaim(f: Dict[str, Any], s: Dict[str, Any]) -> Signal:
    """Stock was below VWAP, has just reclaimed it, and is holding. A genuine
    turn-of-character trade -- but it needs volume, because an unconvinced reclaim
    just fails straight back down."""
    sig = Signal('vwap_reclaim', 'VWAP Reclaim')
    above = bool(f.get('above_vwap'))
    dist = _safe_float(f.get('vwap_distance_pct'))
    reclaimed = bool(f.get('vwap_reclaimed'))
    vol_mult = _safe_float(f.get('volume_multiplier'))

    if not above:
        sig.block('not above VWAP')
    if not reclaimed:
        sig.block('no VWAP reclaim detected (was not below VWAP earlier in the session)')
    if dist is not None and dist > 0.8:
        sig.block(f'already {round(dist, 2)}% past the reclaim level -- the entry has moved on')
    if vol_mult is not None and vol_mult < 1.3:
        sig.block(f'reclaim on only {round(vol_mult, 2)}x volume -- unconvincing')
    if f.get('recent_trend') == 'falling':
        sig.block('recent candles still falling')

    if reclaimed and above:
        sig.add(30, 'VWAP reclaimed and holding')
    if dist is not None and 0 <= dist <= 0.8:
        sig.add(18, f'still close to the reclaim level ({round(dist, 2)}%)')
    if vol_mult is not None and vol_mult >= 1.3:
        sig.add(min(18, (vol_mult - 1.0) * 12), f'volume confirmation {round(vol_mult, 2)}x')
    if f.get('recent_trend') == 'rising':
        sig.add(16, 'recent candles rising')
    if f.get('momentum_state') == 'accelerating':
        sig.add(12, 'momentum accelerating')
    return sig.finalize(target_pct=0.8, stop_pct=0.5, min_confidence=62)


def _s_trend_reversal(f: Dict[str, Any], s: Dict[str, Any]) -> Signal:
    """Downtrend exhaustion followed by a confirmed turn. The hardest and least
    reliable setup here, so it carries the highest confidence bar and the tightest
    stop, and the selector's regime table deliberately penalises it in most regimes.
    Kept mainly so its (likely poor) performance can be MEASURED rather than assumed."""
    sig = Signal('trend_reversal', 'Trend Reversal')
    rsi = _safe_float(f.get('rsi'))
    change = _safe_float(f.get('change_pct')) or 0.0
    vol_mult = _safe_float(f.get('volume_multiplier'))

    if change > -0.4:
        sig.block('no meaningful decline to reverse from')
    if rsi is None:
        sig.block('RSI unavailable')
    elif rsi > 45:
        sig.block(f'RSI {rsi} -- not oversold enough to call exhaustion')
    if f.get('recent_trend') != 'rising':
        sig.block('no confirmed turn yet (candles not rising)')
    if not f.get('above_vwap'):
        sig.block('has not reclaimed VWAP -- reversal unconfirmed')
    if vol_mult is not None and vol_mult < 1.5:
        sig.block(f'reversal on only {round(vol_mult, 2)}x volume -- needs conviction')

    if rsi is not None and rsi <= 45:
        sig.add(22, f'RSI {rsi} showing downside exhaustion')
    if f.get('recent_trend') == 'rising':
        sig.add(24, 'candles have turned up')
    if f.get('above_vwap'):
        sig.add(20, 'VWAP reclaimed -- reversal confirmed')
    if vol_mult is not None and vol_mult >= 1.5:
        sig.add(min(18, (vol_mult - 1.0) * 11), f'conviction volume {round(vol_mult, 2)}x')
    return sig.finalize(target_pct=0.9, stop_pct=0.45, min_confidence=68)


# Registry. `regime_fit` is each strategy's compatibility (0-100) with each regime --
# this is prior structural knowledge about WHEN a strategy type works, and it is
# separate from measured performance (which is learned from real trades).
STRATEGIES: List[Dict[str, Any]] = [
    {'id': 'opening_breakout', 'name': 'Opening Breakout', 'fn': _s_opening_breakout,
     'description': 'Trades a confirmed, volume-backed hold beyond the opening range while it is still fresh; evaluated symmetrically long/short.',
     'regime_fit': {'strong_uptrend': 95, 'mild_uptrend': 80, 'choppy': 35, 'downtrend': 15, 'volatile': 45}},
    {'id': 'vwap_momentum', 'name': 'VWAP Momentum', 'fn': _s_vwap_momentum,
     'description': 'Joins an intact intraday uptrend holding above VWAP with EMA alignment.',
     'regime_fit': {'strong_uptrend': 90, 'mild_uptrend': 88, 'choppy': 45, 'downtrend': 20, 'volatile': 50}},
    {'id': 'pullback_continuation', 'name': 'Pullback Continuation', 'fn': _s_pullback_continuation,
     'description': 'Buys an orderly dip inside an established uptrend once momentum resumes.',
     'regime_fit': {'strong_uptrend': 92, 'mild_uptrend': 85, 'choppy': 55, 'downtrend': 25, 'volatile': 55}},
    {'id': 'ema_trend', 'name': 'EMA Trend', 'fn': _s_ema_trend,
     'description': 'Conservative trend-follower requiring a clean, non-extended EMA stack.',
     'regime_fit': {'strong_uptrend': 85, 'mild_uptrend': 82, 'choppy': 40, 'downtrend': 18, 'volatile': 40}},
    {'id': 'rsi_macd_momentum', 'name': 'RSI/MACD Momentum', 'fn': _s_rsi_macd_momentum,
     'description': 'Oscillator-confirmed momentum that refuses overbought entries.',
     'regime_fit': {'strong_uptrend': 80, 'mild_uptrend': 82, 'choppy': 55, 'downtrend': 25, 'volatile': 45}},
    {'id': 'volume_expansion', 'name': 'Volume Expansion', 'fn': _s_volume_expansion,
     'description': 'Unusual institutional-scale volume with price confirmation.',
     'regime_fit': {'strong_uptrend': 85, 'mild_uptrend': 78, 'choppy': 60, 'downtrend': 35, 'volatile': 62}},
    {'id': 'relative_strength', 'name': 'Relative Strength', 'fn': _s_relative_strength,
     'description': 'Stock materially outperforming the index -- strongest on flat/weak tape.',
     'regime_fit': {'strong_uptrend': 65, 'mild_uptrend': 80, 'choppy': 85, 'downtrend': 70, 'volatile': 60}},
    {'id': 'range_breakout', 'name': 'Intraday Range Break', 'fn': _s_range_breakout,
     'description': 'Break to new session highs out of a tight post-opening coil.',
     'regime_fit': {'strong_uptrend': 82, 'mild_uptrend': 80, 'choppy': 65, 'downtrend': 30, 'volatile': 45}},
    {'id': 'vwap_reclaim', 'name': 'VWAP Reclaim', 'fn': _s_vwap_reclaim,
     'description': 'Turn-of-character: was below VWAP, reclaimed it on volume, now holding.',
     'regime_fit': {'strong_uptrend': 70, 'mild_uptrend': 75, 'choppy': 72, 'downtrend': 48, 'volatile': 58}},
    {'id': 'trend_reversal', 'name': 'Trend Reversal', 'fn': _s_trend_reversal,
     'description': 'Oversold exhaustion plus a confirmed turn. Lowest-reliability setup by design.',
     'regime_fit': {'strong_uptrend': 40, 'mild_uptrend': 45, 'choppy': 55, 'downtrend': 50, 'volatile': 42}},
]

STRATEGY_BY_ID: Dict[str, Dict[str, Any]] = {s['id']: s for s in STRATEGIES}


def _mirror_features_for_short(features: Dict[str, Any]) -> Dict[str, Any]:
    """Create a true direction-symmetric feature view for short evaluation.

    High/low fields are swapped after reflection so breakout/pullback rules retain
    their intended semantics. Directional metrics are inverted, and oscillators are
    mirrored around their neutral scale. No live data or state is touched here.
    """
    source = dict(features or {})
    f = dict(source)
    # Price reflection preserves ordering but turns a downside move into the same
    # geometric shape as the corresponding upside move.
    scalar_price_keys = ('last', 'open', 'vwap', 'ema9', 'ema21', 'ema50', 'resistance_level', 'support_level')
    raw_vals = [_safe_float(source.get(k)) for k in scalar_price_keys]
    raw_vals = [v for v in raw_vals if v is not None]
    anchor = (max(raw_vals) + max(abs(max(raw_vals)) * 1e-6, 1e-6)) if raw_vals else 1.0
    def mirror(v):
        x = _safe_float(v)
        return None if x is None else (2.0 * anchor - x)
    for k in scalar_price_keys:
        mv = mirror(source.get(k))
        if mv is not None:
            f[k] = mv
    # Directional extrema must swap roles after reflection.
    for hi, lo in (('session_high','session_low'), ('opening_high','opening_low')):
        hi_m, lo_m = mirror(source.get(lo)), mirror(source.get(hi))
        if hi_m is not None: f[hi] = hi_m
        if lo_m is not None: f[lo] = lo_m
    if source.get('vwap') is not None and f.get('last') is not None and float(f.get('vwap') or 0) != 0:
        f['vwap_distance_pct'] = ((float(f['last']) - float(f['vwap'])) / float(f['vwap'])) * 100.0
    elif source.get('vwap_distance_pct') is not None:
        f['vwap_distance_pct'] = -float(source['vwap_distance_pct'])
    if f.get('last') is not None and f.get('session_high') is not None and float(f.get('session_high') or 0) != 0:
        f['pullback_from_high_pct'] = ((float(f['last']) - float(f['session_high'])) / float(f['session_high'])) * 100.0
    if f.get('last') is not None and f.get('session_low') is not None and float(f.get('session_low') or 0) != 0:
        f['pullback_from_low_pct'] = ((float(f['last']) - float(f['session_low'])) / float(f['session_low'])) * 100.0
    for k in ('change_pct', 'relative_strength_pct', 'gap_pct'):
        if source.get(k) is not None:
            f[k] = -float(source[k])
    if 'above_vwap' in source:
        f['above_vwap'] = not bool(source.get('above_vwap'))
    f['breakout'] = bool(source.get('breakout_down', False))
    f['failed_breakout'] = bool(source.get('failed_breakdown', False))
    trend = str(source.get('recent_trend') or '').lower()
    f['recent_trend'] = {'rising': 'falling', 'falling': 'rising'}.get(trend, trend)
    rsi = _safe_float(source.get('rsi'))
    if rsi is not None: f['rsi'] = 100.0 - rsi
    macd_hist = _safe_float(source.get('macd_hist'))
    if macd_hist is not None: f['macd_hist'] = -macd_hist
    momentum = str(source.get('momentum_state') or '').lower()
    f['momentum_state'] = {'accelerating':'accelerating', 'decelerating':'decelerating', 'exhausted':'exhausted'}.get(momentum, momentum)
    return f


def evaluate_all_strategies(features: Dict[str, Any], settings: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Evaluate every base strategy in BOTH directions.

    Long and short share the same family definitions, but each receives a direction-
    symmetric feature view.  No side is silently discarded at strategy level.
    """
    out: List[Dict[str, Any]] = []
    for spec in STRATEGIES:
        for side in ('LONG', 'SHORT'):
            try:
                view = features if side == 'LONG' else _mirror_features_for_short(features)
                sig = spec['fn'](view, settings or {}).to_dict()
                sig['base_strategy_id'] = spec['id']
                sig['side'] = side
                sig['strategy_id'] = spec['id'] if side == 'LONG' else f"{spec['id']}_short"
                sig['name'] = spec['name'] if side == 'LONG' else f"{spec['name']} Short"
                out.append(sig)
            except Exception as e:
                sid = spec['id'] if side == 'LONG' else f"{spec['id']}_short"
                out.append({'strategy_id': sid, 'base_strategy_id': spec['id'], 'side': side,
                            'name': spec['name'] if side == 'LONG' else f"{spec['name']} Short",
                            'eligible': False, 'confidence': 0.0, 'reasons': [],
                            'blocks': [f'evaluation_error: {e}'], 'target_pct': None,
                            'stop_pct': None, 'min_confidence': 55.0})
    return out


# ---------------------------------------------------------------------------
# LAYER 2 -- strategy selector
# ---------------------------------------------------------------------------

def _historical_edge(perf: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Turn a strategy's recorded track record into a 0-100 'edge' number, honestly
    flagging when there isn't enough evidence to have an opinion.

    Uses profit factor and expectancy rather than win rate alone, because a 70%
    win rate with oversized losers is worse than a 55% win rate with controlled
    ones -- which is exactly the trap a win-rate-only system falls into.
    """
    if not perf:
        return {'edge': NEUTRAL_PRIOR_EDGE, 'proven': False, 'note': 'no recorded trades yet -- unproven'}
    trades = int(perf.get('total_trades') or 0)
    pf = _safe_float(perf.get('profit_factor'))
    expectancy = _safe_float(perf.get('expectancy_pct_per_trade'))
    win_rate = _safe_float(perf.get('win_rate_pct')) or 0.0
    # Profit factor is the primary driver; 1.0 (breakeven) maps to the neutral 50.
    if pf is None:
        raw_edge = win_rate
    else:
        raw_edge = 50.0 + (pf - 1.0) * 45.0
    if expectancy is not None:
        raw_edge += max(-12.0, min(12.0, expectancy * 18.0))
    if trades < MIN_TRADES_FOR_EDGE:
        # GRADUATED COLD-START (report #2/#3): blend the observed (noisy) edge toward
        # the neutral prior in proportion to how little evidence backs it, rather than
        # a hard binary cutoff that scores 2 trades and 29 trades identically.
        weight = _evidence_weight_below_min_trades(trades)
        blended_edge = NEUTRAL_PRIOR_EDGE * (1 - weight) + raw_edge * weight
        return {'edge': _clamp(blended_edge), 'proven': False,
                'note': (f'only {trades} recorded trades (need {MIN_TRADES_FOR_EDGE}) -- unproven, blended '
                         f'toward neutral at {int(weight * 100)}% evidence weight')}
    return {'edge': _clamp(raw_edge), 'proven': True,
            'note': f'{trades} trades, win rate {round(win_rate, 1)}%, profit factor {pf}'}


def select_strategy(evaluations: List[Dict[str, Any]], regime: str,
                    performance: Optional[Dict[str, Dict[str, Any]]] = None,
                    disabled_ids: Optional[List[str]] = None,
                    ev_fn: Optional[Any] = None,
                    min_expected_edge_pct: float = 0.0) -> Dict[str, Any]:
    """Pick the ONE strategy to act on, and record why -- including why each of the
    others was passed over. Deterministic: same inputs always give the same choice.

    ARCHITECTURAL FIX (the report's #1 issue -- "the EV gate happens AFTER
    strategy selection"). The old pipeline was:

        10 strategies -> select ONE winner -> EV gate -> maybe kill the candidate

    which meant a strategy with high confidence but proven-negative expectancy
    could win the ranking, fail the EV gate, and take the whole stock down with
    it -- even when a second, slightly less confident strategy with genuinely
    POSITIVE expectancy was sitting right behind it in the ranking and was never
    reconsidered. The pipeline is now:

        10 strategies
            -> compute each one's context-adjusted expected net edge (ev_fn)
            -> REMOVE the ones with proven negative EV (and any the fast
               same-day adaptation layer has suspended)
            -> rank what remains, with expected value as the DOMINANT term
            -> select the strongest positive-EV candidate

    So a negative-EV strategy is eliminated as a CANDIDATE, not as a veto over
    the stock, and selection falls through to the next viable strategy instead of
    stopping. `fallback_used` records when that fallthrough actually happened, so
    the decision log can show "A was dropped on negative EV, B was taken instead"
    rather than silently reporting B as if A never competed.

    ev_fn: optional callable(strategy_id) -> dict. May return any of:
        proven (bool)                  -- is there enough evidence to have an opinion
        expected_net_edge_pct (float)  -- context-adjusted expected NET % per trade
        calibrated_win_prob_pct (float)
        note (str)
        block (bool) / block_reason    -- fast same-day adaptation suspension
        score_penalty (float)          -- soft context penalty, in rank-score points
    Passing None reproduces the previous confidence/regime/edge-only behaviour
    exactly, which is what a cold system with no recorded trades should do.

    Returns the selection plus a full ranking, so both the trade record and the
    dashboard can explain the decision without re-deriving it.
    """
    performance = performance or {}
    disabled = set(disabled_ids or [])
    ranked: List[Dict[str, Any]] = []
    any_proven_ev = False

    for ev in evaluations:
        sid = ev['strategy_id']
        base_sid = ev.get('base_strategy_id') or (sid[:-6] if sid.endswith('_short') else sid)
        spec = STRATEGY_BY_ID.get(base_sid, {})
        regime_fit = float((spec.get('regime_fit') or {}).get(regime, 50))
        side = str(ev.get('side') or ('SHORT' if sid.endswith('_short') else 'LONG')).upper()
        # Side-aware evidence is mandatory for directional statistics.  Do not silently
        # fall back from SHORT to the aggregate/LONG track record.  Aggregated base
        # performance is retained only for genuinely side-neutral, legacy records.
        perf = performance.get(f'{base_sid}:{side}'.lower()) or performance.get(f'{base_sid}:{side}')
        if perf is None and side == 'LONG':
            perf = performance.get(base_sid)
        if perf is None:
            perf = performance.get(sid) or performance.get(sid.lower())
        edge_info = _historical_edge(perf)
        auto_disabled = _is_auto_disabled(perf)

        ev_info: Dict[str, Any] = {}
        if ev_fn is not None:
            try:
                # Pass the direction-specific strategy id. The EV layer and
                # same-day adaptation must never silently substitute LONG evidence
                # for a SHORT candidate.
                ev_info = ev_fn(sid) or {}
            except Exception as e:  # an evidence lookup must never break selection
                ev_info = {'proven': False, 'note': f'ev_evaluation_error: {e}'}
        ev_proven = bool(ev_info.get('proven'))
        ev_pct = _safe_float(ev_info.get('expected_net_edge_pct'))
        if ev_proven and ev_pct is not None:
            any_proven_ev = True
        # Unproven EV maps to the same neutral 50 the historical-edge term uses:
        # no track record is NOT evidence of a bad one, so it must not be scored
        # as though it were.
        ev_score = _clamp(50.0 + ev_pct * EV_SCORE_SLOPE) if (ev_proven and ev_pct is not None) else NEUTRAL_PRIOR_EDGE
        score_penalty = _safe_float(ev_info.get('score_penalty')) or 0.0

        ranked.append({
            'strategy_id': sid, 'base_strategy_id': base_sid, 'side': ev.get('side') or ('SHORT' if sid.endswith('_short') else 'LONG'), 'name': ev.get('name'),
            'confidence': ev.get('confidence'), 'regime_fit': regime_fit,
            'historical_edge': round(edge_info['edge'], 1), 'edge_proven': edge_info['proven'],
            'edge_note': edge_info['note'],
            'expected_net_edge_pct': ev_pct if ev_proven else None,
            'calibrated_win_prob_pct': ev_info.get('calibrated_win_prob_pct') if ev_proven else None,
            'ev_proven': ev_proven, 'ev_note': ev_info.get('note'),
            'ev_score': round(ev_score, 1), 'context_score_penalty': round(score_penalty, 1),
            'session_blocked': bool(ev_info.get('block')),
            'session_block_reason': ev_info.get('block_reason'),
            'eligible': bool(ev.get('eligible')),
            'auto_disabled': auto_disabled,
            'target_pct': ev.get('target_pct'), 'stop_pct': ev.get('stop_pct'),
            'min_confidence': ev.get('min_confidence'),
            'reasons': ev.get('reasons') or [], 'blocks': ev.get('blocks') or [],
        })

    # --- Rank scoring -------------------------------------------------------
    # Weights switch to the evidence-present set only when SOMETHING in this
    # candidate set actually has a proven EV; otherwise every strategy would be
    # ranked off an identical 50.0 placeholder, which adds noise, not information.
    for r in ranked:
        if any_proven_ev:
            final = (WEIGHT_CONFIDENCE_EV * float(r.get('confidence') or 0)
                     + WEIGHT_REGIME_FIT_EV * r['regime_fit']
                     + WEIGHT_HISTORICAL_EDGE_EV * float(r['historical_edge'])
                     + WEIGHT_EXPECTED_VALUE_EV * float(r['ev_score']))
        else:
            final = (WEIGHT_CONFIDENCE * float(r.get('confidence') or 0)
                     + WEIGHT_REGIME_FIT * r['regime_fit']
                     + WEIGHT_HISTORICAL_EDGE * float(r['historical_edge']))
        final += r['context_score_penalty']
        r['final_rank_score'] = round(final, 1)
        r['rank_weighting'] = 'expected_value_weighted' if any_proven_ev else 'setup_quality_weighted'

    # --- Candidate elimination (BEFORE ranking decides anything) -------------
    ev_rejected: List[Dict[str, Any]] = []
    for r in ranked:
        sid = r['strategy_id']
        base_sid = r.get('base_strategy_id') or (sid[:-6] if sid.endswith('_short') else sid)
        reason = None
        ev_rejected_here = False
        if not r['eligible']:
            reason = '; '.join(r['blocks'] or ['setup conditions not met'])
        elif sid in disabled or base_sid in disabled:
            reason = 'manually disabled'
        elif r['auto_disabled']:
            reason = f'auto-benched on poor measured performance ({r["edge_note"]})'
        elif r['session_blocked']:
            reason = f'fast_adaptation_suspended: {r["session_block_reason"]}'
        elif r['ev_proven'] and r['expected_net_edge_pct'] is not None and r['expected_net_edge_pct'] <= min_expected_edge_pct:
            ev_rejected_here = True
            reason = (f'negative_expected_net_edge: expected {r["expected_net_edge_pct"]}%/trade net of costs '
                      f'(needs > {min_expected_edge_pct}%). {r.get("ev_note") or ""}').strip()
        r['ev_rejected'] = ev_rejected_here
        r['selectable'] = reason is None
        r['not_selected_reason'] = reason
        if ev_rejected_here:
            ev_rejected.append({'strategy_id': sid, 'base_strategy_id': base_sid, 'side': r.get('side'), 'name': r['name'],
                                'expected_net_edge_pct': r['expected_net_edge_pct'],
                                'confidence': r['confidence'], 'reason': reason})

    ranked.sort(key=lambda r: (r['selectable'], r['final_rank_score']), reverse=True)
    winner = next((r for r in ranked if r['selectable']), None)

    # Did eliminating negative-EV candidates change the answer? "Would have won
    # on confidence alone" is computed against the ELIGIBLE set only -- a strategy
    # that never produced a valid setup was never in the running to begin with.
    eligible_ranked = [r for r in ranked if r['eligible']]
    top_by_confidence = max(eligible_ranked, key=lambda r: float(r.get('confidence') or 0), default=None)

    if winner is None:
        why = 'no strategy produced a valid setup for this stock at this moment'
        if ev_rejected:
            why = ('every strategy that DID produce a valid setup here has proven negative expected value after '
                   'costs: ' + '; '.join(f'{x["strategy_id"]} ({x["expected_net_edge_pct"]}%/trade)' for x in ev_rejected))
        return {'selected': None, 'regime': regime, 'ranking': ranked,
                'ev_rejected_strategies': ev_rejected,
                'rank_weighting': 'expected_value_weighted' if any_proven_ev else 'setup_quality_weighted',
                'why_not_any': why}

    fallback_used = bool(ev_rejected) and any(
        x['strategy_id'] != winner['strategy_id'] for x in ev_rejected)
    displaced = (top_by_confidence is not None
                 and top_by_confidence['strategy_id'] != winner['strategy_id']
                 and not top_by_confidence['selectable'])

    runners_up = [r for r in ranked if r['strategy_id'] != winner['strategy_id']][:4]
    result = {
        'selected': winner,
        'strategy_id': winner['strategy_id'],
        'base_strategy_id': winner.get('base_strategy_id'),
        'side': winner.get('side'),
        'strategy_name': winner['name'],
        'regime': regime,
        'confidence': winner['confidence'],
        'final_rank_score': winner['final_rank_score'],
        'expected_net_edge_pct': winner.get('expected_net_edge_pct'),
        'calibrated_win_prob_pct': winner.get('calibrated_win_prob_pct'),
        'ev_proven': winner.get('ev_proven'),
        'ev_note': winner.get('ev_note'),
        'rank_weighting': winner.get('rank_weighting'),
        'target_pct': winner['target_pct'],
        'stop_pct': winner['stop_pct'],
        'min_confidence': winner['min_confidence'],
        'why_selected': winner['reasons'],
        'ev_rejected_strategies': ev_rejected,
        'fallback_used': fallback_used or displaced,
        'why_others_not': [{'strategy_id': r['strategy_id'], 'name': r['name'],
                            'reason': r['not_selected_reason'] or f'lower rank score ({r["final_rank_score"]} vs {winner["final_rank_score"]})'}
                           for r in runners_up],
        'ranking': ranked,
    }
    if displaced and top_by_confidence is not None:
        result['fallback_note'] = (
            f'{top_by_confidence["strategy_id"]} had the highest raw setup confidence '
            f'({top_by_confidence["confidence"]}) but was eliminated '
            f'({top_by_confidence["not_selected_reason"]}). Selection fell through to '
            f'{winner["strategy_id"]} rather than discarding this stock.')
    return result


def _is_auto_disabled(perf: Optional[Dict[str, Any]]) -> bool:
    """Bench a strategy that has proven, on a meaningful sample, that it loses money.
    Requires a LARGER sample than MIN_TRADES_FOR_EDGE -- turning a strategy off is a
    bigger decision than merely down-weighting it."""
    if not perf:
        return False
    trades = int(perf.get('total_trades') or 0)
    if trades < AUTO_DISABLE_MIN_TRADES:
        return False
    pf = _safe_float(perf.get('profit_factor'))
    return pf is not None and pf < AUTO_DISABLE_PROFIT_FACTOR


# ---------------------------------------------------------------------------
# Post-trade analysis
# ---------------------------------------------------------------------------

FAILURE_TYPES = (
    'FALSE_BREAKOUT', 'IMMEDIATE_ADVERSE_ENTRY', 'GAVE_BACK_PROFIT',
    'MOMENTUM_FADE', 'ENTRY_SLIPPAGE', 'STOPPED_IN_NOISE',
    'TIME_EXIT_NO_FOLLOW_THROUGH', 'NORMAL_LOSS',
)


def classify_trade_outcome(trade: Dict[str, Any]) -> Dict[str, Any]:
    """Turn a completed trade into a diagnosis, using MAE/MFE plus the signal-to-fill
    gap. This is what makes "the trade lost" into something actionable.

    The distinction that matters most: did the trade never work (entry problem) or
    did it work and then get given back (management problem)? Those need opposite
    fixes, and guessing between them is how you end up 'fixing' the wrong layer.
    """
    net = _safe_float(trade.get('net_return_pct')) or 0.0
    mfe = _safe_float(trade.get('mfe_pct'))
    mae = _safe_float(trade.get('mae_pct'))
    gap = _safe_float(trade.get('signal_fill_gap_pct')) or 0.0
    exit_reason = str(trade.get('exit_reason') or '')
    hold_min = _safe_float(trade.get('holding_minutes'))

    if net > 0:
        quality = 'GOOD_EXIT'
        if mfe is not None and mfe > net * 2.2:
            quality = 'LEFT_PROFIT_ON_TABLE'
        return {'is_loss': False, 'failure_type': None, 'quality': quality,
                'diagnosis': ('Winner, but it reached roughly '
                              f'{round(mfe, 2)}% before exiting at {round(net, 2)}% -- the exit may be leaving money behind.'
                              if quality == 'LEFT_PROFIT_ON_TABLE' else
                              f'Winner, exited at {round(net, 2)}%.'),
                'suggested_focus': 'exit_management' if quality == 'LEFT_PROFIT_ON_TABLE' else None}

    loss = abs(net)
    failure = 'NORMAL_LOSS'
    focus = None
    diagnosis = f'Loss of {round(loss, 2)}%.'

    if mfe is None or mae is None:
        return {'is_loss': True, 'failure_type': 'NORMAL_LOSS', 'quality': 'UNDIAGNOSABLE',
                'diagnosis': diagnosis + ' MAE/MFE not recorded for this trade, so the cause cannot be determined.',
                'suggested_focus': None}

    # Entry slippage dominates when the fill itself ate a large share of the stop.
    if gap >= 0.15 and gap >= loss * 0.3:
        failure = 'ENTRY_SLIPPAGE'
        focus = 'execution'
        diagnosis = (f'Entered {round(gap, 2)}% above the signal price, which is {round((gap / loss) * 100)}% '
                     f'of the total {round(loss, 2)}% loss. The trade started at a disadvantage before the setup '
                     'had any chance to work.')
    elif mfe >= loss * 0.75:
        failure = 'GAVE_BACK_PROFIT'
        focus = 'exit_management'
        diagnosis = (f'Reached +{round(mfe, 2)}% in our favour before reversing to a {round(loss, 2)}% loss. '
                     'The entry signal worked; the exit/trailing logic gave it back.')
    elif mfe <= 0.1 and mae >= loss * 0.9:
        failure = 'IMMEDIATE_ADVERSE_ENTRY'
        focus = 'entry_signal'
        diagnosis = (f'Never moved in our favour (best was +{round(mfe, 2)}%) and went straight to '
                     f'-{round(mae, 2)}%. The entry was wrong from the first tick -- a wider stop would '
                     'only have produced a bigger loss.')
    elif exit_reason.startswith('TRAILING_STOP') or 'GAVE_BACK' in exit_reason:
        failure = 'GAVE_BACK_PROFIT'
        focus = 'exit_management'
        diagnosis = f'Trailing stop triggered after reaching +{round(mfe, 2)}%.'
    elif 'MOMENTUM' in exit_reason or 'FADE' in exit_reason:
        failure = 'MOMENTUM_FADE'
        focus = 'entry_signal'
        diagnosis = f'Momentum faded after entry (peak +{round(mfe, 2)}%); the move had no follow-through.'
    elif 'FORCE_EXIT' in exit_reason or 'TIME' in exit_reason or 'END_OF_DAY' in exit_reason:
        failure = 'TIME_EXIT_NO_FOLLOW_THROUGH'
        focus = 'entry_signal'
        diagnosis = (f'Never resolved either way (peak +{round(mfe, 2)}%, worst -{round(mae, 2)}%) and was '
                     'closed by the time/session exit. The setup simply did not do anything.')
    elif mfe > 0.1 and mae >= loss * 0.9 and hold_min is not None and hold_min <= 10:
        failure = 'STOPPED_IN_NOISE'
        focus = 'stop_placement'
        diagnosis = (f'Stopped out within {round(hold_min)} minutes after a brief +{round(mfe, 2)}% move. '
                     'The stop may be inside normal noise for this stock.')

    # A breakout strategy that failed adversely is specifically a false breakout --
    # more precise than the generic label, and directly actionable for that strategy.
    if trade.get('strategy_id') in ('opening_breakout', 'range_breakout') and failure in ('IMMEDIATE_ADVERSE_ENTRY', 'MOMENTUM_FADE'):
        failure = 'FALSE_BREAKOUT'
        focus = 'entry_signal'
        diagnosis = (f'Breakout failed to follow through (peak +{round(mfe, 2)}%, worst -{round(mae, 2)}%). '
                     'Price broke the level, the entry fired, and the move did not sustain.')

    return {'is_loss': True, 'failure_type': failure, 'quality': 'LOSS',
            'diagnosis': diagnosis, 'suggested_focus': focus}


def _entry_time_bucket(trade: Dict[str, Any]) -> Optional[str]:
    """Bucket a trade's entry into a 30-minute intraday window, e.g. '09:30-10:00'.

    Accepts a datetime/time object or an ISO-ish string in entry_time. Returns None
    when the trade has no usable entry time, so callers can skip time-of-day analysis
    for it without crashing.
    """
    et = trade.get('entry_time')
    if et is None:
        return None
    try:
        if hasattr(et, 'hour'):
            hh, mm = et.hour, et.minute
        else:
            s = str(et)
            time_part = s.split('T')[-1].split(' ')[-1]
            hh, mm = int(time_part[0:2]), int(time_part[3:5])
        bucket_start_min = (mm // 30) * 30
        end_h, end_m = (hh, bucket_start_min + 30) if bucket_start_min + 30 < 60 else (hh + 1, 0)
        return f'{hh:02d}:{bucket_start_min:02d}-{end_h:02d}:{end_m:02d}'
    except Exception:
        return None


def summarize_failure_patterns(trades: List[Dict[str, Any]], min_occurrences: int = 4) -> List[Dict[str, Any]]:
    """Group classified losses by (strategy, failure type) to find RECURRING weaknesses.

    Deliberately requires repetition before reporting anything: one losing trade is
    noise, and letting a single loss drive a rule change is how a system 'learns'
    itself into nonsense. Nothing here changes any rule -- it only surfaces evidence
    for a human to act on.

    Each group also breaks its failures down by 30-minute entry-time bucket, so a
    pattern like "this strategy's false breakouts cluster in the first 30 minutes"
    is visible directly, not just the strategy-level total.
    """
    groups: Dict[str, Dict[str, Any]] = {}
    for t in trades:
        cls = t.get('classification') or classify_trade_outcome(t)
        if not cls.get('is_loss') or not cls.get('failure_type'):
            continue
        sid = t.get('strategy_id') or 'unattributed'
        key = f'{sid}::{cls["failure_type"]}'
        g = groups.setdefault(key, {'strategy_id': sid, 'failure_type': cls['failure_type'],
                                    'count': 0, 'total_loss_pct': 0.0,
                                    'suggested_focus': cls.get('suggested_focus'), 'examples': [],
                                    'time_buckets': {}})
        g['count'] += 1
        g['total_loss_pct'] += abs(_safe_float(t.get('net_return_pct')) or 0.0)
        bucket = _entry_time_bucket(t)
        if bucket:
            g['time_buckets'][bucket] = g['time_buckets'].get(bucket, 0) + 1
        if len(g['examples']) < 3:
            g['examples'].append({'ticker': t.get('ticker'), 'net_return_pct': t.get('net_return_pct'),
                                  'mfe_pct': t.get('mfe_pct'), 'mae_pct': t.get('mae_pct')})

    out = [g for g in groups.values() if g['count'] >= min_occurrences]
    for g in out:
        g['avg_loss_pct'] = round(g['total_loss_pct'] / g['count'], 3)
        g['total_loss_pct'] = round(g['total_loss_pct'], 3)
        g['recommendation'] = _recommendation_for(g['failure_type'], g['strategy_id'], g['count'])
        # Sorted list form for the UI/API: [{bucket, count}, ...], busiest window first.
        # A single bucket holding most of the group's failures is exactly the
        # "concentrated in the first 30 minutes" signal worth acting on.
        buckets_sorted = sorted(g['time_buckets'].items(), key=lambda kv: kv[1], reverse=True)
        g['time_of_day'] = [{'bucket': b, 'count': c} for b, c in buckets_sorted]
        if buckets_sorted and buckets_sorted[0][1] >= max(3, int(g['count'] * 0.5)):
            top_bucket, top_count = buckets_sorted[0]
            g['recommendation'] += (f' {top_count} of {g["count"]} ({round(top_count / g["count"] * 100)}%) '
                                     f'occurred in the {top_bucket} window -- consider restricting this '
                                     f'strategy/failure combination away from that time of day.')
        del g['time_buckets']
    out.sort(key=lambda g: g['total_loss_pct'], reverse=True)
    return out


def _recommendation_for(failure_type: str, strategy_id: str, count: int) -> str:
    """A concrete, testable hypothesis -- explicitly framed as something to BACKTEST,
    never as a change to apply automatically."""
    base = {
        'FALSE_BREAKOUT': 'Test requiring stronger breakout follow-through (more sustained closes above the level, or a higher volume multiple) before entry.',
        'IMMEDIATE_ADVERSE_ENTRY': 'Test a stricter entry confirmation -- this strategy is firing before the move is actually confirmed. Widening the stop would NOT help.',
        'GAVE_BACK_PROFIT': 'Test tighter profit protection (earlier breakeven stop or a tighter trail). The entries are working; the exits are not.',
        'MOMENTUM_FADE': 'Test requiring momentum to still be accelerating at the moment of entry, not merely to have been strong earlier.',
        'ENTRY_SLIPPAGE': 'Test a tighter max signal-to-fill gap. These losses are substantially execution cost, not strategy failure.',
        'STOPPED_IN_NOISE': 'Test a volatility-scaled stop (e.g. ATR-based) instead of a fixed percentage for this strategy.',
        'TIME_EXIT_NO_FOLLOW_THROUGH': 'Test a stricter setup quality bar -- these trades never developed in either direction.',
        'NORMAL_LOSS': 'No specific pattern identified; monitor.',
    }.get(failure_type, 'Investigate further.')
    return f'{strategy_id}: seen {count} times. {base} Backtest the change before enabling it live.'


# ---------------------------------------------------------------------------
# FAST ADAPTATION -- the same-day layer (report issue #5)
# ---------------------------------------------------------------------------
# The slow layer already existed: measured expectancy across days feeds
# _historical_edge / _is_auto_disabled / classify_strategy_health, and in Replay
# that evidence snapshot is built once per simulated day from trades that closed
# BEFORE that day. That is correct point-in-time behaviour and it is what stops
# look-ahead leakage -- but it also means five consecutive losses between 09:30
# and 09:50 change absolutely nothing until tomorrow. The strategy keeps being
# selected all day on a snapshot taken before any of those losses existed.
#
# This is the missing second layer. It reads ONLY trades that have already closed
# TODAY (so it is still strictly point-in-time -- it cannot see a trade that has
# not happened yet) and reacts immediately:
#
#     recent failure pattern  ->  timeout / bench / reduce risk
#
# Two deliberate asymmetries:
#   1. It only ever REDUCES exposure. A strategy cannot win its way into larger
#      size intraday off a two-trade hot streak -- that is noise-chasing, and it
#      is the exact failure mode the whole evidence-gated design exists to avoid.
#   2. It is separate from, and does not overwrite, the slow layer. Fast
#      adaptation expires at the end of the session; only validated multi-day
#      expectancy changes a strategy's standing evidence.

def assess_session_adaptation(session_trades: List[Dict[str, Any]], strategy_id: Optional[str],
                              now: Any = None, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """What should THIS strategy be allowed to do right now, given only what has
    already closed today?

    session_trades: trades closed today so far, each needing at least
        {'strategy_id', 'net_pnl' or 'net_return_pct', 'exit_time'}.
    now: current (or simulated) instant, used for the timeout window.

    Returns {'block', 'block_reason', 'score_penalty', 'risk_multiplier', 'notes',
             'losses_today', 'loss_streak', 'net_pnl_today'}.
    """
    cfg = config or {}
    loss_streak_limit = int(cfg.get('loss_streak', FAST_ADAPT_LOSS_STREAK))
    timeout_minutes = float(cfg.get('timeout_minutes', FAST_ADAPT_TIMEOUT_MINUTES))
    max_losses = int(cfg.get('max_losses_per_day', FAST_ADAPT_MAX_LOSSES_PER_DAY))
    penalty_per_loss = float(cfg.get('loss_penalty_per_loss', FAST_ADAPT_LOSS_PENALTY_PER_LOSS))
    max_penalty = float(cfg.get('max_loss_penalty', FAST_ADAPT_MAX_LOSS_PENALTY))
    risk_step = float(cfg.get('risk_step', FAST_ADAPT_RISK_STEP))

    out: Dict[str, Any] = {'block': False, 'block_reason': None, 'score_penalty': 0.0,
                           'risk_multiplier': 1.0, 'notes': [], 'losses_today': 0,
                           'loss_streak': 0, 'net_pnl_today': 0.0}
    if not strategy_id or not session_trades:
        return out

    mine = [t for t in session_trades if t.get('strategy_id') == strategy_id]
    if not mine:
        return out
    mine = sorted(mine, key=lambda t: t.get('exit_time') or 0, reverse=True)

    def _is_loss(t: Dict[str, Any]) -> bool:
        v = _safe_float(t.get('net_pnl'))
        if v is None:
            v = _safe_float(t.get('net_return_pct'))
        return (v or 0.0) <= 0

    losses_today = sum(1 for t in mine if _is_loss(t))
    net_pnl_today = sum(_safe_float(t.get('net_pnl')) or 0.0 for t in mine)
    streak = 0
    for t in mine:
        if _is_loss(t):
            streak += 1
        else:
            break
    out['losses_today'] = losses_today
    out['loss_streak'] = streak
    out['net_pnl_today'] = round(net_pnl_today, 2)

    # (a) Benched for the rest of the day.
    if max_losses > 0 and losses_today >= max_losses:
        out['block'] = True
        out['block_reason'] = (f'{losses_today} losing trade(s) already today on this strategy '
                               f'(limit {max_losses}) -- benched for the rest of the session.')
        out['risk_multiplier'] = 0.0
        return out

    # (b) Timed out after a losing streak. Expires on its own, so a strategy that
    # was simply caught by one bad stretch of tape is not written off for the day.
    if loss_streak_limit > 0 and streak >= loss_streak_limit:
        last_exit = mine[0].get('exit_time')
        minutes_since = None
        try:
            if last_exit is not None and now is not None:
                delta = now - last_exit
                minutes_since = abs(delta.total_seconds()) / 60.0
        except Exception:
            minutes_since = None
        if minutes_since is None or minutes_since < timeout_minutes:
            remaining = round(timeout_minutes - minutes_since, 1) if minutes_since is not None else timeout_minutes
            out['block'] = True
            out['block_reason'] = (f'{streak} consecutive losses today on this strategy -- timed out for '
                                   f'another ~{remaining} min before it may be selected again.')
            out['risk_multiplier'] = 0.0
            return out
        out['notes'].append(f'Loss-streak timeout has expired ({round(minutes_since)} min since the last loss); '
                            f'allowed again but on reduced size.')
        out['risk_multiplier'] = min(out['risk_multiplier'], risk_step)

    # (c) Still allowed, but bleeding -- smaller size and a score handicap so it
    # has to be visibly better than the alternatives to keep getting picked.
    if losses_today > 0:
        out['score_penalty'] = -round(min(max_penalty, penalty_per_loss * losses_today), 1)
        out['notes'].append(f'{losses_today} loss(es) already today on this strategy '
                            f'({out["score_penalty"]} rank points).')
    if net_pnl_today < 0:
        out['risk_multiplier'] = min(out['risk_multiplier'], risk_step)
        out['notes'].append(f'Net -{abs(round(net_pnl_today, 2))} on this strategy today -- risk halved for '
                            f'any further entry it wins.')
    return out


def assess_session_risk_state(session_trades: List[Dict[str, Any]], capital: float,
                              derisk_pct: float = 1.0, stop_pct: float = 2.0) -> Dict[str, Any]:
    """Portfolio-level (not per-strategy) same-day state: how much of today's
    capital has already been lost, and what that should do to further entries.

    Separate from the per-strategy assessment above because a session can bleed
    out across ten different strategies without any single one crossing its own
    limit -- and the account only has one balance.
    """
    out = {'net_pnl_today': 0.0, 'drawdown_pct': 0.0, 'risk_multiplier': 1.0,
           'stop_entries': False, 'note': None}
    if not session_trades or not capital or capital <= 0:
        return out
    net = sum(_safe_float(t.get('net_pnl')) or 0.0 for t in session_trades)
    out['net_pnl_today'] = round(net, 2)
    if net >= 0:
        return out
    dd_pct = (abs(net) / float(capital)) * 100.0
    out['drawdown_pct'] = round(dd_pct, 3)
    if stop_pct > 0 and dd_pct >= stop_pct:
        out['stop_entries'] = True
        out['risk_multiplier'] = 0.0
        out['note'] = (f'Session drawdown {round(dd_pct, 2)}% of capital has reached the daily stop '
                       f'({stop_pct}%) -- no further entries today.')
    elif derisk_pct > 0 and dd_pct >= derisk_pct:
        out['risk_multiplier'] = 0.5
        out['note'] = (f'Session drawdown {round(dd_pct, 2)}% of capital is past the de-risk line '
                       f'({derisk_pct}%) -- remaining entries sized at half risk.')
    return out
