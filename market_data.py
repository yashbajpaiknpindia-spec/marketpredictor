"""
Drop-in replacement for the parts of `yfinance` this app actually uses,
backed by the INDstocks API instead of Yahoo Finance.

app.py imports this as `import market_data as yf`, so `yf.Ticker(...)` and
`yf.download(...)` keep working with the same call signatures. Two things
are deliberately NOT replicated (per the India-only, equity-only, no-
fundamentals/no-news migration this app went through):

  - `Ticker.info` (fundamentals) — not implemented. Code that calls it
    (wrapped in try/except upstream) will get an AttributeError, which is
    caught and degrades to an empty fundamentals dict, same as yfinance
    returning nothing.
  - `Ticker.news` — not implemented, same graceful-degradation story; the
    app's own get_recent_news() already wraps this in try/except and
    returns [] on failure.

Anything asking for a ticker this module can't resolve (unknown symbol, or
a non-India index like '^VIX'/'^GSPC' left over from now-unused US/Global
code paths) gets back an EMPTY pandas DataFrame rather than an exception —
matching how yfinance itself behaves for a bad/delisted ticker, and keeping
every existing `if hist is None or hist.empty` check in app.py working
unmodified.
"""
import datetime
import re
import pandas as pd

import indstocks_client as ind

# Pseudo-tickers app.py already uses for market-wide context (VIX, Nifty
# trend) get mapped to real INDstocks index names here. Anything not listed
# (old US/Global tickers like '^VIX', '^GSPC', 'ACWI', 'SPY') is left
# unresolved on purpose — those code paths are unreachable now that the app
# is India-only, and will just gracefully return "no data".
INDEX_ALIASES = {
    "^NSEI": "NIFTY 50",
    "^NSEBANK": "NIFTY BANK",
    "^INDIAVIX": "INDIA VIX",
    # Sectoral indices (report fix #8 -- sector-relative strength). Pseudo-tickers
    # match sector_data.SECTOR_INDEX_TICKER. If INDstocks doesn't resolve one of
    # these index names, resolve_index_scrip_code's own failure path is unchanged --
    # this app already treats an unresolved index as "no data" everywhere else
    # (see this module's docstring), so a bad name here degrades gracefully rather
    # than breaking anything.
    "^NIFTYBANKIDX": "NIFTY BANK",
    "^NIFTYIT": "NIFTY IT",
    "^NIFTYAUTO": "NIFTY AUTO",
    "^NIFTYPHARMA": "NIFTY PHARMA",
    "^NIFTYFMCG": "NIFTY FMCG",
    "^NIFTYMETAL": "NIFTY METAL",
    "^NIFTYENERGY": "NIFTY ENERGY",
    "^NIFTYFINSERVICE": "NIFTY FIN SERVICE",
    "^NIFTYREALTY": "NIFTY REALTY",
    "^NIFTYMEDIA": "NIFTY MEDIA",
    "^NIFTYPSUBANK": "NIFTY PSU BANK",
    "^NIFTYINFRA": "NIFTY INFRA",
}

_PERIOD_TO_DAYS = {
    "1d": 1, "2d": 2, "5d": 5, "7d": 7, "10d": 10, "15d": 15,
    "1mo": 31, "3mo": 93, "6mo": 186,
    "1y": 365, "2y": 730, "5y": 1825, "10y": 3650,
    "max": 3650,
}


def _resolve_scrip(ticker: str) -> str:
    if ticker in INDEX_ALIASES:
        return ind.resolve_index_scrip_code(INDEX_ALIASES[ticker])
    return ind.resolve_equity_scrip_code(ticker)


def _period_to_start(period, end_dt):
    key = (period or "1y").strip()
    if key in _PERIOD_TO_DAYS:
        return end_dt - datetime.timedelta(days=_PERIOD_TO_DAYS[key])
    # Caller passed a period string this table doesn't have an exact entry for
    # (e.g. a dynamically-built "43d"). Parse it instead of silently falling back
    # to a 365-day window -- that fallback previously caused callers requesting a
    # specific short window to get a full year of data anchored at *today*, which
    # is both wasteful and wrong whenever the intended window wasn't ending today.
    m = re.fullmatch(r"(\d+)\s*d(ay)?s?", key, re.IGNORECASE)
    if m:
        return end_dt - datetime.timedelta(days=int(m.group(1)))
    m = re.fullmatch(r"(\d+)\s*wk?s?", key, re.IGNORECASE)
    if m:
        return end_dt - datetime.timedelta(weeks=int(m.group(1)))
    m = re.fullmatch(r"(\d+)\s*mo(nth)?s?", key, re.IGNORECASE)
    if m:
        return end_dt - datetime.timedelta(days=int(m.group(1)) * 31)
    m = re.fullmatch(r"(\d+)\s*y(ea)?r?s?", key, re.IGNORECASE)
    if m:
        return end_dt - datetime.timedelta(days=int(m.group(1)) * 365)
    return end_dt - datetime.timedelta(days=365)


def _parse_dt(value, default):
    if not value:
        return default
    if isinstance(value, datetime.datetime):
        return value
    try:
        return datetime.datetime.fromisoformat(str(value))
    except ValueError:
        return default


IST_OFFSET = datetime.timezone(datetime.timedelta(hours=5, minutes=30))


def _candles_to_df(candles) -> pd.DataFrame:
    cols = ["Open", "High", "Low", "Close", "Volume"]
    if not candles:
        return pd.DataFrame(columns=cols)
    idx = [
        datetime.datetime.fromtimestamp(c["ts"], tz=IST_OFFSET).replace(tzinfo=None)
        for c in candles
    ]
    df = pd.DataFrame(
        {
            "Open": [c.get("o") for c in candles],
            "High": [c.get("h") for c in candles],
            "Low": [c.get("l") for c in candles],
            "Close": [c.get("c") for c in candles],
            "Volume": [c.get("v") for c in candles],
        },
        index=pd.DatetimeIndex(idx, name="Date"),
    )
    return df[~df.index.duplicated(keep="last")].sort_index()


def history_batch(tickers, period=None, interval="1d", start=None, end=None, timeout=None, **kwargs):
    """Fetch intraday/daily history for many tickers at once, batching by
    INDstocks' 5-scrip-per-call limit (see indstocks_client.MAX_SCRIPS_PER_HISTORICAL_CALL)
    instead of issuing one HTTP round trip per ticker like Ticker.history() does.

    Returns {ticker: DataFrame}, one entry per input ticker (empty DataFrame for
    any ticker that fails to resolve or has no data), same shape as calling
    Ticker(t).history(...) for each t individually.
    """
    ticker_list = list(dict.fromkeys(tickers))  # de-dupe, keep order
    end_dt = _parse_dt(end, datetime.datetime.now())
    start_dt = _parse_dt(start, None) or _period_to_start(period, end_dt)

    scrip_map = {}
    for t in ticker_list:
        try:
            scrip_map[t] = _resolve_scrip(t)
        except Exception as e:
            print(f"[market_data] could not resolve '{t}': {e}")

    result = {t: pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"]) for t in ticker_list}
    if not scrip_map:
        return result

    try:
        raw = ind.get_historical(list(scrip_map.values()), interval, start_dt, end_dt)
    except Exception as e:
        print(f"[market_data] batch history fetch failed: {e}")
        return result

    for t, scrip in scrip_map.items():
        result[t] = _candles_to_df(raw.get(scrip, []))
    return result


class Ticker:
    """Mimics yfinance.Ticker for the subset of its API this app calls:
    .history(period=, interval=, start=, end=, timeout=, ...) — everything
    else (`auto_adjust`, `actions`, `threads`, etc.) is accepted for call-
    signature compatibility and silently ignored, since INDstocks returns
    raw OHLC only and has no corporate-action/split feed in its docs."""

    def __init__(self, ticker: str):
        self.ticker = ticker

    def history(self, period=None, interval="1d", start=None, end=None, timeout=None, **kwargs):
        end_dt = _parse_dt(end, datetime.datetime.now())
        start_dt = _parse_dt(start, None) or _period_to_start(period, end_dt)
        try:
            scrip = _resolve_scrip(self.ticker)
            raw = ind.get_historical([scrip], interval, start_dt, end_dt)
            return _candles_to_df(raw.get(scrip, []))
        except Exception as e:
            print(f"[market_data] history({self.ticker}) failed: {e}")
            return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])


def download(tickers, period="1y", interval="1d", group_by="ticker", auto_adjust=False,
             threads=True, progress=False, timeout=None, **kwargs):
    """Mimics yfinance.download(...) for the multi-ticker batch case used by
    download_universe_snapshots(). Returns a DataFrame with MultiIndex
    columns (ticker, field) when downloading more than one ticker, or a
    flat single-level-column DataFrame for exactly one ticker — matching
    yfinance's own behavior, which app.py's extract_ticker_frame() already
    branches on."""
    ticker_list = tickers.split() if isinstance(tickers, str) else list(tickers)
    end_dt = datetime.datetime.now()
    start_dt = _period_to_start(period, end_dt)

    scrip_map = {}
    for t in ticker_list:
        try:
            scrip_map[t] = _resolve_scrip(t)
        except Exception as e:
            print(f"[market_data] could not resolve '{t}': {e}")

    valid_tickers = list(scrip_map.keys())
    empty_cols = ["Open", "High", "Low", "Close", "Volume"]
    if not valid_tickers:
        return pd.DataFrame(columns=pd.MultiIndex.from_product([[], empty_cols]))

    try:
        raw = ind.get_historical(list(scrip_map.values()), interval, start_dt, end_dt)
    except Exception as e:
        print(f"[market_data] batch historical fetch failed: {e}")
        raw = {}

    frames = {t: _candles_to_df(raw.get(scrip_map[t], [])) for t in valid_tickers}

    if len(valid_tickers) == 1:
        # yfinance returns flat (non-MultiIndex) columns for a single ticker.
        return frames[valid_tickers[0]]

    non_empty = {t: df for t, df in frames.items() if not df.empty}
    if not non_empty:
        return pd.DataFrame(columns=pd.MultiIndex.from_product([[], empty_cols]))
    return pd.concat(non_empty, axis=1)
