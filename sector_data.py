"""
Sector classification for the intraday engine's sector-relative-strength check
(report fix #8 -- "sector context is still missing").

WHY THIS FILE EXISTS: the app's fundamentals path (yf.Ticker(...).info) is not
implemented against the INDstocks backend (see market_data.py's own docstring),
so there was previously NO sector data anywhere in the app -- every candidate's
'sector' field silently reads 'N/A'. Comparing a stock only to NIFTY ("stock vs
index") without a "stock vs its own sector" check misses cases the report calls
out explicitly: a stock at +2.0% while its sector is up +1.2% is a much weaker
relative performer than the same +2.0% against a sector that's DOWN -1.0%.

COVERAGE IS DELIBERATELY PARTIAL AND BEST-EFFORT. This maps the most liquid,
well-known large/mid-cap NSE names into their NIFTY sectoral index -- it does
NOT attempt to classify the full ~500-name trading universe. A ticker missing
from this map simply gets no sector-relative-strength signal (see its use in
app.py's strict_learning_review -- the rule is skipped entirely, never treated
as a penalty or a false neutral). Extend SECTOR_MAP over time as needed; getting
a stock's sector wrong is worse than leaving it unmapped, so additions should be
conservative.

SECTOR_INDEX_TICKER maps each sector name to a pseudo-ticker resolved by
market_data.py's INDEX_ALIASES (added there alongside this file), the same
pattern already used for '^NSEI' -> 'NIFTY 50' and '^NSEBANK' -> 'NIFTY BANK'.
If INDstocks doesn't recognize one of these newer index names, market_data.py's
existing "return empty / no data" behavior degrades gracefully -- exactly like
an unresolvable ticker today -- so a bad index name here cannot crash a scan,
only silently disable that one sector's context (see get_intraday_sector_context
in app.py).
"""

SECTOR_INDEX_TICKER = {
    'Bank': '^NIFTYBANKIDX',          # NIFTY BANK (reuses the existing ^NSEBANK data if preferred)
    'IT': '^NIFTYIT',                 # NIFTY IT
    'Auto': '^NIFTYAUTO',             # NIFTY AUTO
    'Pharma': '^NIFTYPHARMA',         # NIFTY PHARMA
    'FMCG': '^NIFTYFMCG',             # NIFTY FMCG
    'Metal': '^NIFTYMETAL',           # NIFTY METAL
    'Energy': '^NIFTYENERGY',         # NIFTY ENERGY
    'Financial Services': '^NIFTYFINSERVICE',  # NIFTY FIN SERVICE
    'Realty': '^NIFTYREALTY',         # NIFTY REALTY
    'Media': '^NIFTYMEDIA',           # NIFTY MEDIA
    'PSU Bank': '^NIFTYPSUBANK',      # NIFTY PSU BANK
    'Infrastructure': '^NIFTYINFRA',  # NIFTY INFRA
}

# ticker (with the .NS suffix this app uses everywhere) -> sector name (a key of
# SECTOR_INDEX_TICKER above). Grouped by sector for readability/maintainability.
SECTOR_MAP = {}

_BANK = [
    'HDFCBANK', 'ICICIBANK', 'SBIN', 'KOTAKBANK', 'AXISBANK', 'INDUSINDBK',
    'BANKBARODA', 'PNB', 'CANBK', 'IDFCFIRSTB', 'FEDERALBNK', 'AUBANK',
    'BANDHANBNK', 'RBLBANK', 'YESBANK', 'UNIONBANK', 'INDIANB', 'BANKINDIA',
    'CENTRALBK', 'UCOBANK', 'J&KBANK', 'KARURVYSYA', 'CUB', 'MAHABANK',
    'TMB', 'EQUITASBNK', 'CSBBANK', 'IDBI', 'IOB',
]
_PSU_BANK = [
    'SBIN', 'PNB', 'CANBK', 'BANKBARODA', 'UNIONBANK', 'INDIANB', 'BANKINDIA',
    'CENTRALBK', 'UCOBANK', 'MAHABANK', 'IOB', 'IDBI', 'J&KBANK',
]
_IT = [
    'TCS', 'INFY', 'WIPRO', 'HCLTECH', 'TECHM', 'LTIM', 'LTTS', 'COFORGE',
    'PERSISTENT', 'MPHASIS', 'OFSS', 'KPITTECH', 'TATAELXSI', 'CYIENT',
    'MASTEK', 'ZENSARTECH', 'INTELLECT', 'BSOFT', 'HAPPSTMNDS', 'FSL',
    'SONATSOFTW', 'ECLERX', 'LATENTVIEW', 'KFINTECH', 'TANLA', 'ROUTE',
    'DATAPATTNS', 'AFFLE',
]
_AUTO = [
    'MARUTI', 'TATAMOTORS', 'M&M', 'BAJAJ-AUTO', 'HEROMOTOCO', 'EICHERMOT',
    'TVSMOTOR', 'ASHOKLEY', 'BOSCHLTD', 'MRF', 'BALKRISIND', 'APOLLOTYRE',
    'MOTHERSON', 'BHARATFORG', 'EXIDEIND', 'ESCORTS', 'CEATLTD', 'ENDURANCE',
    'SONACOMS', 'UNOMINDA', 'SCHAEFFLER', 'VARROC', 'JBMA', 'ZFCVINDIA',
    'OLECTRA', 'SUNDRMFAST', 'MHRIL',
]
_PHARMA = [
    'SUNPHARMA', 'DRREDDY', 'CIPLA', 'DIVISLAB', 'LUPIN', 'AUROPHARMA',
    'TORNTPHARM', 'ALKEM', 'ZYDUSLIFE', 'BIOCON', 'GLENMARK', 'IPCALAB',
    'LAURUSLABS', 'GRANULES', 'AJANTPHARM', 'NATCOPHARM', 'JBCHEPHARM',
    'ERIS', 'MANKIND', 'ABBOTINDIA', 'SANOFI', 'GLAXO', 'ASTRAZEN', 'FDC',
    'CONCORDBIO', 'SYNGENE', 'SUVENPHAR', 'GLAND', 'ALIVUS', 'APLLTD',
]
_FMCG = [
    'HINDUNILVR', 'ITC', 'NESTLEIND', 'BRITANNIA', 'DABUR', 'MARICO',
    'GODREJCP', 'TATACONSUM', 'COLPAL', 'EMAMILTD', 'VBL', 'UBL', 'RADICO',
    'JYOTHYLAB', 'BIKAJI', 'PATANJALI', 'GILLETTE', 'PGHH', 'HONASA',
    'GODFRYPHLP', 'CCL', 'BAJAJCON',
]
_METAL = [
    'TATASTEEL', 'JSWSTEEL', 'HINDALCO', 'VEDL', 'JINDALSTEL', 'SAIL',
    'NMDC', 'NATIONALUM', 'HINDCOPPER', 'HINDZINC', 'JSL', 'WELCORP',
    'RATNAMANI', 'APLAPOLLO', 'GRAPHITE', 'HEG', 'JINDALSAW', 'GPIL',
]
_ENERGY = [
    'RELIANCE', 'ONGC', 'BPCL', 'IOC', 'GAIL', 'NTPC', 'POWERGRID',
    'COALINDIA', 'OIL', 'ADANIGREEN', 'ADANIPOWER', 'TATAPOWER',
    'JSWENERGY', 'NHPC', 'SJVN', 'TORNTPOWER', 'CESC', 'IGL', 'GUJGASLTD',
    'PETRONET', 'MGL', 'ATGL', 'GSPL', 'ADANIENSOL',
]
_FIN_SERVICES = [
    'BAJFINANCE', 'BAJAJFINSV', 'HDFCLIFE', 'SBILIFE', 'ICICIGI',
    'ICICIPRULI', 'SHRIRAMFIN', 'CHOLAFIN', 'MUTHOOTFIN', 'MANAPPURAM',
    'LICHSGFIN', 'PFC', 'RECLTD', 'HDFCAMC', 'SBICARD', 'JIOFIN',
    'PNBHOUSING', 'POONAWALLA', 'LICI', 'MFSL', 'ANGELONE', 'IIFL',
    'CRISIL', 'CDSL', 'BSE', 'MCX', 'CAMS', 'NUVAMA', 'IRFC', 'ABCAPITAL',
    'SBFC', 'CHOLAHLDNG', 'SAMMAANCAP', 'HUDCO', 'AAVAS', 'HOMEFIRST',
    'CREDITACC', 'FIVESTAR', 'STARHEALTH', 'GICRE', 'NIACL',
]
_REALTY = [
    'DLF', 'GODREJPROP', 'OBEROIRLTY', 'PRESTIGE', 'PHOENIXLTD', 'BRIGADE',
    'SOBHA', 'LODHA', 'MAHLIFE', 'SUNTECK', 'ABREL', 'SIGNATURE',
]
_MEDIA = ['ZEEL', 'SUNTV', 'NETWORK18', 'SAREGAMA', 'PVRINOX']

for _sector_name, _tickers in (
    ('Bank', _BANK), ('PSU Bank', _PSU_BANK), ('IT', _IT), ('Auto', _AUTO),
    ('Pharma', _PHARMA), ('FMCG', _FMCG), ('Metal', _METAL), ('Energy', _ENERGY),
    ('Financial Services', _FIN_SERVICES), ('Realty', _REALTY), ('Media', _MEDIA),
):
    for _t in _tickers:
        # PSU Bank overlaps with the broader Bank list on purpose (a PSU bank IS a
        # bank); PSU Bank is more specific so it wins when both are present.
        if _t not in SECTOR_MAP or _sector_name == 'PSU Bank':
            SECTOR_MAP[f'{_t}.NS'] = _sector_name


def get_sector(ticker: str):
    """Best-effort sector lookup. Returns None (not 'N/A') for anything unmapped,
    so callers can distinguish 'known to have no sector' from 'not looked up yet'
    and skip the sector-relative-strength check cleanly instead of guessing."""
    if not ticker:
        return None
    t = str(ticker).upper()
    if not t.endswith('.NS') and not t.endswith('.BO'):
        t = f'{t}.NS'
    return SECTOR_MAP.get(t)
