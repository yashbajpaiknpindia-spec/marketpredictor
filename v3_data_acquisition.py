"""V3.1 historical-data acquisition and validation adapters.

The engine never fabricates 1-minute candles. This module can ingest genuine 1m
CSV/CSV.GZ/Parquet files or fetch 1m candles from supported authenticated APIs when
credentials are configured by the operator. It normalizes into a canonical schema
and can derive completed 5m structural bars from genuine 1m input.
"""
from __future__ import annotations

import os
import json
import time
import hashlib
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

IST = "Asia/Kolkata"
CANONICAL = ["day", "symbol", "timestamp", "open", "high", "low", "close", "volume", "epoch"]


@dataclass
class AcquisitionSource:
    name: str
    type: str
    url: str
    granularity: str
    coverage: str
    authentication: str
    notes: str


@dataclass
class MinuteAudit:
    rows: int
    symbols: int
    sessions: int
    min_day: str
    max_day: str
    duplicate_keys: int
    invalid_ohlc: int
    negative_volume: int
    expected_regular_minutes: int
    sessions_with_full_regular_day: int
    status: str
    source_sha256: str = ""


def source_catalog() -> Dict[str, Dict[str, Any]]:
    return {
        "dhan_v2": {
            "name": "DhanHQ V2 Historical Data",
            "type": "authenticated_api",
            "url": "https://dhanhq.co/docs/v2/historical-data/",
            "granularity": "1m",
            "coverage": "up to 5 years; 90 calendar days/request for minute data",
            "authentication": "Dhan access token",
            "notes": "Best direct route for exact 2026 historical 1m equity data when credentials are available.",
        },
        "upstox_v3": {
            "name": "Upstox Historical Candle V3",
            "type": "authenticated_api",
            "url": "https://upstox.com/developer/api-documentation/v3/get-historical-candle-data/",
            "granularity": "1m",
            "coverage": "from Jan 2022; 1-15m queries limited to one month per request",
            "authentication": "Upstox bearer token",
            "notes": "Suitable for exact-date 2026 retrieval with one-month chunking.",
        },
        "public_hf": {
            "name": "Indian Stock Market Minute Data (Hugging Face)",
            "type": "public_dataset",
            "url": "https://huggingface.co/datasets/rahulkrraj/indian-stock-market-minute-data",
            "granularity": "1m",
            "coverage": "2022-2026; 2,500+ NSE stocks/indices; large multi-GB dataset",
            "authentication": "none stated",
            "notes": "Useful for offline research/backfill; dataset notes recommend independent verification before live use.",
        },
        "public_github": {
            "name": "NSE F&O Underlyings 1-Min Dataset",
            "type": "public_dataset",
            "url": "https://github.com/voletiramu/nse-fno-1min-data",
            "granularity": "1m",
            "coverage": "2024-04-01 through 2026-04-30; 214 symbols",
            "authentication": "none stated",
            "notes": "Useful for historical regime research, but does not overlap the current Jun-Sep 2026 test window.",
        },
    }


def _read_one(path: Path) -> pd.DataFrame:
    suf = ''.join(path.suffixes).lower()
    if suf.endswith('.parquet'):
        df = pd.read_parquet(path)
    elif suf.endswith('.csv.gz') or suf.endswith('.gz'):
        df = pd.read_csv(path, compression='gzip')
    elif suf.endswith('.csv'):
        df = pd.read_csv(path)
    else:
        raise ValueError(f"Unsupported data file: {path}")
    return df


def load_local_minute(path: str | Path, symbols: Optional[Iterable[str]] = None,
                      start: Optional[str] = None, end: Optional[str] = None) -> pd.DataFrame:
    root = Path(path)
    files = [root] if root.is_file() else sorted([p for p in root.rglob('*') if p.is_file() and (str(p).lower().endswith(('.csv','.csv.gz','.parquet')))])
    if not files:
        raise FileNotFoundError(f"No CSV/CSV.GZ/Parquet files found under {root}")
    wanted = {str(s).upper().replace('.NS','').replace('.BO','') for s in symbols} if symbols else None
    parts=[]
    for p in files:
        try:
            d=_read_one(p)
        except Exception:
            continue
        lower={str(c).lower():c for c in d.columns}
        if not {'open','high','low','close','volume'}.issubset(lower):
            continue
        sym_col=lower.get('symbol') or lower.get('ticker') or lower.get('instrument')
        ts_col=lower.get('timestamp') or lower.get('time') or lower.get('datetime') or lower.get('date')
        if sym_col is None and root.is_file():
            # single-symbol files may use the filename as the symbol
            d['symbol']=root.stem.split('.')[0].upper(); sym_col='symbol'
        if ts_col is None:
            continue
        out=pd.DataFrame({
            'symbol': d[sym_col].astype(str).str.upper().str.replace('.NS','',regex=False).str.replace('.BO','',regex=False),
            'timestamp': pd.to_datetime(d[ts_col], errors='coerce', utc=True),
            'open': pd.to_numeric(d[lower['open']], errors='coerce'),
            'high': pd.to_numeric(d[lower['high']], errors='coerce'),
            'low': pd.to_numeric(d[lower['low']], errors='coerce'),
            'close': pd.to_numeric(d[lower['close']], errors='coerce'),
            'volume': pd.to_numeric(d[lower['volume']], errors='coerce'),
        })
        if wanted is not None: out=out[out.symbol.isin(wanted)]
        if start is not None: out=out[out.timestamp>=pd.Timestamp(start,tz='UTC')]
        if end is not None: out=out[out.timestamp<=pd.Timestamp(end,tz='UTC')]
        if not out.empty: parts.append(out)
    if not parts: raise ValueError("No compatible minute OHLCV records found")
    out=pd.concat(parts,ignore_index=True)
    out['timestamp']=out.timestamp.dt.tz_convert(IST)
    out=out.dropna(subset=['symbol','timestamp','open','high','low','close']).copy()
    out=out.sort_values(['symbol','timestamp']).drop_duplicates(['symbol','timestamp'],keep='last')
    out['day']=out.timestamp.dt.date.astype(str)
    out['epoch']=out.timestamp.astype('int64')//10**9
    return out[CANONICAL]


def audit_minute(df: pd.DataFrame, source_sha256: str = "") -> MinuteAudit:
    x=df.copy(); x['timestamp']=pd.to_datetime(x.timestamp,errors='coerce')
    valid=x.dropna(subset=['timestamp','open','high','low','close']).copy()
    dup=int(valid.duplicated(['day','symbol','timestamp']).sum()) if 'day' in valid else int(valid.duplicated(['symbol','timestamp']).sum())
    invalid=int(((valid.high < valid[['open','close']].max(axis=1)) | (valid.low > valid[['open','close']].min(axis=1))).sum())
    neg=int((pd.to_numeric(valid.volume,errors='coerce')<0).sum())
    sessions=valid[['day','symbol']].drop_duplicates() if {'day','symbol'}.issubset(valid) else pd.DataFrame()
    counts=sessions.merge(valid.groupby(['day','symbol']).size().rename('rows'),left_on=['day','symbol'],right_index=True,how='left')['rows'] if not sessions.empty else pd.Series(dtype=float)
    full=int((counts==375).sum())
    return MinuteAudit(
        rows=int(len(valid)),symbols=int(valid.symbol.nunique()),sessions=int(valid.day.nunique()),
        min_day=str(valid.day.min()) if len(valid) else '',max_day=str(valid.day.max()) if len(valid) else '',
        duplicate_keys=dup,invalid_ohlc=invalid,negative_volume=neg,expected_regular_minutes=375,
        sessions_with_full_regular_day=full,status='PASS' if dup==0 and invalid==0 and neg==0 else 'FAIL',source_sha256=source_sha256,
    )


def resample_completed_5m(minute_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate genuine 1m bars to completed 5m bars; incomplete buckets are dropped."""
    x=minute_df.copy(); x['timestamp']=pd.to_datetime(x.timestamp,utc=True).dt.tz_convert(IST)
    x=x.sort_values(['symbol','timestamp'])
    x=x.set_index('timestamp')
    rows=[]
    for sym,g in x.groupby('symbol',sort=False):
        g=g[['open','high','low','close','volume']]
        five=g.resample('5min',label='left',closed='left').agg({'open':'first','high':'max','low':'min','close':'last','volume':'sum'})
        cnt=g['close'].resample('5min').count()
        five=five[cnt==5].dropna(subset=['open','high','low','close'])
        five=five.reset_index(); five['symbol']=sym; rows.append(five)
    if not rows:return pd.DataFrame(columns=CANONICAL)
    out=pd.concat(rows,ignore_index=True);out['day']=out.timestamp.dt.date.astype(str);out['epoch']=out.timestamp.astype('int64')//10**9
    return out[CANONICAL].sort_values(['day','symbol','timestamp']).reset_index(drop=True)


def sha256_file(path: str | Path, chunk: int = 1024*1024) -> str:
    h=hashlib.sha256()
    with open(path,'rb') as f:
        for b in iter(lambda:f.read(chunk),b''):h.update(b)
    return h.hexdigest()


def download_dhan_intraday(*, security_id: str, from_date: str, to_date: str, interval: int = 1,
                           access_token: Optional[str] = None, exchange_segment: str = 'NSE_EQ',
                           instrument: str = 'EQUITY', timeout: int = 30) -> pd.DataFrame:
    import requests
    token=access_token or os.getenv('DHAN_ACCESS_TOKEN','').strip()
    if not token: raise RuntimeError('DHAN_ACCESS_TOKEN is not configured')
    payload={'securityId':str(security_id),'exchangeSegment':exchange_segment,'instrument':instrument,'interval':str(interval),'oi':False,
             'fromDate':from_date,'toDate':to_date}
    r=requests.post('https://api.dhan.co/v2/charts/intraday',headers={'Accept':'application/json','Content-Type':'application/json','access-token':token},json=payload,timeout=timeout)
    r.raise_for_status(); d=r.json()
    data=d.get('data',d)
    n=len(data.get('timestamp',[]))
    if not n:return pd.DataFrame(columns=CANONICAL)
    out=pd.DataFrame({
        'timestamp':pd.to_datetime(data['timestamp'],unit='s',utc=True).tz_convert(IST),
        'open':data['open'],'high':data['high'],'low':data['low'],'close':data['close'],'volume':data.get('volume',[0]*n)
    })
    out['symbol']=str(security_id);out['day']=out.timestamp.dt.date.astype(str);out['epoch']=out.timestamp.astype('int64')//10**9
    return out[CANONICAL]


def download_upstox_1m(*, instrument_key: str, from_date: str, to_date: str,
                       access_token: Optional[str] = None, timeout: int = 30) -> pd.DataFrame:
    import requests
    token=access_token or os.getenv('UPSTOX_ACCESS_TOKEN','').strip()
    if not token: raise RuntimeError('UPSTOX_ACCESS_TOKEN is not configured')
    url=f"https://api.upstox.com/v3/historical-candle/{instrument_key.replace('|','%7C')}/minutes/1/{to_date}/{from_date}"
    r=requests.get(url,headers={'Content-Type':'application/json','Accept':'application/json','Authorization':f'Bearer {token}'},timeout=timeout)
    r.raise_for_status(); d=r.json()['data']['candles']
    if not d:return pd.DataFrame(columns=CANONICAL)
    out=pd.DataFrame(d,columns=['timestamp','open','high','low','close','volume','oi'])
    out['timestamp']=pd.to_datetime(out.timestamp,utc=True).dt.tz_convert(IST)
    out['symbol']=instrument_key;out['day']=out.timestamp.dt.date.astype(str);out['epoch']=out.timestamp.astype('int64')//10**9
    return out[CANONICAL]


def write_ingest_manifest(path: str | Path, *, source: Dict[str,Any], audit: MinuteAudit,
                          notes: Optional[List[str]]=None) -> Path:
    p=Path(path);p.parent.mkdir(parents=True,exist_ok=True)
    payload={'created_at_utc':pd.Timestamp.utcnow().isoformat(),'source':source,'audit':asdict(audit),'notes':notes or []}
    p.write_text(json.dumps(payload,indent=2,default=str),encoding='utf-8');return p
