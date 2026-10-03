import gc, os, resource
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

N=222
BARS=375
COLS=['Open','High','Low','Close','Volume']

def rss_mb():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024.0

def build(days):
    dates=[datetime(2026,7,1)+timedelta(days=i) for i in range(days)]
    history={}
    for i in range(N):
        t=f'S{i:03d}'
        by_day={}
        for d in dates:
            idx=pd.date_range(d+timedelta(hours=9,minutes=15), periods=BARS, freq='min')
            base=100+i*0.1+np.arange(BARS,dtype=np.float64)*0.005
            arr=np.column_stack([base,base+0.05,base-0.05,base+0.02,np.full(BARS,10000,dtype=np.float64)])
            by_day[d.date()]=pd.DataFrame(arr, index=idx, columns=COLS)
        history[t]=by_day
    # Simulate one day's feature cache retained alongside history.
    feature_cache={}
    target_date=dates[-1].date()
    for t in history:
        df=history[t][target_date]
        c=df['Close'].to_numpy(copy=False)
        feature_cache[t]={
            'last':c.copy(), 'ema9':pd.Series(c).ewm(span=9, adjust=False).mean().to_numpy(),
            'ema21':pd.Series(c).ewm(span=21, adjust=False).mean().to_numpy(),
            'rsi':np.full(BARS,50.0,dtype=np.float64),
            'atr_pct':np.full(BARS,0.5,dtype=np.float64),
        }
    return history, feature_cache

for days in (2,4):
    gc.collect(); history, fc=build(days); print(f'days={days} rss_peak_mb={rss_mb():.1f}')
    del history,fc; gc.collect()
