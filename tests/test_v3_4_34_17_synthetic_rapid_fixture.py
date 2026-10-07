import sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import numpy as np
import pandas as pd

from rapid_forensics import analyze_ticker_day


def make_synthetic_session(seed=17, bars=375):
    idx=pd.date_range('2026-01-09 09:15', periods=bars, freq='1min', tz='Asia/Kolkata')
    rng=np.random.default_rng(seed)
    base=100 + np.cumsum(rng.normal(0,0.06,bars))
    op=base
    cl=base+rng.normal(0,0.03,bars)
    hi=np.maximum(op,cl)+np.abs(rng.normal(0.04,0.02,bars))
    lo=np.minimum(op,cl)-np.abs(rng.normal(0.04,0.02,bars))
    return pd.DataFrame({'Open':op,'High':hi,'Low':lo,'Close':cl,'Volume':100000},index=idx)


def test_synthetic_fixture_exercises_fast_rapid_path_with_missing_context():
    df=make_synthetic_session()
    result=analyze_ticker_day(
        'SYNTH01', df, None, pd.Timestamp('2026-01-09').date(),
        None, None, 0.125, 0.40, 0.15, 5.0,
        parity=False,
    )
    assert result['bars']==375
    assert result['eligible_signals'] >= 0
    assert result.get('aborted',False) is not True
    assert result['parity']['status']=='NOT_RUN'
    assert result['ticker']=='SYNTH01'
    assert result['day']=='2026-01-09'
