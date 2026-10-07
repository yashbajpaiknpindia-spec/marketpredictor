
from pathlib import Path
import sys
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
SCALPER = ROOT / "research" / "scalper"
sys.path.insert(0, str(ROOT))

from research.scalper.v12_calibration import fit_v12_calibration, apply_v12_calibration
from research.scalper.ultra_scalper_engine import ScalperConfig, score_event


def _fixture(n=240):
    t=pd.date_range("2026-10-07 13:56:20", periods=n, freq="s")
    # deterministic score/outcome relationship with enough samples per bin
    px=100+np.cumsum(np.where(np.arange(n)%3==0,0.02,-0.005))
    return pd.DataFrame({
        "ticker":["AAA"]*n,
        "captured_at":t,
        "ltp":px,
        "raw_confidence":50+40*(np.arange(n)%20)/19,
        "signal_confidence":50+40*(np.arange(n)%20)/19,
        "ofi_proxy":0.0,"imbalance_l5":0.0,"microprice_edge_pct":0.0,
        "signal_direction":np.where(np.arange(n)%2,1,-1),
        "edge_pass":True,"score_pass":True,"l2_pass":True,
    })


def test_profile_is_train_only_and_outputs_two_probabilities():
    x=_fixture()
    p=fit_v12_calibration(x.iloc[:160], horizon_seconds=30, bins=8)
    assert p.train_rows == 160
    assert len(p.positive_rates)==len(p.adverse_rates)
    a,b=apply_v12_calibration([55,75],p.to_dict())
    assert np.all((a>=0)&(a<=1))
    assert np.all((b>=0)&(b<=1))


def test_engine_never_treats_raw_confidence_as_probability():
    cfg=ScalperConfig(calibration_profile=None, require_calibrated_probability=True)
    row=pd.Series({
        "ret_1":1,"ret_3":1,"ret_5":1,"range_pct":1,"body_pct":1,"close_location":1,
        "vol_ratio_20":2,"range_ratio_20":2,"rs_1":1,"rs_3":1,"market_ret_1":1,"market_ret_3":1,
    })
    r=score_event(row,cfg)
    assert r["confidence_semantics"]=="model_score_0_100_not_probability"
    assert r["calibration_status"]=="missing_profile"
    assert r["direction"]==0
