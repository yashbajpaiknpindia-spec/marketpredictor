import numpy as np
import pandas as pd
from research.scalper.v12_models import fit_v12_models, apply_v12_models, build_model_features, _side_outcomes


def _fixture(n=800):
    t=pd.date_range('2026-10-07 13:56:20', periods=n, freq='s')
    # Causal L5 features with a deterministic short burst in the middle.
    burst=np.where((np.arange(n)%80)<12,-1.0,0.0)
    px=100+np.cumsum(0.004*burst + 0.001*np.sin(np.arange(n)/9))
    return pd.DataFrame({
        'ticker':['AAA']*n,'captured_at':t,'ltp':px,'volume':1000,
        'raw_direction':np.where(burst<0,-1,1),'signal_direction':np.where(burst<0,-1,1),
        'range_pct':0.05,'body_pct':burst*0.02,'close_location':0.5,
        'spread_pct':0.005,'imbalance_l1':burst,'imbalance_l5':burst,
        'microprice_edge_pct':burst*0.01,'ofi_proxy':burst,'book_pressure':burst,
        'depth_total_qty':1000,'market_ret_1':0,'market_ret_3':0,'rs_1':burst,'rs_3':burst,
    })


def test_direction_edge_profile_is_train_only_and_economic():
    x=_fixture()
    p=fit_v12_models(x.iloc[:600],train_fraction=1.0,horizon_seconds=60)
    s=apply_v12_models(x.iloc[600:],p)
    assert p.train_rows == 600
    assert p.target_net_pct > 0
    assert p.stop_net_pct < 0
    assert set(s['v12_model_direction'].unique()).issubset({-1,0,1})
    assert np.isfinite(s['v12_model_expected_long_net_edge_pct']).all()
    assert np.isfinite(s['v12_model_expected_short_net_edge_pct']).all()


def test_apply_v12_models_preserves_input_row_alignment():
    x = _fixture(300)
    # Deliberately interleave two tickers so feature construction must reorder
    # internally and then restore predictions to the caller's original rows.
    y = pd.concat([x.iloc[::2], x.iloc[1::2]], ignore_index=True)
    p = fit_v12_models(y.iloc[:200], train_fraction=1.0, horizon_seconds=60)
    a = apply_v12_models(y.iloc[200:], p)
    assert len(a) == 100
    assert list(a.index) == list(range(100))
    assert a["v12_model_direction"].notna().all()
    assert a["v12_model_expected_net_edge_pct"].notna().all()


def test_features_and_outcome_labels_do_not_cross_session_boundary():
    x = pd.DataFrame({
        "session_id": [1, 2, 2],
        "ticker": ["AAA", "AAA", "AAA"],
        "captured_at": pd.to_datetime([
            "2026-10-07 15:29:50",
            "2026-10-07 15:30:00",
            "2026-10-07 15:30:30",
        ]),
        "ltp": [100.0, 101.0, 101.2],
        "signal_direction": [1, 1, 1],
        "volume": [100, 100, 100],
    })
    features = build_model_features(x)
    second_session_first = features[(features.session_id == 2)].iloc[0]
    assert second_session_first["ret_1"] == 0.0
    lo, so, _, _ = _side_outcomes(
        features, horizon_seconds=30, target=0.1, stop=0.1
    )
    # The first row cannot use the next session's price as its outcome.
    assert np.isnan(lo[0]) and np.isnan(so[0])
