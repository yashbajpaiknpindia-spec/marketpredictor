"""Phase 1: causal synthetic NSE-like L5/event market generator.

The generator intentionally separates observable market data from hidden
state.  It produces five displayed levels, LTP, spread, volume, index/VIX
context, queue adds/cancels/trades, transient deceptive liquidity, shocks,
reversals, correlated and idiosyncratic flow, and session microstructure
variation.  Hidden labels are retained only for the evaluator and are never
part of the model feature contract.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Tuple, Any
import math
import numpy as np
import pandas as pd

REGIMES = (
    "no_edge", "weak_edge", "moderate_edge", "strong_edge",
    "bull_trend", "bear_trend", "range", "chop", "trend_accel", "trend_decel",
    "vol_expand", "vol_compress", "reversal", "false_breakout", "spoof_imbalance",
    "liquidity_shock", "open_shock", "midday_drift", "close_accel", "gap_follow",
    "gap_fade", "orderflow_flip", "thin_book", "wide_spread", "cross_current",
    "news_jump", "shock_recovery", "correlated_selloff", "idiosyncratic_burst", "dead_zone",
)

FEATURES = (
    "imbalance_l1", "imbalance_l3", "imbalance_l5", "weighted_imbalance",
    "microprice_edge_pct", "imbalance_change", "depth_change_pct", "spread_pct",
    "ret_1_pct", "ret_3_pct", "ret_8_pct", "realized_vol_pct", "volume_z",
    "aggressive_flow", "aggressive_flow_change", "book_pressure_change", "depth_stress",
    "queue_asymmetry", "sweep_proxy", "market_ret_1_pct", "market_ret_5_pct",
    "relative_strength_pct", "market_vol_pct", "vix_level", "vix_change", "trade_intensity",
    "cancel_intensity", "replenish_intensity", "book_turnover", "spread_change_pct",
)

PARAMS: Dict[str, Tuple[float, float, float, float, float, float, float]] = {
    "no_edge": (0.0,.0009,0.00,.12,.00,1.00,1.00), "weak_edge": (.00003,.0010,.25,.45,.25,1.0,1.0),
    "moderate_edge": (.00005,.0011,.48,.58,.48,1.0,1.0), "strong_edge": (.00008,.0012,.72,.72,.72,.98,1.05),
    "bull_trend": (.00045,.00135,.62,.68,.62,.98,1.0), "bear_trend": (-.00045,.00135,.62,.68,.62,.98,1.0),
    "range": (0,.00055,.25,.35,.20,1.02,1.1), "chop": (0,.0010,.10,.08,.08,1.08,.95),
    "trend_accel": (.00030,.0017,.72,.76,.78,1.0,.95), "trend_decel": (.00002,.0012,.34,.42,.32,1.02,1.0),
    "vol_expand": (0,.0020,.58,.58,.56,1.04,.82), "vol_compress": (0,.00038,.28,.52,.22,.98,1.15),
    "reversal": (0,.0015,.62,.72,.62,1.04,.92), "false_breakout": (0,.00145,.35,.40,.20,1.06,.90),
    "spoof_imbalance": (0,.00105,.18,.24,.08,1.05,.98), "liquidity_shock": (0,.0025,.48,.38,.62,1.15,.45),
    "open_shock": (.00012,.0028,.58,.62,.68,1.15,.65), "midday_drift": (0,.00032,.16,.48,.12,1.0,1.2),
    "close_accel": (.00025,.00195,.62,.68,.72,1.08,.88), "gap_follow": (.00030,.0018,.66,.70,.70,1.12,.82),
    "gap_fade": (-.00008,.00175,.54,.64,.52,1.12,.82), "orderflow_flip": (0,.0016,.68,.80,.72,1.04,.90),
    "thin_book": (0,.0017,.48,.52,.62,1.12,.45), "wide_spread": (0,.00125,.50,.50,.48,1.75,.82),
    "cross_current": (0,.0011,.25,.20,.15,1.10,.95), "news_jump": (0,.0031,.70,.48,.78,1.25,.55),
    "shock_recovery": (0,.0022,.52,.55,.60,1.12,.68), "correlated_selloff": (-.00035,.0018,.62,.66,.66,1.10,.80),
    "idiosyncratic_burst": (0,.0024,.58,.52,.64,1.08,.72), "dead_zone": (0,.00020,.02,.03,.02,1.02,1.30),
}

@dataclass(frozen=True)
class MarketConfig:
    bars_per_session: int = 375
    levels: int = 5
    stock_count: int = 40
    seed: int = 20261007
    session_start_price: float = 100.0
    event_probability: float = .025
    spoof_probability: float = .035
    shock_probability: float = .012


def _clip(x,a,b): return float(np.clip(x,a,b))

def _sign_for(regime, i, n):
    if regime in {"bull_trend","trend_accel","gap_follow","strong_edge","moderate_edge","weak_edge","close_accel"}: return 1.0
    if regime in {"bear_trend","correlated_selloff"}: return -1.0
    if regime in {"reversal","shock_recovery"}: return -1.0 if i < n*.5 else 1.0
    if regime == "orderflow_flip": return -1.0 if i < n*.5 else 1.0
    if regime == "false_breakout": return 1.0 if n*.22 < i < n*.36 else -.15
    if regime == "gap_fade": return -1.0 if i < n*.20 else .25
    if regime == "open_shock": return 1.0 if i < n*.12 else .15
    return 0.0


def _features(px, spread, bids, asks, prev_imb, prev_depth, prev_aggr, rets, vols, queue_flow, market_rets, market_vol, vix, vix_change, adds, cancels, trades, prev_spread):
    bd=bids.sum(); ad=asks.sum(); total=max(bd+ad,1e-9)
    l1=(bids[0]-asks[0])/max(bids[0]+asks[0],1e-9)
    l3=(bids[:3].sum()-asks[:3].sum())/max(bids[:3].sum()+asks[:3].sum(),1e-9)
    l5=(bd-ad)/total
    mid=px
    micro=(asks[0]*px*(1-spread/200)+bids[0]*px*(1+spread/200))/max(bids[0]+asks[0],1e-9)
    r1=rets[-1] if rets else 0; r3=sum(rets[-3:]); r8=sum(rets[-8:])
    rv=float(np.std(rets[-20:])) if len(rets)>3 else 0
    vz=(vols[-1]-np.mean(vols[-20:]))/max(np.std(vols[-20:]),1e-6) if len(vols)>4 else 0
    depth=bd+ad
    return {
        "imbalance_l1":l1,"imbalance_l3":l3,"imbalance_l5":l5,"weighted_imbalance":.5*l1+.3*l3+.2*l5,
        "microprice_edge_pct":(micro/mid-1)*100,"imbalance_change":l5-prev_imb,
        "depth_change_pct":(depth/max(prev_depth,1e-9)-1)*100,"spread_pct":spread,"ret_1_pct":r1,
        "ret_3_pct":r3,"ret_8_pct":r8,"realized_vol_pct":rv,"volume_z":vz,
        "aggressive_flow":queue_flow,"aggressive_flow_change":queue_flow-prev_aggr,
        "book_pressure_change":(l5-prev_imb)*100,"depth_stress":1-_clip(depth/(2*12000),0,1),
        "queue_asymmetry":l1,"sweep_proxy":abs(queue_flow)*(_clip(1-depth/(2*12000),0,1)),
        "market_ret_1_pct":market_rets[-1] if market_rets else 0,"market_ret_5_pct":sum(market_rets[-5:]),
        "relative_strength_pct":r3-sum(market_rets[-3:]),"market_vol_pct":market_vol,"vix_level":vix,
        "vix_change":vix_change,"trade_intensity":trades,"cancel_intensity":cancels,
        "replenish_intensity":adds,"book_turnover":(adds+cancels+trades)/max(depth,1),
        "spread_change_pct":spread-prev_spread,
    }


def generate_session(rng: np.random.Generator, regime: str, session_id: int, cfg: MarketConfig, ticker: str = "SYNTH") -> pd.DataFrame:
    n=int(cfg.bars_per_session); drift,vol,edge,persist,impact,spread_mult,depth_mult=PARAMS[regime]
    base_spread=_clip(.018*spread_mult*rng.lognormal(0,.22),.005,.25)
    depth_scale=_clip(8000*depth_mult*rng.lognormal(0,.25),1500,18000)
    px=cfg.session_start_price*rng.lognormal(0,.015)
    bids=depth_scale*rng.uniform(.55,1.2,cfg.levels); asks=depth_scale*rng.uniform(.55,1.2,cfg.levels)
    latent=0.; prev_imb=0.; prev_depth=float(bids.sum()+asks.sum()); prev_aggr=0.; prev_spread=base_spread
    rets=[]; vols=[]; market_rets=[]; rows=[]; vix=13+rng.normal(0,1.4); market_px=100.
    spoof_left=0; spoof_sign=0.; event_left=0; event_sign=0.
    for i in range(n):
        time_frac=i/max(n-1,1)
        intraday=1 + .55*math.exp(-((time_frac-.04)/.07)**2) + .35*math.exp(-((time_frac-.95)/.10)**2)
        target=_sign_for(regime,i,n)
        latent=persist*latent+(1-persist)*target+rng.normal(0,.42)
        if regime=="no_edge": latent=rng.normal(0,1.0)
        if regime=="dead_zone": latent=rng.normal(0,.25)
        if spoof_left<=0 and regime in {"spoof_imbalance","false_breakout"} and rng.random()<cfg.spoof_probability:
            spoof_left=int(rng.integers(2,8)); spoof_sign=float(rng.choice([-1,1]))
        if spoof_left>0: spoof_left-=1
        if event_left<=0 and rng.random()<cfg.event_probability and regime in {"news_jump","idiosyncratic_burst","open_shock","liquidity_shock","shock_recovery"}:
            event_left=int(rng.integers(1,4)); event_sign=float(rng.choice([-1,1]))
        if event_left>0: event_left-=1
        flow=_clip(.72*latent+rng.normal(0,.85),-3,3)
        if regime=="no_edge": flow=rng.normal(0,1)
        if regime=="orderflow_flip" and i>n*.5: flow*=-1
        vis=_clip(math.tanh(.60*flow+rng.normal(0,.65)), -1,1)
        if spoof_left>0: vis=_clip(vis+1.15*spoof_sign,-1,1)
        if regime=="spoof_imbalance" and rng.random()<.08: vis*=-1
        adds=0.; cancels=0.; trades=0.
        for lvl in range(cfg.levels):
            cb=.04*rng.uniform(.3,1); ca=.04*rng.uniform(.3,1)
            if vis>0: ca*=1.35; cb*=.85
            if vis<0: cb*=1.35; ca*=.85
            oldb=bids[lvl]; olda=asks[lvl]
            bids[lvl]*=max(.05,1-cb); asks[lvl]*=max(.05,1-ca)
            cancels += oldb-bids[lvl]+olda-asks[lvl]
            if rng.random()<.16:
                add=depth_scale/(1+.25*lvl)*rng.uniform(.015,.10); bids[lvl]+=add*rng.uniform(.7,1.2); asks[lvl]+=add*rng.uniform(.7,1.2); adds+=2*add
        consume=abs(flow)*depth_scale*.035
        if flow>0:
            asks*=max(.15,1-consume/max(asks.sum(),1e-9)); trades+=consume
        else:
            bids*=max(.15,1-consume/max(bids.sum(),1e-9)); trades+=consume
        spread=base_spread*(1+.65*abs(vis))*intraday
        if regime in {"wide_spread","liquidity_shock","news_jump"}: spread*=rng.uniform(1.3,2.4)
        spread=_clip(spread,.005,.30)
        market_flow=.35*latent+rng.normal(0,.65)
        if regime=="correlated_selloff": market_flow-=.65
        mret=(.00002+.00045*market_flow+rng.normal(0,.0007))*intraday
        market_px=max(20,market_px*(1+mret)); market_rets.append(mret*100)
        mvol=float(np.std(market_rets[-20:])) if len(market_rets)>3 else .05
        vix=_clip(vix+.15*abs(mret*100)-.06*(vix-13)+rng.normal(0,.05),9,35); vix_change=vix-(vix if len(rows)==0 else rows[-1]['vix_level'])
        true_flow = (0 if regime in {"no_edge","dead_zone"} else edge*latent) + .22*market_flow + rng.normal(0,.38)
        if regime in {"spoof_imbalance","cross_current"}: true_flow=.18*latent+rng.normal(0,.50)
        if event_left>0: true_flow += event_sign*1.4
        shock=(vol*(.52*true_flow+rng.normal(0,.92)) + drift)*intraday
        if regime=="false_breakout" and n*.22<i<n*.36: shock+=.0007
        if regime=="false_breakout" and n*.36<=i<n*.45: shock-=.0011
        if regime=="news_jump" and event_left>0: shock+=event_sign*.0022
        if regime=="reversal" and i>n*.5: shock-=.0009*np.sign(latent or 1)
        if regime=="shock_recovery" and i>n*.45: shock+=.00055*np.sign(-latent or 1)
        px=max(5,px*(1+shock)); r=shock*100; rets.append(r); vols.append(max(.01,(1+.6*abs(flow))*rng.lognormal(0,.25)))
        feats=_features(px,spread,bids,asks,prev_imb,prev_depth,prev_aggr,rets,vols,flow,market_rets,mvol,vix,vix_change,adds,cancels,trades,prev_spread)
        row={"session_id":session_id,"bar":i,"timestamp":i,"ticker":ticker,"regime":regime,"ltp":px,"volume":vols[-1],"vix_level":vix,"hidden_state":latent,"hidden_true_flow":true_flow}
        row.update(feats)
        for lvl in range(cfg.levels):
            row[f"bid_px_{lvl+1}"]=px*(1-spread/200)-px*.00005*lvl
            row[f"ask_px_{lvl+1}"]=px*(1+spread/200)+px*.00005*lvl
            row[f"bid_qty_{lvl+1}"]=float(bids[lvl]); row[f"ask_qty_{lvl+1}"]=float(asks[lvl])
        rows.append(row); prev_imb=feats['imbalance_l5']; prev_depth=float(bids.sum()+asks.sum()); prev_aggr=flow; prev_spread=spread
    df=pd.DataFrame(rows)
    h=5; df['forward_return_pct']=df['ltp'].shift(-h)/df['ltp']*100-100
    return df
