"""Offline six-phase simulation over the available 61-session historical bundle.

Runs train -> validation -> untouched test. The supplied bundle is 5m only, so the result
is explicitly marked 5m-compatibility, not 1m-execution validation.
"""
from __future__ import annotations
import argparse, csv, datetime as dt, json, os
from pathlib import Path
from typing import Any, Dict, List, Optional
import numpy as np
import pandas as pd
from v2_engine import (ENGINE_VERSION, current_contract, fit_edge_model, evaluate_candidate, walkforward_splits,
                       canonical_symbol, robustness_summary, risk_profile, simulate_outcome)

IST = "Asia/Kolkata"


def read_bundle(root: Path):
    universe = pd.read_csv(root / "universe" / "large_midcap.csv")
    sector_map = {}
    for _, r in universe.iterrows():
        sym = canonical_symbol(r.get("symbol", ""))
        sec = str(r.get("app_sector") or "").strip()
        if not sec:
            industry = str(r.get("industry") or "").lower()
            # Conservative broad proxy used ONLY for research; source is documented.
            if "financial" in industry or "bank" in industry:
                sec = "Financial Services"
            elif "information technology" in industry or industry == "it" or "software" in industry:
                sec = "IT"
            elif "pharma" in industry or "health" in industry:
                sec = "Pharma"
            elif "automobile" in industry or "auto" in industry:
                sec = "Auto"
            elif "metal" in industry or "mining" in industry:
                sec = "Metal"
            elif "oil" in industry or "power" in industry or "energy" in industry:
                sec = "Energy"
            elif "construction" in industry or "capital goods" in industry:
                sec = "Infrastructure"
            elif "consumer" in industry or "food" in industry or "beverage" in industry:
                sec = "FMCG"
            else:
                sec = "Unknown"
        sector_map[sym] = sec
    day_frames: Dict[dt.date, Dict[str, pd.DataFrame]] = {}
    prev_close_by_day: Dict[dt.date, Dict[str, float]] = {}
    files = sorted((root / "historical_data").glob("*/*.csv"))
    for fp in files:
        day = dt.date.fromisoformat(fp.parent.name)
        try:
            x = pd.read_csv(fp)
            ts = pd.to_datetime(x["timestamp"], utc=True).dt.tz_convert(IST)
            x = x.assign(_ts=ts).set_index("_ts")
            x = x.rename(columns={"open":"Open","high":"High","low":"Low","close":"Close","volume":"Volume"})
            keep = [c for c in ("Open","High","Low","Close","Volume") if c in x.columns]
            x = x[keep].dropna(subset=["Open","High","Low","Close"])
            x = x[~x.index.duplicated(keep="last")]
            day_frames.setdefault(day, {})[canonical_symbol(fp.stem)] = x
        except Exception as exc:
            print("skip", fp, exc)
    ordered = sorted(day_frames)
    for j, day in enumerate(ordered):
        if j == 0:
            prev_close_by_day[day] = {}
        else:
            prior = day_frames[ordered[j-1]]
            prev_close_by_day[day] = {canonical_symbol(sym): float(df["Close"].iloc[-1]) for sym, df in prior.items() if not df.empty}
    return universe, sector_map, day_frames, prev_close_by_day


def _roll_nanmax(a, w):
    out=np.full_like(a,np.nan,dtype=float)
    if a.shape[1] < w: return out
    sw=np.lib.stride_tricks.sliding_window_view(a,w,axis=1)
    out[:,w-1:]=np.nanmax(sw,axis=-1)
    return out

def _roll_nanmin(a, w):
    out=np.full_like(a,np.nan,dtype=float)
    if a.shape[1] < w: return out
    sw=np.lib.stride_tricks.sliding_window_view(a,w,axis=1)
    out[:,w-1:]=np.nanmin(sw,axis=-1)
    return out

def _roll_median_prev(a, w):
    out=np.ones_like(a,dtype=float)
    if a.shape[1] <= w: return out
    sw=np.lib.stride_tricks.sliding_window_view(a[:,:-1],w,axis=1)
    med=np.nanmedian(sw,axis=-1)
    out[:,w:]=np.divide(a[:,w:],med,out=np.ones_like(a[:,w:]),where=med>0)
    return out

def discover_examples_fast(day_frames, prev_closes, sector_map, first_bar=6):
    examples=[]; syms=list(day_frames)
    if not syms: return examples
    N=len(syms); T=max(len(day_frames[s]) for s in syms)
    O=np.full((N,T),np.nan); H=O.copy(); L=O.copy(); C=O.copy(); V=np.zeros((N,T))
    for r,sym in enumerate(syms):
        d=day_frames[sym]; m=len(d)
        O[r,:m]=d['Open'].to_numpy(float); H[r,:m]=d['High'].to_numpy(float); L[r,:m]=d['Low'].to_numpy(float); C[r,:m]=d['Close'].to_numpy(float); V[r,:m]=d['Volume'].fillna(0).to_numpy(float)
    prevc=np.concatenate([np.full((N,1),np.nan),C[:,:-1]],axis=1)
    typ=(H+L+C)/3.0; cv=np.cumsum(V,axis=1); cpv=np.cumsum(typ*V,axis=1); VW=np.divide(cpv,cv,out=np.copy(C),where=cv>0)
    tr=np.nanmax(np.stack([H-L,np.abs(H-prevc),np.abs(L-prevc)]),axis=0)
    cs=np.cumsum(np.nan_to_num(tr,nan=0),axis=1); atr=np.empty_like(C); atr[:,:]=0.5
    for i in range(T):
        lo=max(0,i-13); cnt=i-lo+1; atr[:,i]=((cs[:,i]-(cs[:,lo-1] if lo>0 else 0))/cnt)
    atr_pct=np.clip(np.nan_to_num(np.divide(atr,C,out=np.full_like(C,0.5),where=C>0)*100,nan=0.5),0.1,3.0)
    ret5=np.divide(C-prevc,prevc,out=np.zeros_like(C),where=prevc>0)*100
    ret15=np.zeros_like(C); ret15[:,3:]=np.divide(C[:,3:]-C[:,:-3],C[:,:-3],out=np.zeros_like(C[:,3:]),where=C[:,:-3]>0)*100
    op=O[:,0:1]; ret_open=np.divide(C-op,op,out=np.zeros_like(C),where=op>0)*100
    prev_session=np.array([prev_closes.get(canonical_symbol(syms[r]),np.nan) for r in range(N)])
    gap=np.repeat(((op[:,0]/prev_session-1)*100 if True else np.zeros(N))[:,None],T,axis=1)
    vd=np.divide(C-VW,VW,out=np.zeros_like(C),where=VW>0)*100
    first3hi=np.nanmax(H[:,:3],axis=1)[:,None]; first3lo=np.nanmin(L[:,:3],axis=1)[:,None]
    broke_up=C>first3hi; broke_dn=C<first3lo
    vprev=np.roll(V,1,axis=1); vprev[:,:1]=np.nan
    rvol=_roll_median_prev(V,12)
    # prior six / twelve ranges use windows ending immediately before signal bar
    hi6=_roll_nanmax(H,6); lo6=_roll_nanmin(L,6); hi12=_roll_nanmax(H,12); lo12=_roll_nanmin(L,12)
    p6hi=np.roll(hi6,1,axis=1); p6lo=np.roll(lo6,1,axis=1); p12hi=np.roll(hi12,1,axis=1); p12lo=np.roll(lo12,1,axis=1)
    p6hi[:,:1]=np.nan; p6lo[:,:1]=np.nan; p12hi[:,:1]=np.nan; p12lo[:,:1]=np.nan
    # compression at i = previous-six range / previous-twelve range
    comp=np.divide(p6hi-p6lo,p12hi-p12lo,out=np.ones_like(C),where=(p12hi-p12lo)>0)
    # failed breakout: previous bar exceeded a prior-six range then current reverses
    prev6hi=np.roll(hi6,2,axis=1); prev6lo=np.roll(lo6,2,axis=1); prev6hi[:,:2]=np.nan; prev6lo[:,:2]=np.nan
    failed_long=(C[:,1:]*0).astype(bool) if False else np.zeros_like(C,dtype=bool)
    failed_long[:,1:]=(C[:,:-1]<prev6lo[:,1:])&(C[:,1:]>C[:,:-1])&(ret5[:,1:]>0)
    failed_short=np.zeros_like(C,dtype=bool)
    failed_short[:,1:]=(C[:,:-1]>prev6hi[:,1:])&(C[:,1:]<C[:,:-1])&(ret5[:,1:]<0)
    # market state by timestamp
    breadth=np.nanmean(C>VW,axis=0)*100; med5=np.nanmedian(ret5,axis=0); disp=np.nanstd(ret5,axis=0)
    bull=(breadth>=55)&np.isin(np.where((breadth>=65)&(med5>0.03),'strong_bull',np.where((breadth>=55)&(med5>=0),'weak_bull',np.where((breadth<=35)&(med5<-0.03),'strong_bear',np.where((breadth<=45)&(med5<=0),'weak_bear','range')))),['strong_bull','weak_bull','range'])
    bear=(breadth<=45)&np.isin(np.where((breadth>=65)&(med5>0.03),'strong_bull',np.where((breadth>=55)&(med5>=0),'weak_bull',np.where((breadth<=35)&(med5<-0.03),'strong_bear',np.where((breadth<=45)&(med5<=0),'weak_bear','range')))),['strong_bear','weak_bear','range'])
    mr=np.where((breadth>=65)&(med5>0.03),'strong_bull',np.where((breadth>=55)&(med5>=0),'weak_bull',np.where((breadth<=35)&(med5<-0.03),'strong_bear',np.where((breadth<=45)&(med5<=0),'weak_bear','range'))))
    vr=np.where(disp>=0.45,'high_dispersion',np.where(disp<=0.18,'low_dispersion','normal_dispersion'))
    rank=np.empty_like(C,dtype=float)
    for i in range(T):
        vv=ret15[:,i]; ord=np.argsort(vv,kind='mergesort'); rr=np.empty(N,float); rr[ord]=np.arange(N)/max(1,N-1); rank[:,i]=rr
    sec_names=[sector_map.get(canonical_symbol(s),'Unknown') for s in syms]; unique_secs=set(sec_names); sec_med={}
    for sec in unique_secs:
        idx=np.array([k for k,x in enumerate(sec_names) if x==sec],int)
        sec_med[sec]=np.nanmedian(ret5[idx,:],axis=0) if len(idx) else np.zeros(T)
    for r,sym in enumerate(syms):
        sm=sec_med.get(sec_names[r]);
        masks=[
          ('gap_continuation','LONG',(gap[r]>=0.45)&(ret_open[r]>=0.30)&(ret15[r]>0)&(vd[r]>0)&(rvol[r]>=1.15)),
          ('gap_continuation','SHORT',(gap[r]<=-0.45)&(ret_open[r]<=-0.30)&(ret15[r]<0)&(vd[r]<0)&(rvol[r]>=1.15)),
          ('opening_range_expansion','LONG',broke_up[r]&(ret15[r]>0.15)&(rvol[r]>=1.20)&bull),
          ('opening_range_expansion','SHORT',broke_dn[r]&(ret15[r]<-0.15)&(rvol[r]>=1.20)&bear),
          ('cross_sectional_momentum','LONG',(rank[r]>=0.90)&(ret15[r]>0.20)&(vd[r]>0)&bull),
          ('cross_sectional_momentum','SHORT',(rank[r]<=0.10)&(ret15[r]<-0.20)&(vd[r]<0)&bear),
          ('compression_expansion','LONG',(comp[r]<=0.65)&(C[r]>p6hi[r])&(rvol[r]>=1.25)&(ret5[r]>0.05)),
          ('compression_expansion','SHORT',(comp[r]<=0.65)&(C[r]<p6lo[r])&(rvol[r]>=1.25)&(ret5[r]<-0.05)),
          ('failed_breakout','LONG',failed_long[r]),('failed_breakout','SHORT',failed_short[r]),
          ('conditional_mean_reversion','LONG',(vd[r]<=-0.85)&(ret5[r]>0.08)&np.isin(mr,['range','weak_bull'])),
          ('conditional_mean_reversion','SHORT',(vd[r]>=0.85)&(ret5[r]<-0.08)&np.isin(mr,['range','weak_bear']))]
        if sm is not None:
            masks += [('market_sector_stock_alignment','LONG',(sm>0.05)&(ret15[r]>0.20)&(breadth>=58)&(vd[r]>0)),
                      ('market_sector_stock_alignment','SHORT',(sm<-0.05)&(ret15[r]<-0.20)&(breadth<=42)&(vd[r]<0))]
        for fam,side,mask in masks:
            inds=np.flatnonzero(mask)
            inds=inds[(inds>=first_bar)&(inds<T-2)]
            for i in inds:
                entry=float(O[r,i+1]); sig_price=float(C[r,i]);
                if not np.isfinite(entry) or entry<=0: continue
                fill_gap=abs(entry-sig_price)/sig_price*100 if sig_price else 999
                if fill_gap>0.30: continue
                sp,tp=risk_profile(entry,float(atr_pct[r,i+1])); d=day_frames[sym]; out=simulate_outcome(d,i+1,side,entry,sp,tp)
                examples.append({'timestamp':str(d.index[i]),'symbol':canonical_symbol(sym),'family':fam,'side':side,
                    'regime_key':f'{mr[i]}|{vr[i]}','market_regime':mr[i],'sector':sec_names[r],'signal_strength':0.8,
                    'entry':entry,'entry_idx':i+1,'fill_gap_pct':fill_gap,'mfe_pct':out['mfe_pct'],'mae_pct':out['mae_pct'],
                    'net_return_pct':out['net_return_pct'],'net_pnl':out['net_pnl'],'gross_pnl':out['gross_pnl'],'exit_reason':out['reason'],
                    'stop_pct':out['stop_pct'],'target_pct':out['target_pct'],'cost_amount':out['costs']['total']})
    return examples

def simulate_test(days, day_frames, prev_closes, sector_map, model, capital=200000.0):
    trades=[]
    day_stats=[]
    for day in days:
        frames=day_frames[day]; syms=list(frames); open_positions={} ; daily_pnl=0.0; trades_day=0
        n=max((len(x) for x in frames.values()),default=0)
        for i in range(6,n-2):
            # build one market state for this tick
            valid_syms=[s for s in syms if len(frames[s])>i+1]
            # compute market breadth/dispersion once
            rs=[]; above=0; total=0; secrets={}
            for s in valid_syms:
                df=frames[s].iloc[:i+1]
                if len(df)<3: continue
                last=float(df['Close'].iloc[-1]); prevc=float(df['Close'].iloc[-2]); vw=float(((df['High']+df['Low']+df['Close'])/3*df['Volume']).sum()/max(1,df['Volume'].sum()))
                rs.append((s,(last/prevc-1)*100)); above+=1 if last>vw else 0; total+=1
            vals=np.array([v for _,v in rs],float); breadth=above/total*100 if total else 50; med5=float(np.median(vals)) if len(vals) else 0; disp=float(np.std(vals)) if len(vals) else 0
            mr='strong_bull' if breadth>=65 and med5>0.03 else ('weak_bull' if breadth>=55 and med5>=0 else ('strong_bear' if breadth<=35 and med5<-0.03 else ('weak_bear' if breadth<=45 and med5<=0 else 'range')))
            vr='high_dispersion' if disp>=0.45 else ('low_dispersion' if disp<=0.18 else 'normal_dispersion')
            # 15m ranking
            ret15=[]
            for s in valid_syms:
                x=frames[s]
                if len(x)>i+1: ret15.append((s,(float(x['Close'].iloc[i])/float(x['Close'].iloc[i-3])-1)*100))
            retvals=np.array([v for _,v in ret15]); rank={s:float((retvals<=v).mean()) for s,v in ret15}
            candidates=[]
            for s in valid_syms:
                if s in open_positions: continue
                df=frames[s]; x=df.iloc[:i+1]; last=float(x['Close'].iloc[-1]); vw=float(((x['High']+x['Low']+x['Close'])/3*x['Volume']).sum()/max(1,x['Volume'].sum()))
                op=float(x['Open'].iloc[0]); pc=prev_closes.get(day,{}).get(canonical_symbol(s)); gap=(op/pc-1)*100 if pc else 0
                r5=(last/float(x['Close'].iloc[-2])-1)*100; r15=(last/float(x['Close'].iloc[-4])-1)*100; ro=(last/op-1)*100; vd=(last/vw-1)*100 if vw else 0
                first3hi=float(x['High'].iloc[:3].max()); first3lo=float(x['Low'].iloc[:3].min()); broke_up=last>first3hi; broke_dn=last<first3lo
                a=float(x['High'].iloc[-7:-1].max()-x['Low'].iloc[-7:-1].min()) if len(x)>=8 else float(x['High'].iloc[:-1].max()-x['Low'].iloc[:-1].min())
                b=float(x['High'].iloc[-13:-1].max()-x['Low'].iloc[-13:-1].min()) if len(x)>=14 else a; comp=a/b if b>0 else 1
                p6hi=float(x['High'].iloc[-7:-1].max()) if len(x)>=8 else last; p6lo=float(x['Low'].iloc[-7:-1].min()) if len(x)>=8 else last
                base_vol=float(x['Volume'].iloc[:-1].tail(12).median()) if len(x)>=3 else 0; rvol=float(x['Volume'].iloc[-1]/base_vol) if base_vol>0 else 1
                tr=np.maximum.reduce([(x['High']-x['Low']).to_numpy(),(x['High']-x['Close'].shift(1)).abs().fillna(0).to_numpy(),(x['Low']-x['Close'].shift(1)).abs().fillna(0).to_numpy()]); atp=float(np.nanmean(tr[-14:])/last*100) if last else 0.5
                atp=max(0.1,min(3.0,atp)); sec=sector_map.get(canonical_symbol(s)); sm=None
                # sector proxy median
                if sec:
                    sv=[]
                    for s2 in valid_syms:
                        if sector_map.get(canonical_symbol(s2))==sec:
                            d2=frames[s2]; sv.append((float(d2['Close'].iloc[i])/float(d2['Close'].iloc[i-1])-1)*100)
                    if sv: sm=float(np.median(sv))
                bull=breadth>=55 and mr in {'strong_bull','weak_bull','range'}; bear=breadth<=45 and mr in {'strong_bear','weak_bear','range'}
                sigs=[]
                if gap>=0.45 and ro>=0.30 and r15>0 and vd>0 and rvol>=1.15: sigs.append(('gap_continuation','LONG',0.8))
                if gap<=-0.45 and ro<=-0.30 and r15<0 and vd<0 and rvol>=1.15: sigs.append(('gap_continuation','SHORT',0.8))
                if broke_up and r15>0.15 and rvol>=1.20 and bull: sigs.append(('opening_range_expansion','LONG',0.9))
                if broke_dn and r15<-0.15 and rvol>=1.20 and bear: sigs.append(('opening_range_expansion','SHORT',0.9))
                if rank.get(s,0.5)>=0.90 and r15>0.20 and vd>0 and bull: sigs.append(('cross_sectional_momentum','LONG',rank.get(s,0.9)))
                if rank.get(s,0.5)<=0.10 and r15<-0.20 and vd<0 and bear: sigs.append(('cross_sectional_momentum','SHORT',1-rank.get(s,0.1)))
                if sm is not None and sm>0.05 and r15>0.20 and breadth>=58 and vd>0: sigs.append(('market_sector_stock_alignment','LONG',0.85))
                if sm is not None and sm<-0.05 and r15<-0.20 and breadth<=42 and vd<0: sigs.append(('market_sector_stock_alignment','SHORT',0.85))
                if comp<=0.65 and last>p6hi and rvol>=1.25 and r5>0.05: sigs.append(('compression_expansion','LONG',0.9))
                if comp<=0.65 and last<p6lo and rvol>=1.25 and r5<-0.05: sigs.append(('compression_expansion','SHORT',0.9))
                if i>6:
                    prev_c=float(df['Close'].iloc[i-1]); prev_hi=float(df['High'].iloc[i-7:i-1].max()); prev_lo=float(df['Low'].iloc[i-7:i-1].min())
                    if prev_c>prev_hi and last<prev_c and r5<0: sigs.append(('failed_breakout','SHORT',0.8))
                    if prev_c<prev_lo and last>prev_c and r5>0: sigs.append(('failed_breakout','LONG',0.8))
                if vd<=-0.85 and r5>0.08 and mr in {'range','weak_bull'}: sigs.append(('conditional_mean_reversion','LONG',0.75))
                if vd>=0.85 and r5<-0.08 and mr in {'range','weak_bear'}: sigs.append(('conditional_mean_reversion','SHORT',0.75))
                if not sigs: continue
                sig=max(sigs,key=lambda z:(z[2],z[0]))
                # edge model
                key=f"{sig[0]}|{mr}|{vr}|{sig[1]}"; g=(model.get('groups') or {}).get(key) or (model.get('families') or {}).get(sig[0])
                if not g or not g.get('proven'): continue
                edge=float(g.get('expectancy_pct') or 0)
                entry=float(df['Open'].iloc[i+1]); gapfill=abs(entry-last)/last*100 if last else 999
                if gapfill>0.30: continue
                candidates.append((edge,s,df,sig,entry,atp,mr,vr,g))
            candidates.sort(reverse=True,key=lambda z:z[0]); slots=max(0,4-len(open_positions))
            for edge,s,df,sig,entry,atp,mr,vr,g in candidates[:slots]:
                if trades_day>=5: break
                stop_pct,target_pct=risk_profile(entry,atp); qty=max(1,int(min(50000.0,capital*0.25)//entry)); risk_amt=entry*qty*stop_pct/100 if qty else 0
                if risk_amt>capital*0.01: qty=max(1,int((capital*0.01)/(entry*stop_pct/100)))
                if qty<=0 or daily_pnl<=-capital*0.02: continue
                out=simulate_outcome(df,i+1,sig[1],entry,stop_pct,target_pct)
                t={'trading_date':day.isoformat(),'symbol':canonical_symbol(s),'family':sig[0],'side':sig[1],'signal_time':str(df.index[i]),'entry_time':str(df.index[i+1]),'entry_price':entry,'exit_price':out['exit_price'],
                   'gross_pnl':out['gross_pnl']*qty/out['qty'] if out.get('qty') else 0,'net_pnl':out['net_pnl']*qty/out['qty'] if out.get('qty') else out['net_pnl'],'net_return_pct':out['net_pnl']/ (entry*out['qty']) * 100 if out.get('qty') and entry else 0,
                   'cost_amount':out['costs']['total']*qty/out['qty'] if out.get('qty') else out['costs']['total'],'slippage_amount':out['costs']['slippage']*qty/out['qty'] if out.get('qty') else out['costs']['slippage'],
                   'mfe_pct':out['mfe_pct'],'mae_pct':out['mae_pct'],'exit_reason':out['reason'],'market_regime':mr,'volatility_regime':vr,'expected_net_edge_pct':edge,'evidence_n':g.get('n'),'profit_factor_bucket':g.get('profit_factor'),'win_rate_bucket':g.get('win_rate_pct'),'stop_pct':stop_pct,'target_pct':target_pct,'notional':entry*qty,'qty':qty}
                trades.append(t); daily_pnl+=t['net_pnl']; trades_day+=1
        day_stats.append({'date':day.isoformat(),'trades':trades_day,'net_pnl':daily_pnl,'wins':sum(1 for t in trades if t['trading_date']==day.isoformat() and t['net_pnl']>0),'losses':sum(1 for t in trades if t['trading_date']==day.isoformat() and t['net_pnl']<=0)})
    return trades,day_stats

def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--data",required=True); ap.add_argument("--out",required=True); ap.add_argument("--capital",type=float,default=200000); args=ap.parse_args()
    root=Path(args.data); out=Path(args.out); out.mkdir(parents=True,exist_ok=True)
    universe,sector_map,day_frames,prev_closes=read_bundle(root)
    days=sorted(day_frames)
    train,valid,test=walkforward_splits(days,36,12)
    train_ex=[]
    for d in train:
        train_ex.extend(discover_examples_fast(day_frames[d],prev_closes[d],sector_map))
    model=fit_edge_model(train_ex)
    # validation is used only to choose an evidence minimum, not to touch test days.
    valid_ex=[]
    for d in valid:
        valid_ex.extend(discover_examples_fast(day_frames[d],prev_closes[d],sector_map))
    valid_model_view=fit_edge_model(valid_ex)
    trades,day_stats=simulate_test(test,day_frames,prev_closes,sector_map,model,args.capital)
    summary=robustness_summary(trades,3)
    # family/regime test breakdown
    byfam={}
    for t in trades: byfam.setdefault(t["family"],[]).append(t)
    byreg={}
    for t in trades: byreg.setdefault(t["market_regime"],[]).append(t)
    summary["by_family"]={k:robustness_summary(v) for k,v in byfam.items()}
    summary["by_regime"]={k:robustness_summary(v) for k,v in byreg.items()}
    contract=current_contract("INDstocks historical bundle",one_minute_available=False,index_available=False,vix_available=False,event_available=False)
    report={"engine_version":ENGINE_VERSION,"capital":args.capital,"data_contract":contract.as_dict(),
            "data_days":len(days),"train_days":len(train),"validation_days":len(valid),"test_days":len(test),
            "train_examples":len(train_ex),"validation_examples":len(valid_ex),"test_trades":len(trades),
            "positive_edge_proven_on_test":bool(summary["n"]>=30 and summary["net_pnl"]>0 and summary["profit_factor"]>1.05 and summary["expectancy_pct"]>0),
            "summary":summary,
            "validation_model_summary": {"families": valid_model_view.get("families",{})},
            "limitations":["Historical bundle is 5m, so execution validation is 5m compatibility mode, not 1m.",
                            "NIFTY/VIX/sector index series were absent from the supplied bundle; market state uses cross-sectional breadth/dispersion and sector proxies derived from the universe file.",
                            "The supplied universe is a current snapshot applied backward; point-in-time constituent membership is not available in this research run.",
                            "No bid/ask, tick, order-book or historical pre-open data were available."],
            "sources":{"historical_bundle":"user-supplied 61-session INDstocks historical export","official_nse_data":"https://www.nseindia.com/static/products-services/equity-market-data-reports-download","official_large_midcap":"https://www.nseindia.com/static/products-services/indices-niftylargemidcap250-index"}}
    with open(out/"report.json","w") as f: json.dump(report,f,indent=2,default=str)
    with open(out/"trades.csv","w",newline="") as f:
        if trades:
            w=csv.DictWriter(f,fieldnames=list(trades[0].keys())); w.writeheader(); w.writerows(trades)
    pd.DataFrame(day_stats).to_csv(out/"day_stats.csv",index=False)
    with open(out/"edge_model.json","w") as f: json.dump(model,f,indent=2)
    print(json.dumps({"positive_edge_proven_on_test":report["positive_edge_proven_on_test"],"summary":summary,"trades":len(trades)},indent=2))

if __name__=="__main__": main()
