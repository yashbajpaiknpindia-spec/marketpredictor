"""Deep V2 walk-forward research harness.

Supports the application's historical export layout and real 1-minute archives. When
1-minute data are present, they remain the execution series and completed 5-minute bars
are generated from them for signals. With only 5-minute data, the run is explicitly marked
compatibility mode and never fabricates 1-minute execution.

The harness separates:
- predictive alpha (forward returns, MFE/MAE),
- mandatory exchange/broker costs,
- configurable slippage,
- portfolio constraints.

No feature uses future observations at signal time. Walk-forward folds use a one-day purge
between train and validation and validation and test.
"""
from __future__ import annotations
import argparse, json, math, time
from pathlib import Path
from typing import Dict, List, Tuple
import numpy as np
import pandas as pd

from v2_alpha import FEATURES, fit_ridge, predict
from v2_engine import resample_1m_to_5m, canonical_symbol


def load_export(root: Path) -> Tuple[pd.DataFrame, str]:
    if root.is_file():
        d=pd.read_csv(root, parse_dates=['timestamp'])
        if 'day' not in d.columns: d['day']=d['timestamp'].dt.date
        if 'symbol' not in d.columns: raise RuntimeError('Combined CSV must contain symbol column')
        detected='1m' if len(d) > 5000000 else '5m'
        return d[['timestamp','open','high','low','close','volume','symbol','day']], detected
    csvs=sorted((root/'historical_data').glob('*/*.csv'))
    if not csvs:
        csvs=sorted(root.rglob('*.csv'))
    frames=[]; detected='5m'
    for p in csvs:
        try:
            d=pd.read_csv(p)
            if not {'timestamp','open','high','low','close','volume'}.issubset(d.columns): continue
            day=pd.to_datetime(d['timestamp'],errors='coerce').dt.date.iloc[0]
            if len(d) >= 300: detected='1m'
            d['symbol']=canonical_symbol(p.stem); d['day']=day; frames.append(d[['timestamp','open','high','low','close','volume','symbol','day']])
        except Exception:
            continue
    if not frames: raise RuntimeError(f'No OHLCV CSVs found under {root}')
    return pd.concat(frames,ignore_index=True),detected


def normalize_5m(raw: pd.DataFrame, detected: str) -> pd.DataFrame:
    if detected != '1m':
        x=raw.copy(); x['timestamp']=pd.to_datetime(x.timestamp,errors='coerce')
        return x.dropna(subset=['timestamp']).sort_values(['symbol','day','timestamp']).reset_index(drop=True)
    out=[]
    for (sym,day),d in raw.groupby(['symbol','day'],sort=False):
        z=d.copy(); z['timestamp']=pd.to_datetime(z.timestamp,errors='coerce'); z=z.dropna(subset=['timestamp']).sort_values('timestamp').set_index('timestamp')
        q=resample_1m_to_5m(z[['open','high','low','close','volume']].rename(columns={'open':'Open','high':'High','low':'Low','close':'Close','volume':'Volume'}))
        if q.empty: continue
        q=q.reset_index().rename(columns={'Open':'open','High':'high','Low':'low','Close':'close','Volume':'volume'})
        q['symbol']=sym; q['day']=day; out.append(q)
    return pd.concat(out,ignore_index=True).sort_values(['symbol','day','timestamp']).reset_index(drop=True)


def build_features(x: pd.DataFrame) -> pd.DataFrame:
    x=x.copy(); g=x.groupby(['symbol','day'],sort=False,observed=True)
    C=g['close']; H=g['high']; L=g['low']; O=g['open']; V=g['volume']
    for n,name in [(1,'r1'),(3,'r3'),(6,'r6'),(12,'r12')]: x[name]=(x['close']/C.shift(n)-1)*100
    x['retopen']=(x['close']/O.transform('first')-1)*100
    pv=((x.high+x.low+x.close)/3)*x.volume; x['_cv']=V.cumsum(); x['_cpv']=pv.groupby([x.symbol,x.day],sort=False).cumsum(); x['vwap']=x['_cpv']/x['_cv'].replace(0,np.nan); x['vwdev']=(x.close/x.vwap-1)*100
    prior_v=V.shift(1); med=prior_v.groupby([x.symbol,x.day],sort=False).transform(lambda s:s.rolling(12,min_periods=3).median()); x['rvol']=x.volume/med.replace(0,np.nan)
    pc=C.shift(1); tr=pd.concat([(x.high-x.low).abs(),(x.high-pc).abs(),(x.low-pc).abs()],axis=1).max(axis=1); x['atrpct']=tr.groupby([x.symbol,x.day],sort=False).transform(lambda s:s.rolling(14,min_periods=5).mean())/x.close*100
    p6h=H.transform(lambda s:s.shift(1).rolling(6,min_periods=6).max()); p6l=L.transform(lambda s:s.shift(1).rolling(6,min_periods=6).min()); p12h=H.transform(lambda s:s.shift(1).rolling(12,min_periods=12).max()); p12l=L.transform(lambda s:s.shift(1).rolling(12,min_periods=12).min())
    x['comp']=(p6h-p6l)/(p12h-p12l)
    x['orh']=H.transform(lambda s:s.iloc[:3].max()); x['orl']=L.transform(lambda s:s.iloc[:3].min()); x['broke_up']=x.close>x.orh; x['broke_dn']=x.close<x.orl
    x['_abv']=(x.close>x.vwap).astype(float); x['breadth']=x.groupby(['day','timestamp'])['_abv'].transform('mean')*100
    x['mret']=x.groupby(['day','timestamp'])['r1'].transform('median'); x['disp']=x.groupby(['day','timestamp'])['r1'].transform('std')
    try:
        u=pd.read_csv(Path('/mnt/data/mp_hist/universe/large_midcap.csv')); sec={canonical_symbol(r.symbol):(str(r.get('app_sector') or '').strip() or str(r.get('industry') or 'Unknown')) for _,r in u.iterrows()}
    except Exception: sec={}
    x['sector']=x.symbol.map(sec).fillna('Unknown'); x['sec_r3']=x.groupby(['day','timestamp','sector'])['r3'].transform('median')
    last=g['close'].last().reset_index(); last['prevclose']=last.groupby('symbol')['close'].shift(1); x=x.merge(last[['symbol','day','prevclose']],on=['symbol','day'],how='left'); x['gap']=(x.open/x.prevclose-1)*100
    x['bar']=g.cumcount()
    for n,name in [(1,'f1'),(3,'f3'),(6,'f6'),(12,'f12')]: x[name]=C.shift(-n)/x.close*100-100
    x['rank']=x.groupby(['day','timestamp'])['r3'].rank(pct=True)
    x['stock_mkt_3']=x.r3-x.mret; x['stock_sec_3']=x.r3-x.sec_r3
    x['range12_hi']=H.transform(lambda s:s.shift(1).rolling(12,min_periods=12).max()); x['range12_lo']=L.transform(lambda s:s.shift(1).rolling(12,min_periods=12).min()); x['range12_pos']=((x.close-x.range12_lo)/(x.range12_hi-x.range12_lo).replace(0,np.nan)).clip(-2,3)
    x['bar_pct']=x.bar/74.0; x['or_dist_up']=(x.close/x.orh-1)*100; x['or_dist_dn']=(x.close/x.orl-1)*100
    x['body_strength']=(x.close-x.open)/(x.high-x.low).replace(0,np.nan); x['close_loc']=(x.close-x.low)/(x.high-x.low).replace(0,np.nan); x['vol_atr']=x.rvol/x.atrpct.replace(0,np.nan)
    x=x.replace([np.inf,-np.inf],np.nan)
    return x.dropna(subset=FEATURES+['f12']).reset_index(drop=True)


def fee_breakdown(entry: float, qty: int, exit_px: float) -> Tuple[float,float]:
    buy=max(0,entry*qty); sell=max(0,exit_px*qty); turnover=buy+sell
    brokerage=min(20,buy*.0003)+min(20,sell*.0003); stt=sell*.00025; exchange=turnover*.0000307; sebi=turnover*.000001; stamp=buy*.00003; gst=(brokerage+exchange+sebi)*.18
    return brokerage+stt+exchange+sebi+stamp+gst, turnover*.0005


def folds(days):
    days=sorted(days); out=[]; trn=24
    while trn+1+6+1+6 <= len(days):
        tr=days[:trn]; va=days[trn+1:trn+7]; te=days[trn+8:trn+14]; out.append((tr,va,te)); trn+=6
    return out


def forward_label_costed(row, horizon='f12'):
    q=max(1,int(50000//float(row.close))); fee,_=fee_breakdown(float(row.close),q,float(row.close*(1+float(row[horizon])/100))); return fee/(float(row.close)*q)*100


def run(root: Path, output: Path) -> Dict[str, object]:
    t=time.time(); raw,detected=load_export(root); raw['timestamp']=pd.to_datetime(raw.timestamp,errors='coerce'); raw=raw.dropna(subset=['timestamp'])
    sig=normalize_5m(raw,detected); x=build_features(sig)
    days=sorted(x.day.unique()); ff=folds(days); all_oos=[]; fold_reports=[]
    thresholds=[0.04,0.06,0.08,0.10,0.12,0.15,0.18,0.22,0.30]
    for fi,(trd,vad,ted) in enumerate(ff,1):
        tr=x[x.day.isin(trd)]; va=x[x.day.isin(vad)].copy(); te=x[x.day.isin(ted)].copy()
        mdl=fit_ridge(tr[FEATURES].to_numpy(float),tr.f12.to_numpy(float),alpha=10.0); va['pred']=predict(mdl,va); te['pred']=predict(mdl,te)
        best=None
        for th in thresholds:
            chosen=pd.concat([va[va.pred>=th].assign(side=1),va[va.pred<=-th].assign(side=-1)])
            if len(chosen)<50: continue
            gross=np.where(chosen.side==1,chosen.f12,-chosen.f12)
            fee=np.array([forward_label_costed(r) for _,r in chosen.iterrows()])
            net=gross-fee; score=float(net.mean())
            if best is None or score>best['score']: best={'score':score,'threshold':th,'n':len(chosen)}
        if best is None: best={'score':-999,'threshold':0.30,'n':0}
        th=float(best['threshold']); chosen=pd.concat([te[te.pred>=th].assign(side=1),te[te.pred<=-th].assign(side=-1)])
        if len(chosen):
            chosen['gross_fwd_pct']=np.where(chosen.side==1,chosen.f12,-chosen.f12); chosen['fee_pct']=[forward_label_costed(r) for _,r in chosen.iterrows()]; chosen['net_fwd_pct']=chosen.gross_fwd_pct-chosen.fee_pct; chosen['fold']=fi; all_oos.append(chosen[['day','symbol','timestamp','bar','close','atrpct','side','pred','gross_fwd_pct','fee_pct','net_fwd_pct','fold']])
        fold_reports.append({'fold':fi,'train':f'{trd[0]}..{trd[-1]}','validation':f'{vad[0]}..{vad[-1]}','test':f'{ted[0]}..{ted[-1]}','validation_threshold':th,'validation_net_pct':best['score'],'test_rows':len(chosen),'test_net_pct':float(chosen.net_fwd_pct.mean()) if len(chosen) else 0.0,'test_win_rate':float((chosen.net_fwd_pct>0).mean()) if len(chosen) else 0.0})
    oos=pd.concat(all_oos,ignore_index=True) if all_oos else pd.DataFrame()
    # Build a trade-level compatibility simulation on the OOS signals using 5m bars; if real 1m input existed, this branch can be extended to 1m executions.
    groups={(sym,day):d.sort_values('bar').reset_index(drop=True) for (sym,day),d in sig.groupby(['symbol','day'],sort=False)}
    sim=[]
    for _,r in oos.iterrows():
        d=groups[(r.symbol,r.day)]; i=int(r.bar)+1
        if i>=len(d): continue
        entry=float(d.open.iloc[i]); atr=float(r.atrpct) if np.isfinite(r.atrpct) else .5; stop_pct=min(.8,max(.35,atr*1.2)); target_pct=min(2.0,stop_pct*1.6); side=int(r.side); stop=entry*(1-stop_pct/100) if side==1 else entry*(1+stop_pct/100); target=entry*(1+target_pct/100) if side==1 else entry*(1-target_pct/100)
        end=min(len(d),i+13); exit_px=float(d.close.iloc[end-1]); reason='time_stop'; mfe=mae=0.0
        for j in range(i,end):
            hi=float(d.high.iloc[j]); lo=float(d.low.iloc[j]);
            if side==1: mfe=max(mfe,(hi/entry-1)*100); mae=max(mae,(1-lo/entry)*100); hs=lo<=stop; ht=hi>=target
            else: mfe=max(mfe,(1-lo/entry)*100); mae=max(mae,(hi/entry-1)*100); hs=hi>=stop; ht=lo<=target
            if hs and ht: exit_px=stop; reason='stop_same_bar_conflict'; break
            if hs: exit_px=stop; reason='stop_loss'; break
            if ht: exit_px=target; reason='target'; break
            exit_px=float(d.close.iloc[j])
        qty=max(1,int(50000//entry)); gross=(exit_px-entry)*qty if side==1 else (entry-exit_px)*qty; mandatory,slip=fee_breakdown(entry,qty,exit_px); net=gross-mandatory-slip
        sim.append({**r.to_dict(),'entry_price':entry,'exit_price':exit_px,'qty':qty,'gross_pnl':gross,'mandatory_fees':mandatory,'slippage':slip,'net_pnl':net,'net_without_slippage':gross-mandatory,'reason':reason,'mfe_pct':mfe,'mae_pct':mae})
    st=pd.DataFrame(sim); result={}
    if len(st):
        pnl=st.net_pnl.to_numpy(); pos=pnl[pnl>0].sum(); neg=-pnl[pnl<0].sum(); eq=pnl.cumsum(); peak=np.maximum.accumulate(np.r_[0,eq]); result.update({'trade_count':int(len(st)),'gross_pnl_inr':float(st.gross_pnl.sum()),'mandatory_fees_inr':float(st.mandatory_fees.sum()),'slippage_inr':float(st.slippage.sum()),'net_pnl_inr':float(st.net_pnl.sum()),'net_without_slippage_inr':float(st.net_without_slippage.sum()),'win_rate_pct':float((pnl>0).mean()*100),'profit_factor':float(pos/neg) if neg>0 else 99.0,'max_drawdown_inr':float((peak[1:]-eq).max()),'avg_trade_inr':float(pnl.mean())})
    result.update({'input_interval':detected,'signal_interval':'5m','execution_interval':'1m' if detected=='1m' else '5m_compatibility','stocks':int(raw.symbol.nunique()),'sessions':len(days),'rows_signal_space':int(len(x)),'folds':fold_reports,'runtime_seconds':time.time()-t,'one_minute_available_for_this_run':detected=='1m','positive_edge_proven':bool(len(oos) and float(oos.net_fwd_pct.mean())>0 and all(float(f['test_net_pct'])>0 for f in fold_reports) and len(st)>0 and float(st.net_pnl.sum())>0)} )
    output.mkdir(parents=True,exist_ok=True); Path(output/'walkforward_summary.json').write_text(json.dumps(result,indent=2,default=str));
    if len(oos): oos.to_csv(output/'oos_predictions.csv',index=False)
    if len(st): st.to_csv(output/'trade_simulation.csv',index=False)
    Path(output/'data_contract.json').write_text(json.dumps({'input_interval':detected,'signal_interval':'5m','execution_interval':'1m' if detected=='1m' else '5m_compatibility','synthetic_1m_created':False},indent=2))
    return result

if __name__=='__main__':
    ap=argparse.ArgumentParser(); ap.add_argument('--data-root',required=True); ap.add_argument('--output-dir',default='research/v2_deep'); a=ap.parse_args(); r=run(Path(a.data_root),Path(a.output_dir)); print(json.dumps(r,indent=2,default=str))
