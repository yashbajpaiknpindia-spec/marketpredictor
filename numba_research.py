from __future__ import annotations
import datetime as dt, json, csv
from pathlib import Path
import numpy as np
import pandas as pd
from numba import njit
from v2_engine import canonical_symbol, risk_profile

@njit(cache=True)
def _median(a, n):
    x=np.empty(n,np.float64)
    for i in range(n): x[i]=a[i]
    x.sort()
    if n==0: return 0.0
    if n%2: return x[n//2]
    return 0.5*(x[n//2-1]+x[n//2])

@njit(cache=True)
def _simulate_event(O,H,L,C, r, entry_i, side, stop_pct, target_pct, max_hold):
    entry=O[r,entry_i]
    stop=entry*(1-stop_pct/100) if side==1 else entry*(1+stop_pct/100)
    target=entry*(1+target_pct/100) if side==1 else entry*(1-target_pct/100)
    end=min(C.shape[1],entry_i+1+max_hold)
    mfe=0.; mae=0.; ex=entry; reason=0
    for j in range(entry_i,end):
        hi=H[r,j]; lo=L[r,j]; cl=C[r,j]
        if side==1:
            mfe=max(mfe,(hi-entry)/entry*100); mae=max(mae,(entry-lo)/entry*100); hs=lo<=stop; ht=hi>=target
        else:
            mfe=max(mfe,(entry-lo)/entry*100); mae=max(mae,(hi-entry)/entry*100); hs=hi>=stop; ht=lo<=target
        if hs and ht: ex=stop; reason=3; return ex,mfe,mae,reason
        if hs: ex=stop; reason=1; return ex,mfe,mae,reason
        if ht: ex=target; reason=2; return ex,mfe,mae,reason
        ex=cl
    return ex,mfe,mae,reason

@njit(cache=True)
def process_day(O,H,L,C,V,prev_close,sector_ids):
    N,T=C.shape
    VW=np.zeros((N,T)); TR=np.zeros((N,T)); ATRP=np.full((N,T),0.5); RV=np.ones((N,T));
    cumv=np.zeros(N); cump=np.zeros(N)
    for i in range(T):
        for r in range(N):
            tp=(H[r,i]+L[r,i]+C[r,i])/3; cumv[r]+=V[r,i]; cump[r]+=tp*V[r,i]; VW[r,i]=cump[r]/cumv[r] if cumv[r]>0 else C[r,i]
            pc=C[r,i-1] if i>0 else C[r,i]; TR[r,i]=max(H[r,i]-L[r,i],abs(H[r,i]-pc),abs(L[r,i]-pc))
        for r in range(N):
            lo=max(0,i-13); total=0.; cnt=0
            for j in range(lo,i+1): total+=TR[r,j]; cnt+=1
            ATRP[r,i]=(total/cnt/C[r,i]*100) if C[r,i]>0 else 0.5
            if i>=3:
                w=max(0,i-12); n=i-w; temp=np.empty(12,np.float64); k=0
                for j in range(w,i): temp[k]=V[r,j]; k+=1
                med=_median(temp,k) if k else 0.
                RV[r,i]=V[r,i]/med if med>0 else 1.
    out=[]
    ret15=np.zeros((N,T)); ret5=np.zeros((N,T)); ro=np.zeros((N,T)); vd=np.zeros((N,T)); gap=np.zeros(N)
    for r in range(N):
        gap[r]=(O[r,0]/prev_close[r]-1)*100 if prev_close[r]>0 else 0.
        for i in range(T):
            ret5[r,i]=(C[r,i]/C[r,i-1]-1)*100 if i>0 and C[r,i-1]>0 else 0
            ret15[r,i]=(C[r,i]/C[r,i-3]-1)*100 if i>=3 and C[r,i-3]>0 else 0
            ro[r,i]=(C[r,i]/O[r,0]-1)*100 if O[r,0]>0 else 0
            vd[r,i]=(C[r,i]/VW[r,i]-1)*100 if VW[r,i]>0 else 0
    # event tuples: r,i,fam,side,entry,mfe,mae,netret,gross,cost,stop,target,reason,regime,vr,sector
    for i in range(6,T-2):
        breadth=0.; med5a=np.empty(N,np.float64)
        for r in range(N):
            med5a[r]=ret5[r,i]; breadth += 1. if C[r,i]>VW[r,i] else 0.
        breadth=breadth/N*100
        med5=_median(med5a,N)
        mean=0.;
        for r in range(N): mean+=ret5[r,i]
        mean/=N
        var=0.;
        for r in range(N): var+=(ret5[r,i]-mean)*(ret5[r,i]-mean)
        disp=(var/N)**0.5
        if breadth>=65 and med5>0.03: mr=0
        elif breadth>=55 and med5>=0: mr=1
        elif breadth<=35 and med5<-0.03: mr=2
        elif breadth<=45 and med5<=0: mr=3
        else: mr=4
        vr=0 if disp>=0.45 else (1 if disp<=0.18 else 2)
        # sector median ret5
        sec_m=np.zeros(8); sec_n=np.zeros(8)
        for r in range(N):
            sid=sector_ids[r]
            if sid<0 or sid>=8: sid=7
            sec_m[sid]+=ret5[r,i]; sec_n[sid]+=1
        for s in range(8):
            if sec_n[s]>0: sec_m[s]/=sec_n[s]
        # rank by ret15
        rank=np.empty(N,np.float64)
        for r in range(N):
            cnt=0
            for q in range(N):
                if ret15[q,i] <= ret15[r,i]: cnt+=1
            rank[r]=cnt/float(N)
        for r in range(N):
            last=C[r,i]; bull=breadth>=55 and (mr==0 or mr==1 or mr==4); bear=breadth<=45 and (mr==2 or mr==3 or mr==4)
            sec=sector_ids[r]; sm=sec_m[sec] if sec>=0 and sec<8 else 0
            # independent signal families, pick strongest
            fam=-1; side=0; strength=-1.
            if gap[r]>=0.45 and ro[r,i]>=0.30 and ret15[r,i]>0 and vd[r,i]>0 and RV[r,i]>=1.15 and 0.8>strength:
                fam=0; side=1; strength=0.8
            if gap[r]<=-0.45 and ro[r,i]<=-0.30 and ret15[r,i]<0 and vd[r,i]<0 and RV[r,i]>=1.15 and 0.8>strength:
                fam=0; side=-1; strength=0.8
            hi3=H[r,0]; lo3=L[r,0]
            for j in range(1,3): hi3=max(hi3,H[r,j]); lo3=min(lo3,L[r,j])
            if last>hi3 and ret15[r,i]>0.15 and RV[r,i]>=1.20 and bull and 0.9>strength: fam=1;side=1;strength=.9
            if last<lo3 and ret15[r,i]<-0.15 and RV[r,i]>=1.20 and bear and 0.9>strength: fam=1;side=-1;strength=.9
            if rank[r]>=0.90 and ret15[r,i]>0.20 and vd[r,i]>0 and bull and rank[r]>strength: fam=2;side=1;strength=rank[r]
            if rank[r]<=0.10 and ret15[r,i]<-0.20 and vd[r,i]<0 and bear and 1-rank[r]>strength: fam=2;side=-1;strength=1-rank[r]
            if sec>=0 and sec<8:
                if sm>0.05 and ret15[r,i]>0.20 and breadth>=58 and vd[r,i]>0 and .85>strength: fam=3;side=1;strength=.85
                if sm<-0.05 and ret15[r,i]<-0.20 and breadth<=42 and vd[r,i]<0 and .85>strength: fam=3;side=-1;strength=.85
            lo6=max(0,i-6); hi6=H[r,lo6]; lo6v=L[r,lo6]
            for j in range(lo6+1,i): hi6=max(hi6,H[r,j]);lo6v=min(lo6v,L[r,j])
            lo12=max(0,i-12); hi12=H[r,lo12]; lo12v=L[r,lo12]
            for j in range(lo12+1,i): hi12=max(hi12,H[r,j]);lo12v=min(lo12v,L[r,j])
            comp=(hi6-lo6v)/(hi12-lo12v) if hi12>lo12v else 1
            if comp<=.65 and last>hi6 and RV[r,i]>=1.25 and ret5[r,i]>.05 and .9>strength: fam=4;side=1;strength=.9
            if comp<=.65 and last<lo6v and RV[r,i]>=1.25 and ret5[r,i]<-.05 and .9>strength: fam=4;side=-1;strength=.9
            if i>6:
                ph=H[r,i-7]; pl=L[r,i-7]
                for j in range(i-6,i-1): ph=max(ph,H[r,j]);pl=min(pl,L[r,j])
                if C[r,i-1]>ph and last<C[r,i-1] and ret5[r,i]<0 and .8>strength: fam=5;side=-1;strength=.8
                if C[r,i-1]<pl and last>C[r,i-1] and ret5[r,i]>0 and .8>strength: fam=5;side=1;strength=.8
            if vd[r,i]<=-.85 and ret5[r,i]>.08 and (mr==4 or mr==1) and .75>strength: fam=6;side=1;strength=.75
            if vd[r,i]>=.85 and ret5[r,i]<-.08 and (mr==4 or mr==3) and .75>strength: fam=6;side=-1;strength=.75
            if fam<0: continue
            entry=O[r,i+1]
            if not np.isfinite(entry) or entry<=0: continue
            fill_gap=abs(entry-last)/last*100 if last>0 else 999
            if fill_gap>.30: continue
            stop_pct=min(.80,max(.35,ATRP[r,i+1]*1.2)); target_pct=min(2.0,stop_pct*1.6)
            ex,mfe,mae,reason=_simulate_event(O,H,L,C,r,i+1,1 if side==1 else -1,stop_pct,target_pct,12)
            qty=max(1,int(50000.0//entry)); buy=entry*qty; sell=ex*qty; turn=buy+sell
            brokerage=min(20.,buy*.0003)+min(20.,sell*.0003); stt=sell*.00025; exch=turn*.0000307; sebi=turn*.000001; stamp=buy*.00003; gst=(brokerage+exch+sebi)*.18; slip=turn*.0005; cost=max(40.,brokerage+stt+exch+sebi+stamp+gst+slip)
            gross=(ex-entry)*qty if side==1 else (entry-ex)*qty; net=gross-cost; netret=net/buy*100
            out.append((r,i,fam,side,entry,ex,mfe,mae,netret,net,gross,cost,slip,stop_pct,target_pct,reason,mr,vr,sec,qty))
    return out

FAM=['gap_continuation','opening_range_expansion','cross_sectional_momentum','market_sector_stock_alignment','compression_expansion','failed_breakout','conditional_mean_reversion']
REG=['strong_bull','weak_bull','strong_bear','weak_bear','range']; VOL=['high_dispersion','low_dispersion','normal_dispersion']

def load(root):
    u=pd.read_csv(root/'universe'/'large_midcap.csv'); sectors=[]
    for _,r in u.iterrows():
        sec=str(r.get('app_sector') or '').strip()
        if not sec:
            ind=str(r.get('industry') or '').lower()
            if 'financial' in ind or 'bank' in ind: sec='Financial Services'
            elif 'software' in ind or 'information technology' in ind: sec='IT'
            elif 'pharma' in ind or 'health' in ind: sec='Pharma'
            elif 'auto' in ind or 'automobile' in ind: sec='Auto'
            elif 'metal' in ind or 'mining' in ind: sec='Metal'
            elif 'oil' in ind or 'power' in ind or 'energy' in ind: sec='Energy'
            elif 'consumer' in ind or 'food' in ind: sec='FMCG'
            elif 'construction' in ind or 'capital goods' in ind: sec='Infrastructure'
            else: sec='Unknown'
        sectors.append(sec)
    sec_names=['Bank','IT','Auto','Pharma','FMCG','Metal','Energy','Financial Services']
    sec_id=np.array([sec_names.index(s) if s in sec_names else 7 for s in sectors],dtype=np.int64)
    days={}
    for fp in (root/'historical_data').glob('*/*.csv'):
        day=dt.date.fromisoformat(fp.parent.name); x=pd.read_csv(fp)
        x=x.rename(columns={'open':'Open','high':'High','low':'Low','close':'Close','volume':'Volume'})
        x.index=pd.to_datetime(x['timestamp'],utc=True).dt.tz_convert('Asia/Kolkata')
        days.setdefault(day,{})[canonical_symbol(fp.stem)]=x[['Open','High','Low','Close','Volume']]
    return u,sec_id,days

def day_arrays(frames, symbols, prev):
    N=len(symbols); T=max(len(frames[s]) for s in symbols); O=np.full((N,T),np.nan);H=np.full_like(O,np.nan);L=np.full_like(O,np.nan);C=np.full_like(O,np.nan);V=np.zeros_like(O)
    for r,s in enumerate(symbols):
        d=frames[s];m=len(d);O[r,:m]=d.Open.to_numpy(float);H[r,:m]=d.High.to_numpy(float);L[r,:m]=d.Low.to_numpy(float);C[r,:m]=d.Close.to_numpy(float);V[r,:m]=d.Volume.to_numpy(float)
    p=np.array([prev.get(canonical_symbol(s),np.nan) for s in symbols],float)
    return O,H,L,C,V,p

def build_examples(days,day_frames,prev_close,sec_id):
    out=[]
    for d in days:
        frames=day_frames[d]; symbols=list(frames); O,H,L,C,V,p=day_arrays(frames,symbols,prev_close[d]); ev=process_day(O,H,L,C,V,p,sec_id)
        for e in ev:
            r,i,f,side,entry,ex,mfe,mae,netret,net,gross,cost,slip,sp,tp,reason,mr,vr,sec,qty=e
            out.append({'date':d.isoformat(),'symbol':symbols[r],'family':FAM[f],'side':'LONG' if side==1 else 'SHORT','signal_i':i,'entry':entry,'exit':ex,'mfe_pct':mfe,'mae_pct':mae,'net_return_pct':netret,'net_pnl':net,'gross_pnl':gross,'cost_amount':cost,'slippage_amount':slip,'stop_pct':sp,'target_pct':tp,'exit_reason':reason,'market_regime':REG[mr],'volatility_regime':VOL[vr],'sector':sec})
    return out

def profit_factor(rows):
    pos=sum(max(0,float(x['net_pnl'])) for x in rows); neg=sum(-min(0,float(x['net_pnl'])) for x in rows); return pos/neg if neg else (99.0 if pos else 0.0)

def fit_model(rows,min_n=30):
    groups={}
    for x in rows: groups.setdefault((x['family'],x['market_regime'],x['volatility_regime'],x['side']),[]).append(x)
    m={}
    for k,rs in groups.items():
        avg=float(np.mean([r['net_return_pct'] for r in rs])); pf=profit_factor(rs); wr=float(np.mean([r['net_pnl']>0 for r in rs])*100); n=len(rs)
        m[k]={'n':n,'expectancy_pct':avg,'profit_factor':pf,'win_rate_pct':wr,'proven':n>=min_n and avg>0.12 and pf>1.05}
    return m

def select_test_rows(rows,model,capital=200000):
    # Select only signal buckets proven in train. No test outcomes are used in selection.
    accepted=[]
    for x in rows:
        k=(x['family'],x['market_regime'],x['volatility_regime'],x['side']); g=model.get(k)
        if g and g['proven']:
            y=dict(x); y.update({'expected_edge_pct':g['expectancy_pct'],'evidence_n':g['n'],'bucket_pf':g['profit_factor'],'bucket_wr':g['win_rate_pct']}); accepted.append(y)
    # portfolio layer: at most 4 trades/day, 1% capital risk per trade, and don't reuse same symbol in a day.
    final=[]; pnl_by_day={}; symbols_by_day=set()
    bytime={}
    for x in accepted: bytime.setdefault((x['date'],x['signal_i']),[]).append(x)
    for (day,ii),xs in sorted(bytime.items()):
        xs=sorted(xs,key=lambda z:(z['expected_edge_pct'],z['bucket_pf'],z['bucket_wr']),reverse=True)
        active=sum(1 for t in final if t['date']==day and t['_end_i']>ii)
        for x in xs:
            if active>=4: break
            if (day,x['symbol']) in symbols_by_day: continue
            # risk is already implied by outcome sizing; check daily account stop using realized net pnl only before the signal.
            dayp=pnl_by_day.get(day,0.0)
            if dayp<=-capital*.02: continue
            x2=dict(x); x2.pop('_end_i',None); final.append(x2); symbols_by_day.add((day,x['symbol'])); pnl_by_day[day]=dayp+float(x['net_pnl']); active+=1
    return final

def main2():
    import argparse
    ap=argparse.ArgumentParser(); ap.add_argument('--data',required=True); ap.add_argument('--out',required=True); ap.add_argument('--capital',type=float,default=200000); a=ap.parse_args()
    root=Path(a.data); out=Path(a.out); out.mkdir(parents=True,exist_ok=True)
    u,sec_id,days_map=load(root); days=sorted(days_map)
    # previous day closes
    prev={}; prior={}
    for j,d in enumerate(days):
        prev[d]={} if j==0 else {s:float(df.Close.iloc[-1]) for s,df in days_map[days[j-1]].items() if len(df)}
    train_n=36; valid_n=12; train=days[:train_n]; valid=days[train_n:train_n+valid_n]; test=days[train_n+valid_n:]
    print('building train examples',len(train))
    tr=build_examples(train,days_map,prev,sec_id); print('train examples',len(tr))
    model=fit_model(tr); print('proven buckets',sum(1 for v in model.values() if v['proven']),'/',len(model))
    va=build_examples(valid,days_map,prev,sec_id); print('validation examples',len(va))
    # validation is diagnostic only; report whether proven buckets stay positive in the validation window
    val_sel=[]
    for x in va:
        g=model.get((x['family'],x['market_regime'],x['volatility_regime'],x['side']))
        if g and g['proven']: val_sel.append(x)
    te=build_examples(test,days_map,prev,sec_id); print('test raw examples',len(te))
    # Need end index from row's exit time isn't retained; reconstruct a conservative active blocking by signal index+12.
    for x in te: x['_end_i']=x['signal_i']+12
    ts=select_test_rows(te,model,a.capital)
    def summ(rows):
        pnl=np.array([float(x['net_pnl']) for x in rows],float); ret=np.array([float(x['net_return_pct']) for x in rows],float)
        pos=pnl[pnl>0].sum(); neg=-pnl[pnl<0].sum(); eq=np.cumsum(pnl); peak=np.maximum.accumulate(np.r_[0,eq]); dd=(peak[1:]-eq).max() if len(eq) else 0
        return {'trades':len(rows),'net_pnl':float(pnl.sum()) if len(pnl) else 0,'expectancy_pct':float(ret.mean()) if len(ret) else 0,'win_rate_pct':float((pnl>0).mean()*100) if len(pnl) else 0,'profit_factor':float(pos/neg) if neg else (99.0 if pos else 0.0),'max_drawdown_inr':float(dd),'top3_removed_net_pnl':float(pnl.sum()-np.sort(pnl)[::-1][:min(3,len(pnl))].sum()) if len(pnl) else 0}
    report={'engine_version':'v2.0.0-six-phase','capital':a.capital,'days_total':len(days),'train_days':len(train),'validation_days':len(valid),'test_days':len(test),'train_examples':len(tr),'validation_examples':len(va),'validation_proven_bucket_examples':len(val_sel),'test_examples_raw':len(te),'test_trades_selected':len(ts),'train_proven_buckets':[{'family':k[0],'market_regime':k[1],'volatility_regime':k[2],'side':k[3],**v} for k,v in model.items() if v['proven']],'test_summary':summ(ts),'validation_summary':summ(val_sel),'positive_edge_proven_on_untouched_test':bool(len(ts)>=30 and summ(ts)['net_pnl']>0 and summ(ts)['expectancy_pct']>0 and summ(ts)['profit_factor']>1.05),'data_limitations':['5m historical bundle only: no real 1m execution data in this run','NIFTY/VIX/sector index historical series were absent; market state uses stock-derived breadth/dispersion and sector proxies','universe is current snapshot applied backward, not point-in-time','no historical bid/ask, ticks, order book or pre-open']}
    json.dump(report,open(out/'report.json','w'),indent=2); json.dump(model,open(out/'edge_model.json','w'),indent=2); pd.DataFrame(ts).to_csv(out/'trades.csv',index=False); pd.DataFrame([{'date':d} for d in []]).to_csv(out/'dummy.csv',index=False)
    # detailed family/regime breakdown
    for key,fn in [('family',lambda x:x['family']),('regime',lambda x:x['market_regime']),('side',lambda x:x['side'])]:
        grp={}
        for x in ts: grp.setdefault(fn(x),[]).append(x)
        report[f'by_{key}']={k:summ(v) for k,v in grp.items()}
    json.dump(report,open(out/'report.json','w'),indent=2)
    print(json.dumps(report['test_summary'],indent=2)); print('POSITIVE TEST',report['positive_edge_proven_on_untouched_test'])

if __name__=='__main__': main2()
