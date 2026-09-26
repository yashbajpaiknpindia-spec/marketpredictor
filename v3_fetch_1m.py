"""CLI for exact 1-minute historical backfill into V3.1.

Examples:
  python v3_fetch_1m.py --source dhan --mapping dhan_symbols.csv --from 2026-06-24 --to 2026-09-18 --out data/1m_raw
  python v3_fetch_1m.py --source upstox --mapping upstox_symbols.csv --from 2026-06-24 --to 2026-09-18 --out data/1m_raw

Mapping CSVs must contain: symbol,security_id (Dhan) or symbol,instrument_key (Upstox).
Credentials are read from environment variables and never written to outputs.
"""
from __future__ import annotations
import argparse, json, time
from pathlib import Path
import pandas as pd
import v3_data_acquisition as acq

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--source',choices=['dhan','upstox'],required=True)
    ap.add_argument('--mapping',required=True)
    ap.add_argument('--from',dest='start',required=True)
    ap.add_argument('--to',dest='end',required=True)
    ap.add_argument('--out',required=True)
    ap.add_argument('--sleep',type=float,default=0.15)
    args=ap.parse_args()
    mp=pd.read_csv(args.mapping)
    outdir=Path(args.out);outdir.mkdir(parents=True,exist_ok=True)
    logs=[];allparts=[]
    for i,r in mp.iterrows():
        sym=str(r.get('symbol') or '').upper().strip()
        try:
            if args.source=='dhan':
                d=acq.download_dhan_intraday(security_id=str(r['security_id']),from_date=args.start,to_date=args.end,interval=1)
            else:
                d=acq.download_upstox_1m(instrument_key=str(r['instrument_key']),from_date=args.start,to_date=args.end)
            if len(d):
                d['symbol']=sym
                p=outdir/f'{sym}_1m.csv.gz';d.to_csv(p,index=False,compression='gzip')
                logs.append({'symbol':sym,'rows':len(d),'file':str(p),'status':'ok'})
                allparts.append(d)
            else:
                logs.append({'symbol':sym,'rows':0,'status':'empty'})
        except Exception as e:
            logs.append({'symbol':sym,'rows':0,'status':'error','error':str(e)})
        if args.sleep: time.sleep(args.sleep)
    manifest={'source':args.source,'from':args.start,'to':args.end,'requested_symbols':len(mp),'results':logs}
    (outdir/'fetch_manifest.json').write_text(json.dumps(manifest,indent=2,default=str))
    if allparts:
        full=pd.concat(allparts,ignore_index=True);full['timestamp']=pd.to_datetime(full['timestamp'],utc=True);full=full.sort_values(['day','symbol','timestamp']);
        audit=acq.audit_minute(full);(outdir/'audit.json').write_text(json.dumps(audit.__dict__,indent=2))
        five=acq.resample_completed_5m(full);five.to_csv(outdir/'derived_5m.csv.gz',index=False,compression='gzip')
    print(json.dumps({'requested':len(mp),'ok':sum(x['status']=='ok' for x in logs),'errors':sum(x['status']=='error' for x in logs)},indent=2))
if __name__=='__main__': main()
