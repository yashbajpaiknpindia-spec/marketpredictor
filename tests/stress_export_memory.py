"""Offline synthetic stress test. Run with OPENBLAS_NUM_THREADS=1 PYTHONPATH=. python tests/stress_export_memory.py."""
import os
# Keep BLAS virtual mappings bounded before importing the real application.
os.environ['OPENBLAS_NUM_THREADS'] = '1'
os.environ['OMP_NUM_THREADS'] = '1'
os.environ['INTRADAY_SERVER_AUTO_ON_START'] = 'false'
os.environ['PAPER_AUTO_ON_START'] = 'false'
os.environ['EXIT_MONITOR_SERVER_AUTO_ON_START'] = 'false'
os.environ['DATABASE_URL'] = ''
os.environ['HISTORICAL_EXPORT_PACE_SECONDS'] = '0'
os.environ['HISTORICAL_EXPORT_MEMORY_LIMIT_MB'] = '512'
os.environ.pop('UPSTOX_ACCESS_TOKEN', None)
os.environ.pop('DHAN_ACCESS_TOKEN', None)
import csv
import datetime as dt
import gzip
import io
import json
import resource
import sys
import tempfile
import time
import zipfile
from pathlib import Path
from unittest.mock import patch

import requests
import app as application
import historical_export as he
import historical_fallbacks as hf

# Cap total address space, which is stricter than an RSS-only limit for this process.
resource.setrlimit(resource.RLIMIT_AS, (512 * 1024**2, 512 * 1024**2))
started = time.monotonic()
report = {'test_type':'offline synthetic; real Flask application and exporter; mocked market APIs',
          'address_space_limit_mb':512, 'baseline_rss_mb':he._rss_mb()}

class Response:
    status_code = 200
    headers = {}
    def __init__(self, path): self.file = open(path,'rb')
    def raise_for_status(self): pass
    def iter_content(self,chunk_size):
        while True:
            b=self.file.read(chunk_size)
            if not b: return
            yield b
    def close(self): self.file.close()
    @property
    def content(self): raise AssertionError('unbounded response.content')
    @property
    def text(self): raise AssertionError('unbounded response.text')

with tempfile.TemporaryDirectory() as folder:
    master=Path(folder)/'master.json.gz'
    junk={'segment':'NSE_FO','instrument_type':'OPTIDX','name':'irrelevant derivative','details':'x'*500}
    encoded=json.dumps(junk).encode()
    count=500_000
    with gzip.open(master,'wb',compresslevel=1) as f:
        f.write(b'[')
        for i in range(count): f.write(encoded+b',')
        f.write(json.dumps({'segment':'NSE_INDEX','instrument_type':'INDEX','instrument_key':'NSE_INDEX|Nifty 50','name':'Nifty 50'}).encode()+b']')
    with patch.object(hf.requests,'get',lambda *a,**k: Response(master)):
        rows=hf.load_upstox_master('nse')
    assert len(rows)==1
    report['catalogue']={'discarded_derivative_records':count,'uncompressed_mb':len(encoded)*count/1024**2,
                         'retained_index_records':len(rows),'rss_after_mb':he._rss_mb()}
    he.EXPORT_DIR=folder
    he.PERSIST_DB=False
    # This host cgroup contains other tasks; measure the capped test process itself.
    he._memory_used_mb=he._rss_mb
    he._db_save_meta=lambda *a,**k:None
    he._resolve_equity=lambda m:(m['symbol'],'SYMBOL',m['symbol'])
    he.ind.get_nse_instrument_rows=lambda syms:{}
    he.ind.get_index_instrument_rows=lambda:[]
    start=dt.date(2026,6,1)
    end=dt.date(2026,8,31)
    nstocks=250
    sessions=sum((start+dt.timedelta(days=i)).weekday()<5 for i in range((end-start).days+1))
    def fetch(jid,codes,interval,ws,we):
        data={}
        for code in codes:
            candles=[]
            day=ws.date()
            while day<we.date():
                if day.weekday()<5:
                    ts=int(dt.datetime.combine(day,dt.time(9,15),he.IST).timestamp())
                    base=100+int(code[1:])
                    for i in range(375):
                        px=base+i*0.003
                        candles.append({'ts':ts+i*60,'o':px,'h':px+0.1,'l':px-0.1,'c':px+0.02,'v':1000+i})
                day+=dt.timedelta(days=1)
            data[code]=candles
        return data,[]
    he._fetch_resilient=fetch
    jid='abcdef123456'
    he._jobs[jid]={'job_id':jid,'status':'pending','progress':{},'warnings':[]}
    spec={'symbols':[{'symbol':f'S{i:03d}'} for i in range(nstocks)],'start':start,'end':end,
          'interval':'1m','include_indices':False,'extras':False,'compact':True}
    # Hard-fail if a missing mock would access a real service.
    with patch.object(requests.sessions.Session,'request',side_effect=AssertionError('network disabled')):
        he._run_job(jid,spec)
    job=he.get_job(jid)
    assert job['status']=='completed',job
    archive=he.zip_path_for_download(jid)
    with zipfile.ZipFile(archive) as z:
        assert z.testzip() is None
        assert len(z.namelist())==len(set(z.namelist()))
        with z.open('quality/data_quality.csv') as f:
            quality_count=0; bars=0
            for row in csv.DictReader(io.TextIOWrapper(f)):
                quality_count+=1; bars+=int(row['bars'])
        assert quality_count==nstocks*sessions
        assert bars==nstocks*sessions*375
        manifest=json.loads(z.read('manifest.json'))
        assert manifest['coverage']['stock_day_files']==quality_count
    client=application.app.test_client()
    response=client.get(f'/api/historical-export/{jid}/download',buffered=False)
    downloaded=sum(len(part) for part in response.response)
    assert response.status_code==200 and downloaded==os.path.getsize(archive)
    response.close()
    response=client.get(f'/api/historical-export/{jid}/download',headers={'Range':'bytes=0-1023'},buffered=False)
    assert response.status_code==206 and sum(len(c) for c in response.response)==1024
    response.close()
    report['export']={'stocks':nstocks,'sessions':sessions,'candles_verified':bars,'stock_day_files':quality_count,
                      'zip_mb':os.path.getsize(archive)/1024**2,'zip_crc':'PASS','duplicate_paths':0,
                      'http_download_bytes':downloaded,'http_range_resume':'PASS','rss_after_mb':he._rss_mb()}
report['peak_rss_mb']=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024
report['elapsed_seconds']=round(time.monotonic()-started,2)
report['status']='PASS'
Path('V3.2.5_STRESS_RESULTS.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(report,indent=2),flush=True)
