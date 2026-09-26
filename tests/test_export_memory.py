import csv
import datetime as dt
import gzip
import io
import json
import os
import threading
import zipfile
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock
from pathlib import Path

import pytest
import requests

import historical_export as he
import historical_fallbacks as hf
import indstocks_client as ind
from export_streaming import DiskRows, json_master_rows, response_file, response_json, write_csv_entry


class StreamResponse:
    def __init__(self, source, status=200):
        self.source = source
        self.status_code = status
        self.headers = {}
        self.closed = False
    def raise_for_status(self):
        if self.status_code != 200:
            raise requests.HTTPError(str(self.status_code))
    def iter_content(self, chunk_size):
        while True:
            block = self.source.read(chunk_size)
            if not block:
                return
            yield block
    @property
    def content(self):
        raise AssertionError('Whole response buffered')
    @property
    def text(self):
        raise AssertionError('Whole response decoded')
    def close(self):
        self.closed = True
        self.source.close()


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setattr(requests.sessions.Session, 'request', Mock(side_effect=AssertionError('Unexpected live HTTP')))
    monkeypatch.setattr(he, '_db_save_meta', lambda *a, **k: None)
    hf._load_upstox_master.cache_clear()
    hf._load_dhan_master.cache_clear()


def test_missing_credentials_skip_catalogues(monkeypatch):
    monkeypatch.delenv('UPSTOX_ACCESS_TOKEN', raising=False)
    monkeypatch.delenv('DHAN_ACCESS_TOKEN', raising=False)
    for fn in (hf.fetch_upstox_index, hf.fetch_upstox_global, hf.fetch_dhan_index_by_names):
        with pytest.raises(hf.FallbackError, match='CREDENTIAL_MISSING'):
            fn(['NIFTY'], '1m', dt.date(2026, 1, 1), dt.date(2026, 1, 2))


@pytest.mark.parametrize('compressed', [True, False])
def test_upstox_master_streamed_filtered(monkeypatch, compressed):
    rows = [{'segment':'NSE_FO', 'instrument_type':'OPTIDX', 'instrument_key':str(i)} for i in range(2000)]
    wanted = {'segment':'NSE_INDEX','instrument_type':'INDEX','instrument_key':'NSE_INDEX|Nifty 50','name':'Nifty 50'}
    rows.append(wanted)
    data = json.dumps(rows).encode()
    r = StreamResponse(io.BytesIO(gzip.compress(data) if compressed else data))
    get = Mock(return_value=r)
    monkeypatch.setattr(hf.requests, 'get', get)
    assert hf.load_upstox_master('nse') == [wanted]
    assert hf.resolve_upstox_index(['Nifty 50']) == wanted
    assert get.call_count == 1 and get.call_args.kwargs['stream'] and r.closed


def test_dhan_master_streamed_filtered(monkeypatch):
    r = StreamResponse(io.BytesIO(b'INSTRUMENT,DISPLAY_NAME,SECURITY_ID\nOPTIDX,Option,1\nINDEX,NIFTY 50,13\n'))
    monkeypatch.setattr(hf.requests, 'get', lambda *a, **k: r)
    assert len(hf.load_dhan_master()) == 1
    assert hf.resolve_dhan_index(['NIFTY 50'])['_security_id'] == '13'
    assert r.closed


def test_response_limit_and_http_errors_close():
    r = StreamResponse(io.BytesIO(b'x' * 100))
    with pytest.raises(ValueError, match='size limit'):
        with response_file(r, max_bytes=20):
            pass
    assert r.closed
    r = StreamResponse(io.BytesIO(b'oops'), 503)
    with pytest.raises(requests.HTTPError):
        response_json(r)
    assert r.closed


def test_report_spool_order_counts_and_csv(tmp_path):
    rows = DiskRows(tmp_path/'report.sqlite')
    rows.append({'symbol':'B','date':'2026-01-02','complete':False})
    rows.append({'symbol':'A','date':'2026-01-01','complete':True})
    with zipfile.ZipFile(tmp_path/'report.zip', 'w') as z:
        write_csv_entry(z, 'q.csv', ['symbol','date','complete'], rows)
    assert len(rows) == 2 and rows.incomplete == 1
    with zipfile.ZipFile(tmp_path/'report.zip') as z:
        assert list(csv.DictReader(io.StringIO(z.read('q.csv').decode())))[0]['symbol'] == 'A'
    rows.close()


def test_ind_master_accepts_stream_and_preserves_isin():
    result = ind._parse_equity_csv(iter(['TRADING_SYMBOL,EXCH,SECURITY_ID,ISIN','ABC,NSE,10,IN1','ABC,BSE,20,IN1']))
    assert result['map']['ABC'] == {'NSE':'10','BSE':'20'}
    assert result['isin']['IN1'] == 'ABC'
    assert len(result['rows']) == 1


def candle(day, minute=0, volume=10):
    ts = int(dt.datetime.combine(day, dt.time(9,15), he.IST).timestamp()) + minute * 60
    return {'ts':ts,'o':100,'h':101,'l':99,'c':100.5,'v':volume}


def test_index_fallback_windowed_and_fenced(monkeypatch, tmp_path):
    monkeypatch.setenv('UPSTOX_ACCESS_TOKEN','test')
    calls = []
    def fetch(names, interval, start, end):
        calls.append((start,end))
        # Include an out-of-window bar to test inclusive-end provider responses.
        return [candle(start), candle(end + dt.timedelta(days=1))], {'instrument_key':'test','provider':'Upstox'}
    monkeypatch.setattr(hf,'fetch_upstox_index',fetch)
    guard = Mock()
    result = he._spool_index_fallback('Upstox',['NIFTY'],'1m',dt.date(2026,6,1),dt.date(2026,6,30),None,str(tmp_path/'n.csv'),True,guard)
    assert len(calls) == 5 and all((b-a).days < 7 for a,b in calls)
    assert result['rows'] == 5 and guard.call_count == 5
    with open(tmp_path/'n.csv') as f:
        assert sum(1 for _ in csv.DictReader(f)) == 5


@pytest.fixture
def job_env(monkeypatch, tmp_path):
    monkeypatch.setattr(he, 'EXPORT_DIR', str(tmp_path))
    monkeypatch.setattr(he, 'PERSIST_DB', False)
    monkeypatch.setattr(he, '_memory_used_mb', lambda: 100)
    monkeypatch.setattr(he, '_memory_limit_mb', lambda: 512)
    monkeypatch.setattr(he, '_release_heap', lambda: None)
    monkeypatch.setattr(he, '_pace_for', lambda i: 0)
    monkeypatch.setattr(he, '_resolve_equity', lambda m: (m['symbol'],'SYMBOL',m['symbol']))
    monkeypatch.setattr(ind, 'get_nse_instrument_rows', lambda s: {})
    monkeypatch.setattr(ind, 'get_index_instrument_rows', lambda: [])
    monkeypatch.setattr(ind, 'credentials_configured', lambda: True)
    he._jobs.clear()
    return tmp_path


def seed_job(spec):
    jid = '123456abcdef'
    he._jobs[jid] = {'job_id':jid,'status':'pending','progress':{},'warnings':[],'created_at':'2026-09-25T00:00:00Z'}
    he._run_job(jid,spec)
    assert he._jobs[jid]['status'] == 'completed', he._jobs[jid]
    return jid


def test_end_to_end_equity_fallback_and_integrity(monkeypatch, job_env):
    monkeypatch.setenv('UPSTOX_ACCESS_TOKEN','test')
    d1,d2 = dt.date(2026,6,1),dt.date(2026,6,2)
    monkeypatch.setattr(he, '_fetch_resilient', lambda jid,codes,iv,ws,we: ({c: [candle(d1),candle(d2,-0,-2)] if c=='A' else [candle(d1)] for c in codes},[]))
    calls=[]
    def fetch(isin,interval,start,end):
        calls.append((start,end))
        return [candle(start)], {'instrument_key':'NSE_EQ|'+isin}
    monkeypatch.setattr(hf,'fetch_upstox_equity',fetch)
    jid=seed_job({'symbols':[{'symbol':'A','isin':'INA'},{'symbol':'B','isin':'INB'}], 'start':d1,'end':d2,'interval':'1m','include_indices':False,'extras':False})
    assert calls == [(d2,d2)]
    with zipfile.ZipFile(he.zip_path_for_download(jid)) as z:
        assert z.testzip() is None
        assert len(z.namelist()) == len(set(z.namelist()))
        quality=list(csv.DictReader(io.StringIO(z.read('quality/data_quality.csv').decode())))
        assert len(quality)==4
        assert sum(int(r['bars']) for r in quality)==4
        assert len(list(csv.DictReader(io.StringIO(z.read('quality/data_anomalies.csv').decode()))))==1
        assert json.loads(z.read('manifest.json'))['coverage']['stock_day_files']==4
    assert not list(job_env.glob('*.spool'))


def test_persistence_holds_job_slot(monkeypatch, job_env):
    monkeypatch.delenv('UPSTOX_ACCESS_TOKEN',raising=False)
    day=dt.date(2026,6,1)
    spec={'symbols':[{'symbol':'A'}], 'start':day,'end':day,'interval':'1m','include_indices':False,'extras':False}
    monkeypatch.setattr(he,'_fetch_resilient',lambda jid,codes,*a: ({c:[candle(day)] for c in codes},[]))
    monkeypatch.setattr(he,'PERSIST_DB',True)
    def save(jid,path):
        assert he.any_running()==jid
        assert he.get_job(jid)['stage']=='persisting'
        assert not he.start_job(spec)['ok']
        return True
    monkeypatch.setattr(he,'_db_store_archive',save)
    jid=seed_job(spec)
    assert he.get_job(jid)['archive_persisted']


def test_cancel_before_fetch_cleans_partial(monkeypatch, job_env):
    jid='123456abcdef'
    he._jobs[jid]={'job_id':jid,'status':'pending','progress':{},'_cancel':True}
    day=dt.date(2026,6,1)
    he._run_job(jid,{'symbols':[{'symbol':'A'}], 'start':day,'end':day,'interval':'1m','include_indices':False,'extras':False})
    assert he._jobs[jid]['status']=='cancelled'
    assert not list(job_env.glob('*.zip*')) and not list(job_env.glob('*.spool'))


def test_restore_is_serialized(monkeypatch,tmp_path):
    target=str(tmp_path/'out.zip')
    calls=[]
    def restore(jid,path):
        calls.append(jid)
        with open(path,'wb') as f: f.write(b'archive')
        return True
    monkeypatch.setattr(he,'_db_restore_archive_unlocked',restore)
    with ThreadPoolExecutor(max_workers=4) as pool:
        assert all(pool.map(lambda _: he._db_restore_archive('123456abcdef',target),range(8)))
    assert len(calls)==1


def test_gunicorn_defaults():
    import runpy
    config=runpy.run_path('gunicorn.conf.py')
    assert config['workers']==1 and config['worker_class']=='gthread'
    assert config['max_requests']==0


@pytest.mark.parametrize('size_ok',[True,False])
def test_archive_restore_chunk_cursor_and_size_check(monkeypatch,tmp_path,size_ok):
    chunks=[memoryview(b'abc'),memoryview(b'def')]
    class Cursor:
        def __enter__(self): return self
        def __exit__(self,*args): pass
        def execute(self,*args): pass
        def __iter__(self):
            assert self.itersize==1
            return iter((c,) for c in chunks)
    class Conn:
        closed=False
        def cursor(self,**kwargs):
            assert kwargs.get('name')
            return Cursor()
        def close(self): self.closed=True
    conn=Conn()
    monkeypatch.setattr(he,'_db_load_meta',lambda jid:{'archive_persisted':True,'size_bytes':6 if size_ok else 7})
    monkeypatch.setattr(he,'_db_conn',lambda:conn)
    dest=tmp_path/'restored.zip'
    assert he._db_restore_archive('123456abcdef',str(dest)) is size_ok
    assert conn.closed
    if size_ok: assert dest.read_bytes()==b'abcdef'
    else: assert not dest.exists()
    assert not Path(str(dest)+'.restore.partial').exists()


def test_memory_pressure_stops_before_provider(monkeypatch,job_env):
    monkeypatch.setattr(he,'_memory_used_mb',lambda:490)
    monkeypatch.setattr(he,'MEMORY_WAIT_SECONDS',0)
    monkeypatch.setattr(he.time,'sleep',lambda _:None)
    fetch=Mock(side_effect=AssertionError('fetch under memory pressure'))
    monkeypatch.setattr(he,'_fetch_resilient',fetch)
    jid='123456abcdef'
    he._jobs[jid]={'job_id':jid,'status':'pending','progress':{}}
    day=dt.date(2026,6,1)
    he._run_job(jid,{'symbols':[{'symbol':'A'}],'start':day,'end':day,'interval':'1m','include_indices':False,'extras':False})
    assert he.get_job(jid)['status']=='error'
    assert 'memory' in he.get_job(jid)['error']
    fetch.assert_not_called()
    assert not list(job_env.glob('*.spool'))


def test_fallback_failure_does_not_publish_partial_index(monkeypatch,job_env):
    monkeypatch.setenv('UPSTOX_ACCESS_TOKEN','test')
    monkeypatch.setenv('DHAN_ACCESS_TOKEN','test')
    first,last=dt.date(2026,6,1),dt.date(2026,6,15)
    monkeypatch.setattr(he,'_resolve_index_series',lambda *a:([],[{'series':'NIFTY','index_name':'NIFTY 50'}],[]))
    monkeypatch.setattr(he,'_fetch_resilient',lambda jid,codes,iv,ws,we:({c:[candle(ws.date())] for c in codes},[]))
    upcalls=[]
    def up(*args):
        upcalls.append(1)
        if len(upcalls)>1: raise hf.FallbackError('provider failed on second window')
        return [candle(first)],{'instrument_key':'upstox'}
    monkeypatch.setattr(hf,'fetch_upstox_index',up)
    monkeypatch.setattr(hf,'fetch_dhan_index_by_names',lambda names,iv,a,b:([candle(a.date())],{'instrument_key':'dhan'}))
    jid=seed_job({'symbols':[{'symbol':'A'}],'start':first,'end':last,'interval':'1m','include_indices':True,'extras':False})
    with zipfile.ZipFile(he.zip_path_for_download(jid)) as z:
        rows=list(csv.DictReader(io.StringIO(z.read('market/NIFTY.csv').decode())))
        assert len(rows)==3
        attempts=list(csv.DictReader(io.StringIO(z.read('reference/fallback_provider_attempts.csv').decode())))
        assert [(r['provider'],r['status']) for r in attempts]==[('Upstox','FAILED'),('Dhan','SUCCESS')]


def test_equity_fallback_failure_preserves_later_days(monkeypatch,job_env):
    monkeypatch.setenv('UPSTOX_ACCESS_TOKEN','test')
    first=dt.date(2026,6,1)
    days=[first+dt.timedelta(days=i) for i in range(3)]
    monkeypatch.setattr(he,'_fetch_resilient',lambda jid,codes,*a:({c:[candle(d) for d in days] if c=='A' else [] for c in codes},[]))
    calls=[]
    def fetch(isin,iv,start,end):
        calls.append(start)
        if start==days[1]: raise hf.FallbackError('temporary failure')
        return [candle(start)],{'instrument_key':'NSE_EQ|'+isin}
    monkeypatch.setattr(hf,'fetch_upstox_equity',fetch)
    jid=seed_job({'symbols':[{'symbol':'A','isin':'INA'},{'symbol':'B','isin':'INB'}],'start':first,'end':days[-1],'interval':'1m','include_indices':False,'extras':False})
    assert calls==days
    with zipfile.ZipFile(he.zip_path_for_download(jid)) as z:
        assert f'historical_data/{days[-1]}/B.csv' in z.namelist()
        assert f'historical_data/{days[1]}/B.csv' not in z.namelist()
        attempts=list(csv.DictReader(io.StringIO(z.read('reference/fallback_provider_attempts.csv').decode())))
        assert attempts[-1]['status']=='PARTIAL' and attempts[-1]['rows']=='2'


def test_container_memory_includes_other_processes(monkeypatch):
    from unittest.mock import mock_open
    real_open=open
    def fake_open(path,*a,**k):
        if path=='/sys/fs/cgroup/memory.current': return io.StringIO(str(400*1024**2))
        if path=='/sys/fs/cgroup/memory.stat': return io.StringIO(f'inactive_file {50*1024**2}\n')
        return real_open(path,*a,**k)
    monkeypatch.setattr(he,'_rss_mb',lambda:100)
    monkeypatch.setattr('builtins.open',fake_open)
    assert he._memory_used_mb()==350


def test_oversized_extras_report_error_without_losing_candles(monkeypatch,tmp_path):
    import historical_extras as hx
    ctx=hx.ExtrasContext(dt.date(2026,1,1),dt.date(2026,1,2),None,[],[],lambda:False,lambda s:None,
                        memory_check=lambda: (_ for _ in ()).throw(hx.DatasetError('memory headroom')))
    def rows():
        hx._check(ctx)
        yield {'a':1}
    with zipfile.ZipFile(tmp_path/'extras.zip','w') as z:
        z.writestr('candles.csv','preserved')
        result=hx._stream_dataset(z,'extras.csv',['a'],rows())
        assert result['error']=='memory headroom' and not result['wrote']
    with zipfile.ZipFile(tmp_path/'extras.zip') as z:
        assert z.read('candles.csv')==b'preserved'


@pytest.mark.parametrize('interval',['1m','5m','15m'])
def test_estimate_matches_safe_request_batching(monkeypatch,interval):
    monkeypatch.setattr(he,'_memory_limit_mb',lambda:512)
    estimate=he.estimate(10,interval,dt.date(2026,6,1),dt.date(2026,6,7),5,False)
    assert estimate['api_calls_est']==10
    assert estimate['gentle_mode']['workers']==1
