import ast, datetime as dt, json, tempfile, zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / 'app.py'
SRC = APP.read_text()
mod = ast.parse(SRC)
helper = next(n for n in mod.body if isinstance(n, ast.FunctionDef) and n.name == '_collect_export_archive_inventory')
ns = {'Dict':dict,'List':list,'Any':object,'Tuple':tuple,'datetime':__import__('datetime'),'time':__import__('time'),'json':json,'hashlib':__import__('hashlib')}

class FakeExport:
    def __init__(self, jobs, access): self.jobs=jobs; self.access=access
    def list_jobs(self, limit=1000): return self.jobs
    def archive_access_status(self, jid): return self.access[jid]

class FakeMarket:
    def __call__(self, market, day): return day.weekday() < 5

def load_helper():
    ns['historical_export'] = None
    ns['is_trading_day'] = FakeMarket()
    ns['datetime'] = dt
    ns['time'] = __import__('time')
    code = compile(ast.Module(body=[helper], type_ignores=[]), str(APP), 'exec')
    exec(code, ns)
    return ns['_collect_export_archive_inventory']


def make_zip(path):
    with zipfile.ZipFile(path, 'w') as z:
        z.writestr('manifest.json', '{}')


def test_synthetic_1m_verified_export_is_visible_to_both_calendars():
    f=load_helper()
    with tempfile.TemporaryDirectory() as td:
        p=Path(td)/'export.zip'; make_zip(p)
        jobs=[{'job_id':'J1','status':'completed','archive_persisted':False,'size_bytes':p.stat().st_size,
               'params':{'interval':'1m','start_date':'2026-07-01','end_date':'2026-07-03'}}]
        ns['historical_export']=FakeExport(jobs, {'J1':{'accessible':True,'mode':'local','path':str(p)}})
        verified, recs, unavailable, unavailable_days = f('IN','1m')
        assert len(verified)==3, verified
        assert recs[0]['job_id']=='J1'
        assert not unavailable
        assert not unavailable_days


def test_synthetic_1m_unavailable_export_is_shown_as_unavailable_not_missing():
    f=load_helper()
    jobs=[{'job_id':'J2','status':'completed','archive_persisted':False,'size_bytes':123,
           'params':{'interval':'1m','start_date':'2026-07-06','end_date':'2026-07-08'}}]
    ns['historical_export']=FakeExport(jobs, {'J2':{'accessible':False,'reason':'metadata_only'}})
    verified, recs, unavailable, unavailable_days = f('IN','1m')
    assert not verified
    assert unavailable and unavailable[0]['job_id']=='J2'
    assert set(unavailable_days)=={'2026-07-06','2026-07-07','2026-07-08'}
    assert all(unavailable_days[d]['archive_access_reason']=='metadata_only' for d in unavailable_days)


def test_synthetic_5m_can_use_1m_archive_but_uses_distinct_resolution():
    f=load_helper()
    jobs=[{'job_id':'J3','status':'completed','archive_persisted':True,'size_bytes':999,
           'params':{'interval':'1m','start_date':'2026-08-03','end_date':'2026-08-05'}}]
    ns['historical_export']=FakeExport(jobs, {'J3':{'accessible':True,'mode':'postgres'}})
    verified, recs, unavailable, unavailable_days = f('IN','5m')
    assert len(verified)==3
    assert all(x['interval']=='1m' for x in verified.values())
    assert recs[0]['interval']=='1m'
    assert not unavailable
    assert not unavailable_days


def test_frontend_replay_and_export_both_use_shared_inventory_endpoint():
    html=(ROOT/'templates'/'index.html').read_text()
    endpoint='/api/data-inventory/coverage?market=${encodeURIComponent(marketCode)}&interval=${encodeURIComponent(replayInterval)}'
    assert endpoint in html
    assert '/api/data-inventory/coverage?market=${encodeURIComponent(market)}&interval=${encodeURIComponent(interval)}' in html
    assert "context:'replay'" in html
    assert "context:'export'" in html

if __name__=='__main__':
    tests=[v for k,v in globals().items() if k.startswith('test_') and callable(v)]
    for t in tests: t(); print('PASS',t.__name__)
    print(f'{len(tests)}/{len(tests)} passed')
