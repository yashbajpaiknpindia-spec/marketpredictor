import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / 'app.py').read_text(encoding='utf-8')
HTML = (ROOT / 'templates' / 'index.html').read_text(encoding='utf-8')


def _func(name):
    tree = ast.parse(APP)
    return next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)


def test_auto_mode_does_not_abort_when_a_covering_export_is_unavailable():
    fn = _func('_run_intraday_strategy_tournament_worker')
    src = ast.get_source_segment(APP, fn)
    assert "reusing every accessible archive first" in src
    assert "then downloading only ticker-days still missing" in src
    assert "Provider download was blocked to prevent duplicate historical downloads" not in src
    fn2 = _func('_run_intraday_replay_worker')
    src2 = ast.get_source_segment(APP, fn2)
    assert "reusing every accessible archive first" in src2
    assert "Provider download was blocked to prevent duplicate historical downloads" not in src2


def test_hydrator_keeps_source_interval_per_candidate():
    fn = _func('_hydrate_replay_cache_from_historical_exports')
    src = ast.get_source_segment(APP, fn)
    assert "job_source_interval" in src
    assert "candidates.append((str(job.get('updated_at') or job.get('created_at') or ''), job, access, source_interval))" in src
    assert "for _sort_key, job, _access, job_source_interval in candidates" in src
    assert "if job_source_interval == '1m' and target_interval == '5m'" in src


def test_export_inventory_is_cached_and_refresh_invalidates_it():
    assert '_EXPORT_ARCHIVE_INVENTORY_CACHE' in APP
    assert '_EXPORT_ARCHIVE_INVENTORY_TTL = 20.0' in APP
    assert "_EXPORT_ARCHIVE_INVENTORY_CACHE.pop(f'{market}:{interval}', None)" in APP


def test_browser_preflight_does_not_surface_raw_abort_reason():
    assert "controller.abort('stored-data-preflight-timeout')" in HTML
    assert "const timedOut = e?.name === 'AbortError'" in HTML
    assert 'signal is aborted without reason' not in HTML
