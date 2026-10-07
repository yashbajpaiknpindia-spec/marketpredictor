from pathlib import Path
import ast
import math

ROOT=Path(__file__).resolve().parents[1]
APP=(ROOT/'app.py').read_text(encoding='utf-8')
HTML=(ROOT/'templates/index.html').read_text(encoding='utf-8')


def _extract_function_source(name):
    tree=ast.parse(APP)
    for node in tree.body:
        if isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef)) and node.name==name:
            lines=APP.splitlines()
            return '\n'.join(lines[node.lineno-1:node.end_lineno])
    raise AssertionError(name)


def test_parallel_parent_child_schema_and_scheduler_are_present():
    assert 'parent_run_id INTEGER' in APP
    assert 'is_shard BOOLEAN NOT NULL DEFAULT FALSE' in APP
    assert '_rapid_create_parallel_parent_and_children' in APP
    assert '_rapid_parallel_coordinator' in APP
    assert '_rapid_parallel_schedule_dates' in APP
    assert "parallel_schedule_mode':'SPREAD_ACROSS_PERIOD'" in APP
    assert "status IN ('pending','running','stopping') AND COALESCE(stop_requested,FALSE)=FALSE" in APP
    assert 'COALESCE(is_shard,FALSE)=FALSE' in APP


def test_parallel_scheduler_is_bounded_and_durable():
    assert '_rapid_parallel_lane_cap' in APP
    assert "parallel_batch_size: int=12" in APP
    assert 'Concurrent lane' in HTML
    assert 'Auto · memory-safe' in HTML
    assert 'current work is durably checkpointed' in APP
    assert 'durable acquisition checkpoint saved' in APP
    assert "active_day_state" in APP
    assert "batch_cursor" in APP


def test_live_status_contains_parallel_children_and_live_result():
    assert "payload['parallel_children']=parallel_children" in APP
    assert "payload['parallel_summary']" in APP
    assert "payload['live_result']=result" in APP
    assert 'const result = data.live_result || data.result_json' in HTML
    assert 'rapidParallelLanesCard' in HTML
    assert 'rapidParallelLanesBody' in HTML


def test_live_pnl_stock_and_technical_tables_are_marked_live():
    assert 'Session-by-session P/L · live' in HTML
    assert 'Stock exposure & outcome · live' in HTML
    assert 'Technical analysis · live' in HTML
    assert 'renderRapidForensics(data' in HTML


def test_parallel_estimate_reports_concurrent_memory_and_storage():
    assert 'estimated_peak_parallel_session_market_storage_bytes' in APP
    assert 'estimated_parallel_lane_working_set_mb' in APP
    assert 'estimated_parallel_speedup_upper_bound' in APP
    assert 'parallel_lanes' in APP
    assert 'parallel_batch_size' in APP
    assert 'parallel_batch_size:Number(document.getElementById' in HTML
    assert 'parallel_lanes:Number(document.getElementById' in HTML


def test_spread_scheduler_distributes_dates_across_period():
    src=_extract_function_source('_rapid_parallel_schedule_dates')
    import datetime
    import math
    ns={'List':list,'datetime':datetime,'math':math}
    exec(src,ns)
    fn=ns['_rapid_parallel_schedule_dates']
    import datetime as dt
    dates=[dt.date(2026,1,1)+dt.timedelta(days=i) for i in range(120)]
    out=fn(dates,12)
    assert len(out)==len(dates)
    assert sorted(out)==sorted(dates)
    # First scheduler wave must be spread through the range, not the first 12 consecutive days.
    first_wave=out[:12]
    assert first_wave[0]==dates[0]
    assert first_wave[-1]>dates[11]
    assert len(set(first_wave))==12
