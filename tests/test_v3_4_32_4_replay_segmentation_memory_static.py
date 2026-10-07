from pathlib import Path
import ast

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "app.py").read_text(encoding="utf-8")


def _func(name):
    tree = ast.parse(APP)
    return next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)


def test_one_minute_multiday_start_is_always_segmented():
    fn = _func("intraday_replay_start_endpoint")
    src = ast.get_source_segment(APP, fn)
    assert "_probe_interval = str(get_intraday_replay_config().get('interval') or '1m').lower()" in src
    assert "_must_segment = len(_probe_days) > 1 and bool(re.match(r'^1m$', _probe_interval))" in src
    assert "if _must_segment or len(_probe_days) > INTRADAY_REPLAY_MAX_SELECTED_DAYS:" in src
    assert "start_extended_intraday_replay(_ext_body)" in src


def test_extended_batch_size_uses_authoritative_memory_estimate_and_1m_is_one_day():
    fn = _func("_extended_batch_size")
    src = ast.get_source_segment(APP, fn)
    assert "if minutes == 1:\n        return 1" in src
    assert "estimate_replay_memory_mb(universe_size, days, interval)" in src
    assert "est['estimated_mb'] < INTRADAY_REPLAY_RUNTIME_MEMORY_HARD_MB" in src


def test_memory_guard_message_is_not_used_to_block_safe_child_batches():
    assert "Long 1-minute replays are automatically segmented" in APP
    assert "this segment requested {len(trading_days)} day(s)" in APP
