from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "app.py").read_text(encoding="utf-8")
HTML = (ROOT / "templates" / "index.html").read_text(encoding="utf-8")

def test_memory_guard_defaults_to_advisory():
    assert "INTRADAY_REPLAY_MEMORY_GUARD_MODE = os.environ.get('INTRADAY_REPLAY_MEMORY_GUARD_MODE', 'warn')" in APP
    assert "if mode == 'strict':" in APP

def test_preflight_no_longer_has_legacy_unconditional_refusal():
    assert "Estimated memory for this segment (~" not in APP
    assert "Strict memory guard mode is enabled." in APP

def test_runtime_memory_does_not_raise_in_warn_or_off_mode():
    idx = APP.index("def _enforce_replay_runtime_memory")
    body = APP[idx: idx + 5000]
    assert "if mode == 'strict':" in body
    assert "raise MemoryError" in body  # strict-only escape hatch

def test_one_day_extended_replay_uses_reduced_warmup():
    assert "replay_warmup_days_override=(1 if str(get_intraday_replay_config().get('interval') or '1m').lower() == '1m' else None)" in APP
    assert "settings.get('_replay_warmup_days')" in APP

def test_ui_no_longer_claims_hard_internal_memory_blocker():
    assert "Hard Render memory protection cannot be bypassed" not in HTML
    assert "Memory guard is advisory by default" in HTML

def test_no_shard_nameerror_symbol():
    assert not re.search(r"\bshard_count\b", APP)


def test_cache_matrix_uses_bounded_server_side_fetch():
    start = APP.index("def _load_cached_replay_matrix")
    end = APP.index("def _store_cached_replay_days_bulk", start)
    body = APP[start:end]
    assert "cursor(name=" in body
    assert "fetchmany(16)" in body
    assert "fetchmany(8)" in body
    assert "cur.fetchall()" not in body

