from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / 'app.py'
HTML = ROOT / 'templates' / 'index.html'


def text(p): return p.read_text()


def test_scalper_schema_migration_is_committed_before_other_delta():
    s = text(APP)
    a = s.index('def _apply_schema_compatibility_delta')
    b = s.index('# Market Data Library', a)
    block = s[a:b]
    assert "conn.commit()" in block
    assert 'scalper_live_settings' in block and 'scalper_live_sessions' in block


def test_partial_scalper_tables_are_repaired_additively():
    s = text(APP)
    a = s.index('scalper_column_repairs = {')
    b = s.index('# Commit the Scalper contract independently', a)
    block = s[a:b]
    for table in ('scalper_live_settings','scalper_live_sessions','scalper_live_snapshots','scalper_paper_trades'):
        assert table in block
    assert 'ADD COLUMN IF NOT EXISTS' in block


def test_request_path_never_runs_scalper_ddl():
    s = text(APP)
    assert '_SCALPER_SCHEMA_REQUEST_DDL_DISABLED = True' in s
    start = s.index('def _ensure_scalper_live_schema')
    end = s.index('def _scalper_schema_required', start)
    block = s[start:end]
    assert 'ADD COLUMN' not in block and 'CREATE TABLE' not in block


def test_explicit_scalper_schema_diagnostic_exists():
    s = text(APP)
    assert "@app.get('/api/scalper/live/schema-status')" in s
    assert "'request_path_ddl': False" in s


def test_explicit_scalper_market_status_exists():
    s = text(APP)
    assert "@app.get('/api/scalper/live/market-status')" in s
    assert "get_market_session_state('IN')" in s


def test_settings_save_has_explicit_arm_action():
    s = text(APP)
    assert "arm_action = str(payload.get('_arm_action') or 'save').lower()" in s
    assert "if arm_action == 'arm':" in s
    assert "elif arm_action == 'disarm':" in s
    assert "merged['enabled'] = bool(current.get('enabled'))" in s


def test_ui_save_and_arm_are_distinct():
    s = text(HTML)
    assert 'saveScalperLiveSettings(true,this)' in s
    assert 'saveScalperLiveSettings(undefined,this)' in s
    assert 'checkScalperMarketStatus(this)' in s
    assert "payload._arm_action=action" in s


def test_worker_start_does_not_lock_snapshot_state():
    s = text(ROOT / 'research' / 'scalper' / 'scalper_live_paper.py')
    a = s.index('def start(self)')
    b = s.index('def stop(self)', a)
    block = s[a:b]
    assert 'return {"ok": True' in block
    assert 'snapshot_state()' in block
    # snapshot_state must be called after the with self.lock block closes.
    assert block.index('self.thread.start()') < block.index('return {"ok": True')


def test_autostart_does_not_expire_after_two_minutes():
    s = text(APP)
    a = s.index('def _scalper_autostart_loop')
    b = s.index('threading.Thread(target=_scalper_autostart_loop', a)
    block = s[a:b]
    assert 'while True:' in block
    assert 'range(24)' not in block


def test_old_data_is_not_rewritten_by_compression_path():
    s = text(APP)
    assert '_SCALPER_DEPTH_CODEC = \'zlib-json-v1\'' in s
    assert 'depth_payload BYTEA' in s
    # New compression is an additive column; legacy JSONB ladder columns remain.
    assert "bid_prices JSONB NOT NULL DEFAULT '[]'::jsonb" in s
