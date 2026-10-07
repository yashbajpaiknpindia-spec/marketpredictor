from pathlib import Path
import gzip
import json

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "app.py").read_text(encoding="utf-8")
HTML = (ROOT / "templates" / "index.html").read_text(encoding="utf-8")


def test_existing_db_repair_is_additive_and_scalper_schema_is_in_compatibility_delta():
    start = APP.index('def _apply_schema_compatibility_delta')
    end = APP.index('def _log_advisory_lock_holders', start)
    block = APP[start:end]
    assert 'CREATE TABLE IF NOT EXISTS scalper_live_settings' in block
    assert 'CREATE TABLE IF NOT EXISTS scalper_live_sessions' in block
    assert 'CREATE TABLE IF NOT EXISTS scalper_live_snapshots' in block
    assert 'CREATE TABLE IF NOT EXISTS scalper_paper_trades' in block
    # The existing-DB compatibility path must never rewrite/delete historical rows.
    assert 'UPDATE scalper_' not in block
    assert 'DELETE FROM scalper_' not in block
    assert 'TRUNCATE' not in block


def test_new_snapshots_use_compressed_depth_and_legacy_rows_remain_readable():
    assert "_SCALPER_DEPTH_CODEC = 'zlib-json-v1'" in APP
    assert '_scalper_compress_depth_payload' in APP
    assert 'json.dumps([]), json.dumps([]), json.dumps([]), json.dumps([]), _scalper_compress_depth_payload(r), _SCALPER_DEPTH_CODEC' in APP
    assert '_scalper_restore_depth_fields' in APP
    assert 'depth_payload BYTEA' in APP


def test_v12_zero_minimum_edge_can_be_saved():
    assert "merged['min_remaining_edge_pct'] = max(0.0, min(float(merged.get('min_remaining_edge_pct') if merged.get('min_remaining_edge_pct') is not None else 0.0), 2.0))" in APP


def test_save_buttons_are_busy_and_arm_path_is_present():
    assert 'saveScalperLiveSettings(true,this)' in HTML
    assert 'saveScalperLiveSettings(undefined,this)' in HTML
    assert "'/api/scalper/live/settings'" in HTML
    assert "'/api/scalper/live/start'" in APP
    assert "'/api/scalper/live/stop'" in APP


def test_compressed_l5_round_trip_preserves_ladder_and_legacy_rows():
    namespace = {}
    import json as _json, zlib as _zlib
    from typing import Any, Dict, Optional
    namespace.update(json=_json, zlib=_zlib, Any=Any, Dict=Dict, Optional=Optional)
    src = APP[APP.index("_SCALPER_DEPTH_CODEC"):APP.index("# Legacy DDL list retained", APP.index("_SCALPER_DEPTH_CODEC"))]
    exec(src, namespace)
    row = {
        'bid_prices': [{'price': 100.1, 'qty': 10}], 'bid_qtys': [{'price': 100.1, 'qty': 10}],
        'ask_prices': [{'price': 100.2, 'qty': 12}], 'ask_qtys': [{'price': 100.2, 'qty': 12}],
    }
    blob = namespace['_scalper_compress_depth_payload'](row)
    assert isinstance(blob, bytes) and blob
    restored = namespace['_scalper_restore_depth_fields']({
        'bid_prices': [], 'bid_qtys': [], 'ask_prices': [], 'ask_qtys': [],
        'depth_payload': blob, 'depth_payload_codec': 'zlib-json-v1'
    })
    assert restored['bid_prices'] == row['bid_prices']
    assert restored['bid_qtys'] == row['bid_qtys']
    assert restored['ask_prices'] == row['ask_prices']
    assert restored['ask_qtys'] == row['ask_qtys']
    legacy = dict(row)
    legacy['depth_payload'] = blob
    legacy['depth_payload_codec'] = 'zlib-json-v1'
    legacy_restored = namespace['_scalper_restore_depth_fields'](legacy)
    assert legacy_restored['bid_prices'] == row['bid_prices']
    assert legacy_restored['ask_prices'] == row['ask_prices']


def test_worker_start_no_longer_calls_snapshot_state_while_state_lock_is_held():
    src = Path(ROOT / 'research' / 'scalper' / 'scalper_live_paper.py').read_text(encoding='utf-8')
    start = src.index('    def start(self) -> Dict[str, Any]:')
    end = src.index('    def stop(self) -> Dict[str, Any]:', start)
    block = src[start:end]
    assert 'with self.lock:' in block
    assert 'return {"ok": True, "state": self.snapshot_state()}' not in block
    assert 'return {"ok": True, "already_running": already_running, "started": not already_running, "state": self.snapshot_state()}' in block
