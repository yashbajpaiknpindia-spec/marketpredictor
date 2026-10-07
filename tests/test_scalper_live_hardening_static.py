from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / 'app.py').read_text(encoding='utf-8')
HTML = (ROOT / 'templates' / 'index.html').read_text(encoding='utf-8')


def test_scalper_json_safe_and_self_healing_settings_schema():
    assert 'def _scalper_json_safe' in APP
    assert 'ALTER TABLE scalper_live_settings ADD COLUMN IF NOT EXISTS' in APP
    assert "return _scalper_json_response({'ok': True, 'settings': result['settings']" in APP


def test_scalper_live_ui_has_distinct_ids_and_prominent_banner():
    assert HTML.count('id="scalperLivePill"') == 1
    assert HTML.count('id="scalperLivePillTop"') == 1
    assert 'id="scalperLiveBanner"' in HTML
    assert 'LIVE · PAPER' in HTML
    assert '_scalperFetchJson' in HTML
    assert 'Notification.requestPermission' in HTML


def test_scalper_boot_recovers_persisted_arm_state():
    assert 'def _scalper_autostart_loop' in APP
    assert 'SCALPER_AUTOSTART_ON_BOOT' in APP
    assert "worker.start()" in APP
