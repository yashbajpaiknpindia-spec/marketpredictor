from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / 'app.py').read_text(encoding='utf-8')
HTML = (ROOT / 'templates' / 'index.html').read_text(encoding='utf-8')
HIST = (ROOT / 'historical_export.py').read_text(encoding='utf-8')


def test_replay_inventory_uses_current_replay_interval_not_hardcoded_5m():
    assert "const replayInterval=String(irLastDataPlan?.interval||'1m').toLowerCase();" in HTML
    assert "interval=${encodeURIComponent(replayInterval)}" in HTML
    assert "interval=5m${forceRefresh" not in HTML


def test_replay_inventory_refreshes_when_plan_interval_changes():
    assert "!STORED_DATA_INVENTORY || STORED_DATA_INVENTORY.interval !== d.interval" in HTML


def test_inventory_checks_accessible_export_archive_not_only_persistence_flag():
    assert "archive_access_status(str(job.get('job_id') or ''))" in APP
    start = APP.index('def _collect_export_archive_inventory')
    end = APP.index('def _build_stored_data_inventory', start)
    block = APP[start:end]
    assert "if access.get('accessible'):" in block
    assert "elif token not in export_days:" in block
    assert "if not job.get('archive_persisted'):" not in block


def test_inventory_reports_unavailable_export_archives_separately():
    assert "'unavailable_export_archives': export_unavailable" in APP


def test_large_one_minute_export_can_be_persisted_for_replay_reuse():
    assert 'HISTORICAL_EXPORT_DB_MAX_MB", "512"' in HIST
    assert 'HISTORICAL_EXPORT_DB_KEEP", "2"' in HIST
    assert 'def persist_existing_archive(job_id: str)' in HIST
    assert "historical_export_persist_endpoint" in APP
    assert "Make Replay-persistent" in HTML
