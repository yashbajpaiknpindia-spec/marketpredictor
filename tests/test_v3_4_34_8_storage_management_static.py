from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
APP=(ROOT/"app.py").read_text(encoding="utf-8")
HIST=(ROOT/"historical_export.py").read_text(encoding="utf-8")
HTML=(ROOT/"templates/storage.html").read_text(encoding="utf-8")
INDEX=(ROOT/"templates/index.html").read_text(encoding="utf-8")

def test_storage_management_schema_and_cleanup_ledger():
    assert "def _ensure_storage_management_schema" in APP
    assert "CREATE TABLE IF NOT EXISTS storage_cleanup_log" in APP
    assert "archive_sha256" in APP
    assert "@app.route('/api/storage/cleanup-preview'" in APP
    assert "@app.route('/api/storage/cleanup-history'" in APP

def test_storage_deletion_is_exact_candidate_based():
    assert "candidate_ids=body.get('candidate_ids')" in APP
    assert "DELETE FROM historical_export_archive_chunks WHERE job_id=ANY(%s)" in APP
    assert "INSERT INTO storage_cleanup_log" in APP
    assert "Exact deleted records are stored in Cleanup History." in APP

def test_historical_export_hash_and_permanent_delete():
    assert "archive_sha256 VARCHAR(64)" in HIST
    assert "digest = hashlib.sha256()" in HIST
    assert "def delete_job_permanently(job_id: str)" in HIST
    assert "DELETE FROM historical_export_jobs WHERE job_id=%s" in HIST

def test_storage_ui_has_deep_review_and_delete_controls():
    for text in (
        "Review exact records", "Delete this replay", "Delete job + DB chunks",
        "Cleanup history — what was actually deleted", "Historical Export archive review",
        "storage_cleanup_log", "PROTECTED", "Page / component",
    ):
        assert text in HTML

def test_storage_nav_deduplicated_and_svg():
    assert INDEX.count('href="/storage"') == 1
    assert '<svg' in INDEX
    assert 'aria-label="Database storage"' in INDEX
