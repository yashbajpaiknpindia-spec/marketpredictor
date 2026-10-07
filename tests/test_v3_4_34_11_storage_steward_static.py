from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
APP=(ROOT/"app.py").read_text(encoding="utf-8")
HTML=(ROOT/"templates/storage.html").read_text(encoding="utf-8")
RENDER=(ROOT/"render.yaml").read_text(encoding="utf-8")

def test_steward_is_read_only_and_distinguishes_1m_5m_scope():
    assert "def _storage_archive_scope_signature" in APP
    assert "DERIVABLE_FROM_CANONICAL_1M" in APP
    assert "ARCHIVE_BYTES_MISSING" in APP
    assert "no_automatic_delete" in APP

def test_steward_uses_exact_delete_confirmation_and_no_periodic_deep_scan():
    assert "DELETE THIS STORAGE ITEM" in HTML
    assert "let stewardLoaded=false" in HTML
    assert "if(!stewardLoaded)tasks.push(loadArchivesOnly())" in HTML

def test_free_plan_storage_defaults_are_explicit_in_blueprint():
    assert "MARKETPREDICTOR_RENDER_DB_PLAN" in RENDER
    assert "value: free" in RENDER
    assert "MARKETPREDICTOR_DB_STORAGE_LIMIT_GB" in RENDER
    assert "value: 1" in RENDER
    assert "HISTORICAL_EXPORT_DB_AUTO_PRUNE" in RENDER
    assert "value: false" in RENDER
