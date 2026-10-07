from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
APP=(ROOT/"app.py").read_text(encoding="utf-8")
HTML=(ROOT/"templates/storage.html").read_text(encoding="utf-8")

def test_opaque_export_ids_never_cast_to_int_in_steward_accounting():
    assert "candidate_bytes[int(f['job_id'])]" not in APP
    assert "key=str(f['job_id'])" in APP

def test_storage_inventory_shows_all_records_and_exact_ids():
    assert "Show every Rapid Scan" in HTML
    assert "Show every Replay" in HTML
    assert "Show every Data Acquisition job" in HTML
    assert "Show every persisted Export archive" in HTML
    assert "Candidate ID:" in HTML
    assert "No records exist in this category." in HTML
    assert "slice(0,12)" not in HTML

def test_cleanup_preview_supports_all_records_without_qualifying_filter():
    assert "show_all=str(request.args.get('all')" in APP
    assert "mode':'ALL_RECORDS'" in APP
    assert "&all=1" in HTML

def test_exact_orphan_coverage_delete_uses_candidate_key():
    assert "targets.append(tuple(parts))" in APP
    assert "DELETE FROM market_data_coverage WHERE status<>'COMPLETE' AND ({where})" in APP
