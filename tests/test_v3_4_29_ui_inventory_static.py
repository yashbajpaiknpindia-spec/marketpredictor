from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
HTML = ROOT / "templates" / "index.html"
text = HTML.read_text(encoding="utf-8")


def test_ui_refresh_layer_present():
    assert 'id="marketpredictor-ui-refresh"' in text
    assert '.secondary-btn,.scan-btn,.first-scan-btn' in text
    assert '.tabs{position:sticky' in text


def test_replay_uses_persistent_inventory_endpoint():
    assert "async function loadReplayStoredDataInventory(forceRefresh=false)" in text
    assert "/api/data-inventory/coverage?market=" in text
    assert "Promise.resolve(loadReplayStoredDataInventory())" in text


def test_export_and_replay_share_calendar_renderer():
    assert "function _inventoryCalendarHtml(data, options={})" in text
    assert "_inventoryCalendarHtml(data,{context:'export'" in text
    assert "_inventoryCalendarHtml(data,{context:'replay'" in text


def test_replay_calendar_has_month_navigation():
    for token in ("irInventoryMonthSelect", "irInventoryJumpToSelected", "irInventoryScrollEnd(-1)", "irInventoryScrollEnd(1)"):
        assert token in text


def test_no_duplicate_ids_for_new_replay_inventory_controls():
    ids = re.findall(r'\bid=["\']([^"\']+)["\']', text)
    for wanted in {"irInventoryToolbar", "irInventoryMonthSelect", "irInventorySummaryMini"}:
        assert ids.count(wanted) == 1, wanted
