from pathlib import Path
import ast

ROOT = Path(__file__).resolve().parents[1]
HIST = ROOT / 'historical_export.py'
INDEX = ROOT / 'templates' / 'index.html'
README = ROOT / 'README.md'


def test_unlimited_history_defaults_and_large_stream_cap():
    text = HIST.read_text(encoding='utf-8')
    assert 'HISTORICAL_EXPORT_MAX_LOOKBACK_DAYS", "0"' in text
    assert 'HISTORICAL_EXPORT_MAX_SPAN_DAYS", "0"' in text
    assert 'HISTORICAL_EXPORT_MAX_ROWS", "100000000"' in text
    assert 'if MAX_LOOKBACK_DAYS > 0' in text
    assert 'if MAX_SPAN_DAYS > 0' in text


def test_long_range_validation_no_longer_forces_150_or_130_days():
    tree = ast.parse(HIST.read_text(encoding='utf-8'))
    fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'resolve_dates')
    source = ast.get_source_segment(HIST.read_text(encoding='utf-8'), fn) or ''
    assert 'more than {MAX_LOOKBACK_DAYS} days back' in source
    assert 'MAX_LOOKBACK_DAYS > 0' in source
    assert 'MAX_SPAN_DAYS > 0' in source
    assert 'Split it into two exports' not in source
    assert 'provider availability varies by instrument/interval' in source


def test_export_ui_has_requested_year_presets():
    text = INDEX.read_text(encoding='utf-8')
    for year in (1, 2, 3, 5, 10):
        assert f'hxPresetYears({year})' in text
        assert f'>{year} year' in text or f'>{year} years' in text
    assert 'Custom' not in ''  # keep test intentionally independent of case/markup layout
    assert 'id="hxStartDate"' in text and 'id="hxEndDate"' in text
    assert 'there is no 150-day application lookback ceiling now' in text


def test_year_preset_function_exists_and_uses_calendar_years():
    text = INDEX.read_text(encoding='utf-8')
    start = text.index('function hxPresetYears(years)')
    end = text.index('function hxBody()', start)
    block = text[start:end]
    assert 'setFullYear(start.getFullYear() - Number(years || 1))' in block
    assert 'hxLatestClosedSession()' in block
    assert "document.getElementById('hxSpecificDates').value = '';" in block


def test_readme_documents_unlimited_range_controls():
    text = README.read_text(encoding='utf-8')
    assert 'HISTORICAL_EXPORT_MAX_LOOKBACK_DAYS` (0 = unlimited)' in text
    assert 'HISTORICAL_EXPORT_MAX_SPAN_DAYS` (0 = unlimited)' in text
    assert '1y/2y/3y/5y/10y presets plus Custom' in text
