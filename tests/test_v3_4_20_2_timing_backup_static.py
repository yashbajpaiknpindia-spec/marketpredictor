from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / 'app.py').read_text(encoding='utf-8')
HTML = (ROOT / 'templates' / 'index.html').read_text(encoding='utf-8')
BACKUP = (ROOT / 'app_data_backup.py').read_text(encoding='utf-8')


def test_terminal_replay_timing_is_reconstructed_for_stopped_and_error_runs():
    assert "_terminal = str(row.get('status') or '').lower() in ('completed', 'stopped', 'error', 'cancelled')" in APP
    assert "'terminal_timing_label': _terminal_timing_label" in APP
    assert "elif _status_text == 'stopped':" in APP
    assert "elif _status_text == 'error':" in APP


def test_extended_parent_uses_scan_cycles_not_universe_multiplied_units():
    assert "estimated_units=max(1,len(trading_days)*bars_per_day)" in APP
    # The old universe multiplier must not feed ETA work units.
    assert "*estimate_universe*bars_per_day" not in APP


def test_full_data_backup_is_disk_backed_and_compressed():
    assert 'marketpredictor-full-data-backup' in BACKUP
    assert "mode=\"w:gz\"" in BACKUP
    assert 'COPY {} TO STDOUT' in BACKUP
    assert 'COPY {} ({}) FROM STDIN' in BACKUP
    assert 'REPEATABLE READ' in BACKUP


def test_ui_exposes_single_click_full_export_and_import():
    assert 'Export all application data' in HTML
    assert 'Import all application data' in HTML
    assert "'/api/app-data/export/start'" in HTML
    assert "'/api/app-data/import/start'" in HTML
    assert 'terminal_timing_label' in HTML
