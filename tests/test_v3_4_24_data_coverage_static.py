from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "app.py").read_text(encoding="utf-8")
HTML = (ROOT / "templates" / "index.html").read_text(encoding="utf-8")


def test_data_plan_is_network_free_and_has_date_level_coverage():
    start = APP.index("@app.route(\'/api/intraday-replay/data-plan\', methods=[\'POST\'])")
    end = APP.index("@app.route(\'/api/intraday-replay/<int:run_id>/status\'", start)
    block = APP[start:end]
    assert "daily_coverage" in block
    assert "_load_cached_cap_allowlist_row" in block
    assert "urllib.request" not in block
    assert "CREATE INDEX IF NOT EXISTS idx_intraday_replay_candle_coverage" in APP


def test_replay_reuses_cached_1m_for_5m_without_provider_fetch():
    start = APP.index("def _load_cached_replay_matrix")
    end = APP.index("def _store_cached_replay_days_bulk", start)
    block = APP[start:end]
    assert "interval='1m'" in block
    assert "resample('5min'" in block
    assert "if str(interval).lower() == '5m'" in block


def test_calendar_ui_exposes_resolution_and_reuse_states():
    assert 'id="irCoverageCalendar"' in HTML
    assert 'Replay cache · instant reuse' in HTML
    assert 'Export Data · local reuse' in HTML
    assert '1m→5m' in HTML
    assert 'Fast stored-data check' in HTML


def test_replay_ready_requires_warmup_coverage():
    assert "missing===0?'<div style=\"margin-top:5px;\"><span class=\"ok\"><strong>Replay-ready:" in HTML
