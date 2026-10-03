from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / 'app.py'
HTML = ROOT / 'templates' / 'index.html'


def test_research_worker_isolated_and_timed():
    src = APP.read_text(encoding='utf-8')
    assert "multiprocessing.get_context('spawn')" in src
    assert 'isolated_research_worker = bool(settings.get(\'_research_sweep\')) or tournament_mode' in src
    assert 'profit_factor_checkpoint' in src
    assert 'estimated_remaining_seconds' in src
    assert 'estimated_finish_at' in src


def test_research_loss_contract_and_strategy_immutability_contract():
    src = APP.read_text(encoding='utf-8')
    assert "post_entry = {'enabled': False, 'action': 'HOLD', 'reason': 'research_sweep_original_stop_only'}" in src
    assert "and not research_trail_only" in src
    assert "strategy_definitions_changed': False" in src
    assert "research_profit_trail_policy" in src


def test_long_runs_and_streaming_exports_are_batched():
    src = APP.read_text(encoding='utf-8')
    assert 'INTRADAY_REPLAY_EXTENDED_MAX_TRADING_DAYS' in src
    assert "len(_probe_days) > INTRADAY_REPLAY_MAX_SELECTED_DAYS" in src
    assert 'def _build_streaming_replay_export' in src
    assert 'def _build_streaming_replay_json_export' in src
    assert 'fetchmany(2000)' in src
    assert 'fetchmany(25)' in src
    assert 'streamed_rows' in src


def test_ui_surfaces_timing_and_research_exit_contract():
    html = HTML.read_text(encoding='utf-8')
    assert 'irTimingPanel' in html
    assert 'elapsed_time_label' in html
    assert 'estimated_time_label' in html
    assert 'estimated_finish_at' in html
    assert 'Research exit contract:' in html
