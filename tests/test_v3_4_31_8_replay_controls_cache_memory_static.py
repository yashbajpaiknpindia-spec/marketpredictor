from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / 'app.py').read_text(encoding='utf-8')
TPL = (ROOT / 'templates' / 'index.html').read_text(encoding='utf-8')
STRAT = (ROOT / 'strategies.py').read_text(encoding='utf-8')


def test_strategy_toggle_and_run_snapshot_contract():
    assert 'id="irStrategyControls"' in TPL
    assert 'function irSelectedStrategyIds()' in TPL
    assert 'enabled_strategy_ids: irSelectedStrategyIds()' in TPL or '...irReplayRiskSettings()' in TPL
    assert "'enabled_strategy_ids': list(replay_enabled_ids)" in APP
    assert "'enabled_strategy_count': len(replay_enabled_ids)" in APP
    assert "_replay_enabled_strategy_ids" in STRAT


def test_replay_protection_is_visible_and_persisted():
    assert 'id="irLossProtectionPct"' in TPL
    assert 'id="irProfitProtectionPct"' in TPL
    assert 'Loss protection:' in TPL
    assert 'Profit protection activates:' in TPL
    assert "'replay_loss_protection_pct': loss_protection_pct" in APP
    assert "'replay_profit_protection_activation_pct': profit_activation_pct" in APP


def test_recent_history_delete_restored():
    assert 'deleteIntradayReplayRun(${r.run_id})' in TPL
    assert "@app.route('/api/intraday-replay/<int:run_id>', methods=['DELETE'])" in APP


def test_historical_archive_restore_failure_blocks_duplicate_provider_download():
    assert 'historical_export_restore_failures' in APP
    assert 'Provider download was blocked to prevent duplicate historical downloads' in APP
    assert 'verified Export Data bytes' in TPL


def test_long_replay_memory_is_bounded_at_runtime_and_container_level():
    assert 'def _current_container_memory_mb()' in APP
    assert "metric = 'container RSS'" in APP
    assert '_enforce_replay_runtime_memory(run_id, f\'tick_{ticks_done}\')' in APP
    assert "if minutes == 1:\n        return 1" in APP
    assert 'runtime_memory_hard_limit_mb' in APP
