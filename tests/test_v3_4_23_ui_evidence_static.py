from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / 'app.py').read_text(encoding='utf-8')
HTML = (ROOT / 'templates' / 'index.html').read_text(encoding='utf-8')
STRATEGIES = (ROOT / 'strategies.py').read_text(encoding='utf-8')


def test_history_endpoint_has_failure_boundary_and_nonfatal_exit_refresh():
    start = APP.index("@app.route('/api/trade-history', methods=['GET'])")
    end = APP.index("@app.route('/api/trade-history', methods=['DELETE'])", start)
    block = APP[start:end]
    assert "except Exception as e:" in block
    assert "Trade history could not be loaded" in block
    assert "exit_refresh_warning" in APP


def test_failure_patterns_are_test_phase_with_explicit_denominator_and_attribution():
    start = APP.index("@app.route('/api/intraday/failure-patterns', methods=['GET'])")
    end = APP.index("@app.route('/api/intraday/settings', methods=['POST'])", start)
    block = APP[start:end]
    assert "COALESCE(phase,'legacy') <> 'training'" in block
    assert "exit_time IS NOT NULL" in block
    assert "wins_analyzed" in block and "losses_analyzed" in block
    assert "classified_losses" in block and "unclassified_losses" in block
    assert "logical_runs_analyzed" in block
    assert "inferred_from_pattern_key" in block
    assert "No failure cause is invented" in block


def test_strategy_detail_explains_regime_fit_and_win_rate_math():
    assert 'Regime-fit prior score (0–100)' in HTML
    assert 'not measured win rates from this replay' in HTML
    assert 'win(s)' in HTML and 'loss(es)' in HTML
    assert 'wins ÷' in HTML


def test_recent_replay_history_is_visible_without_opening_subpage():
    assert 'id="irRecentHistoryCard"' in HTML
    assert 'id="irRecentRunsBody"' in HTML
    assert 'Open full Run History' in HTML


def test_navigation_is_display_first_and_tabs_are_not_form_submitters():
    assert "function _showAppView(id, visible)" in HTML
    assert 'function showPageLoaderError(viewId, err)' in HTML
    assert '<button type="button" class="tab-btn" id="tabHistory"' in HTML
    assert '<button type="button" class="tab-btn" id="tabExport"' in HTML


def test_failure_pattern_definition_is_explicit():
    assert 'def failure_pattern_definition()' in STRATEGIES
    assert "'unit': 'completed losing replay trade'" in STRATEGIES
    assert "'win_rate_definition': 'winning closed trades / analyzed closed trades in the attribution group'" in STRATEGIES
