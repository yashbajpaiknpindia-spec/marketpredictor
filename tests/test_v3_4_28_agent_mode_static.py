from pathlib import Path
import ast
import re

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / 'app.py'
HTML = ROOT / 'templates' / 'index.html'


def test_agent_schema_and_endpoints_exist():
    s = APP.read_text(encoding='utf-8')
    for token in (
        'ai_agent_proposals',
        "/api/ai-copilot/propose",
        "/api/ai-copilot/proposals",
        "/api/ai-copilot/proposals/<int:proposal_id>/approve",
        "/api/ai-copilot/proposals/<int:proposal_id>/rollback",
        'AGENT_MUTABLE_TRADING_SETTINGS',
        'OPENAI_AGENT_REQUIRE_CONFIRMATION',
    ):
        assert token in s


def test_agent_does_not_allow_live_or_source_code_changes():
    s = APP.read_text(encoding='utf-8')
    assert "'mode'" in s and "'capital_inr'" in s and "'enabled'" in s
    assert 'source-code mutation' in s
    assert 'order placement' in s


def test_agent_ui_has_read_propose_execute_modes_and_audit():
    s = HTML.read_text(encoding='utf-8')
    for token in ('agentModeRead', 'agentModePropose', 'agentModeExecute', 'agentProposalsList', 'approveAgentProposal', 'rollbackAgentProposal'):
        assert token in s


def test_app_parses():
    ast.parse(APP.read_text(encoding='utf-8'))
