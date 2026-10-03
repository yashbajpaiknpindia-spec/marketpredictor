import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / 'app.py'
SRC = APP.read_text(encoding='utf-8')


def _endpoint():
    tree = ast.parse(SRC)
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == 'intraday_replay_data_plan_endpoint':
            return node
    raise AssertionError('data-plan endpoint missing')


def test_export_inventory_variables_are_bound_before_use():
    node = _endpoint()
    assignment_line = None
    load_lines = {'export_verified': [], 'export_unavailable': []}
    for n in ast.walk(node):
        if isinstance(n, ast.Assign):
            names = set()
            for t in n.targets:
                for x in ast.walk(t):
                    if isinstance(x, ast.Name):
                        names.add(x.id)
            if {'export_verified', 'export_unavailable'} <= names:
                assignment_line = n.lineno
        if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load) and n.id in load_lines:
            load_lines[n.id].append(n.lineno)
    assert assignment_line is not None, 'export inventory result variables are never initialized'
    for name, lines in load_lines.items():
        assert lines, f'{name} is never read by the endpoint'
        assert min(lines) > assignment_line, f'{name} is used before assignment at lines {lines}'


def test_data_plan_has_fail_soft_export_inventory_handling():
    assert "export_inventory_error = None" in SRC
    assert "historical export inventory preflight failed" in SRC
    assert "'export_data_archives_covering_range':export_matches" in SRC
    assert "'export_data_archives_unavailable':export_unavailable_matches" in SRC
