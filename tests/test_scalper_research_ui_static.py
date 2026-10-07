from pathlib import Path
import json

ROOT = Path(__file__).resolve().parents[1]


def test_scalper_research_manifest_is_packaged():
    p = ROOT / "research" / "scalper" / "research_manifest.json"
    data = json.loads(p.read_text(encoding="utf-8"))
    assert data["synthetic_lab_v2"]["regimes_count"] == 25
    assert data["synthetic_lab_v2"]["evidence_class"] == "SYNTHETIC_L5_CONTROLLED_LAB_V2"
    assert data["synthetic_lab_v2"]["fixed_target_dependency"] is False
    assert data["real_archive"]["sessions"] == 83
    assert data["real_archive"]["stocks"] == 226
    assert data["real_archive"]["resolution"] == "1m"


def test_scalper_priority_page_and_eye_menu():
    html = (ROOT / "templates" / "index.html").read_text(encoding="utf-8")
    assert "⚡ Scalper Engine" in html
    assert 'id="scalperRealTestsBody"' in html
    assert 'id="scLiveQualityStatus"' in html
    assert 'id="scLivePollLatency"' in html
    assert 'id="scLiveProviderLatency"' in html
    assert 'id="scLiveDiagnostics"' in html
    assert 'Run full synthetic market lab' not in html
    assert 'id="synL5TestTrades"' not in html
    assert 'api/scalper/synthetic/start' not in html
    assert "api/scalper/research" in html
    assert "AI Copilot" in html
    assert "Persistent state:" in html
    assert "Last status check" in html
    assert "Save &amp; Arm" in html
    assert '<circle cx="12" cy="12" r="2.8"></circle>' in html


def test_scalper_engine_imports():
    import sys
    sys.path.insert(0, str(ROOT))
    from research.scalper.ultra_scalper_engine import ScalperConfig, score_event
    assert ScalperConfig().target_pct == 0.60
    assert score_event.__name__ == "score_event"
