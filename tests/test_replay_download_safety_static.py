"""Dependency-free regression checks for the v3.4.16 replay download path.

These checks intentionally inspect source text rather than importing the Flask app,
because the production dependency set may not be installed in lightweight audit
containers. They guard the important memory-safety invariants of the exporter.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "app.py").read_text(encoding="utf-8")


def test_zip_is_disk_backed():
    assert "tempfile.NamedTemporaryFile" in APP
    assert "send_file(tmp_path" in APP
    assert "io.BytesIO()" not in APP[APP.find("def intraday_replay_download_endpoint"):APP.find("def intraday_replay_download_endpoint") + 30000]


def test_candles_are_bounded():
    section_start = APP.find("def _stream_replay_candles_into_zip")
    section_end = APP.find("@app.route('/api/intraday-replay/<int:run_id>/download'", section_start)
    section = APP[section_start:section_end]
    assert "cursor(name=" in section
    assert "fetchmany(25)" in section
    assert "zf.open('market_candles.jsonl', 'w')" in section
    assert "cur.fetchall()" not in section


def test_builder_does_not_materialize_candles_by_default():
    assert "def build_intraday_replay_download(run_id: int, include_candle_cache: bool = False)" in APP
    endpoint_pos = APP.find("def intraday_replay_download_endpoint")
    assert APP.find("build_intraday_replay_download(run_id, include_candle_cache=False)", endpoint_pos) != -1


def test_json_export_is_safe():
    section_start = APP.find("def intraday_replay_download_endpoint")
    section_end = APP.find("@app.route('/api/backtest/compare-learning'", section_start)
    section = APP[section_start:section_end]
    assert "included_in_json': False" in section
    assert "result['candle_cache'] = []" in section


if __name__ == "__main__":
    tests = [test_zip_is_disk_backed, test_candles_are_bounded, test_builder_does_not_materialize_candles_by_default, test_json_export_is_safe]
    for test in tests:
        test()
        print(f"PASS: {test.__name__}")
