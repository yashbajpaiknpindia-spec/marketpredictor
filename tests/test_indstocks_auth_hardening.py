"""Offline tests for the INDstocks token / request-storm hardening (no network, no DB).

A local HTTP server imitates api.indstocks.com as documented (api-docs.indstocks.com):
  * POST /generate/token  - one live token at a time, 1 mint / 60 s, or a Cloudflare "Just a moment..." 429
  * GET  /market/quotes/full|mkt|ltp, /market/instruments - need the CURRENT token, else 403 TokenException
"""
import importlib
import json
import threading
import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

import pytest

CF_HTML = "<!DOCTYPE html><html><head><title>Just a moment...</title></head><body>Checking your browser</body></html>"
EQ_CSV = "EXCH,SEGMENT,SECURITY_ID,TRADING_SYMBOL,SYMBOL_NAME\nNSE,E,2885,RELIANCE,RELIANCE\nNSE,E,11536,TCS,TCS\n"
IX_CSV = "EXCH,SEGMENT,SECURITY_ID\nNSE,NIFTY 50,13\n"


class Provider:
    def __init__(self):
        self.mode = "ok"            # ok | cloudflare | data_cloudflare | instruments_429
        self.live_token = None
        self.mints = 0
        self.last_mint = 0.0
        self.token_calls = 0
        self.calls = {"full": 0, "mkt": 0, "ltp": 0, "instruments": 0}
        self.full_has_depth = False
        self.rejects = 0


def make_handler(p: Provider):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _send(self, code, body, ctype="application/json"):
            raw = body.encode() if isinstance(body, str) else body
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_POST(self):
            n = int(self.headers.get("Content-Length") or 0)
            self.rfile.read(n)
            if self.path != "/generate/token":
                return self._send(404, "{}")
            p.token_calls += 1
            if p.mode == "cloudflare":
                return self._send(429, CF_HTML, "text/html")
            if time.time() - p.last_mint < 60 and p.last_mint:
                return self._send(429, json.dumps({"status": "error", "message": "1 token per 60 seconds"}))
            p.mints += 1
            p.last_mint = time.time()
            p.live_token = f"TOKEN{p.mints}"
            return self._send(200, json.dumps({"status": "success", "data": {"token": p.live_token}}))

        def do_GET(self):
            u = urlparse(self.path)
            q = parse_qs(u.query)
            if p.mode == "data_cloudflare":
                return self._send(403, CF_HTML, "text/html")
            if self.headers.get("Authorization") != p.live_token:
                p.rejects += 1
                return self._send(403, json.dumps({"status": "error", "message": "TokenException: invalid token", "error_code": "TOKEN_EXCEPTION"}))
            codes = (q.get("scrip-codes") or [""])[0].split(",")
            depth = {"depth": [{"buy": {"quantity": "2,318", "price": f"{100 - i * .05:.2f}"}, "sell": {"quantity": "1,792", "price": f"{100.05 + i * .05:.2f}"}} for i in range(5)]}
            if u.path == "/market/quotes/full":
                p.calls["full"] += 1
                d = {c: {"live_price": 100.0, "volume": 1000, **({"market_depth": depth} if p.full_has_depth else {})} for c in codes}
                return self._send(200, json.dumps({"status": "success", "data": d}))
            if u.path == "/market/quotes/mkt":
                p.calls["mkt"] += 1
                return self._send(200, json.dumps({"status": "success", "data": {c: {"market_depth": depth} for c in codes}}))
            if u.path == "/market/quotes/ltp":
                p.calls["ltp"] += 1
                return self._send(200, json.dumps({"status": "success", "data": {c: {"live_price": 100.0} for c in codes}}))
            if u.path == "/market/instruments":
                p.calls["instruments"] += 1
                if p.mode == "instruments_429":
                    return self._send(429, CF_HTML, "text/html")
                return self._send(200, IX_CSV if (q.get("source") or [""])[0] == "index" else EQ_CSV, "text/csv")
            return self._send(404, "{}")
    return H


class FakeSharedStore:
    """Stands in for the PostgreSQL store: shared dict + a real mutex."""
    def __init__(self):
        self.row = None
        self._mutex = threading.Lock()
        self.saves = 0

    def load(self):
        return dict(self.row) if self.row else None

    def save(self, st):
        self.row = dict(st)
        self.saves += 1

    @contextmanager
    def lock(self):
        got = self._mutex.acquire(blocking=False)
        try:
            yield got
        finally:
            if got:
                self._mutex.release()


@pytest.fixture()
def env(monkeypatch):
    p = Provider()
    srv = HTTPServer(("127.0.0.1", 0), make_handler(p))
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    for k in ("INDSTOCKS_ACCESS_TOKEN", "INDSTOCKS_TOKEN"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("INDSTOCKS_API_KEY", "client")
    monkeypatch.setenv("INDSTOCKS_MPIN", "1234")
    monkeypatch.setenv("INDSTOCKS_TOTP_SECRET", "JBSWY3DPEHPK3PXP")
    monkeypatch.setenv("INDSTOCKS_MIN_REQUEST_GAP", "0")
    import indstocks_client as c
    c = importlib.reload(c)
    monkeypatch.setattr(c, "BASE_URL", f"http://127.0.0.1:{srv.server_address[1]}")
    c._static_rejected.clear()
    yield c, p
    srv.shutdown()


def restart(c):
    """Simulate a process restart: all in-memory state is lost."""
    c._token_cache.update({"token": None, "expires_at": 0.0, "source": None})
    c._token_state.update({"fail_count": 0, "cooldown_until": 0.0, "last_error": None, "last_error_at": None,
                           "last_attempt_at": 0.0, "last_ok_at": None, "last_mint_at": 0.0})
    c._instrument_cache.update({"equity": None, "loaded_at": 0.0})
    c._instrument_fail.update({"at": 0.0, "msg": None})


def test_cloudflare_block_is_not_retried_every_call(env):
    c, p = env
    p.mode = "cloudflare"
    for _ in range(30):
        with pytest.raises(c.INDstocksError):
            c.get_ltp(["NSE_1"])
    assert p.token_calls == 1, "a Cloudflare-blocked token endpoint must be hit once, then left alone during cooldown"
    assert "Cloudflare" in c.token_status()["last_error"]


def test_cooldown_and_token_survive_restart_via_shared_store(env):
    c, p = env
    store = FakeSharedStore()
    c.configure_token_store(store)
    p.mode = "cloudflare"
    with pytest.raises(c.INDstocksError):
        c.get_ltp(["NSE_1"])
    assert p.token_calls == 1
    restart(c)  # deploy / restart
    with pytest.raises(c.INDstocksError):
        c.get_ltp(["NSE_1"])
    assert p.token_calls == 1, "cooldown must persist across a restart"
    # provider recovers; cooldown expires -> exactly one mint
    p.mode = "ok"
    store.row["cooldown_until"] = 0
    store.row["last_attempt_at"] = 0
    assert c.get_ltp(["NSE_1"])["NSE_1"]["live_price"] == 100.0
    assert p.mints == 1
    restart(c)  # another restart: reuse the shared token, no new mint
    assert c.get_ltp(["NSE_1"])["NSE_1"]["live_price"] == 100.0
    assert p.mints == 1 and p.token_calls == 2


def test_other_instance_rotated_token_is_adopted_not_reminted(env):
    c, p = env
    store = FakeSharedStore()
    c.configure_token_store(store)
    c.get_ltp(["NSE_1"])
    assert p.mints == 1
    # "another instance" mints a new token -> ours is now revoked
    p.live_token = "TOKEN_OTHER"
    store.row["token"] = "TOKEN_OTHER"
    assert c.get_ltp(["NSE_1"])["NSE_1"]["live_price"] == 100.0
    assert p.mints == 1, "adopting the shared token must not mint (minting kills the other instance's token)"


def test_revoked_token_remint_is_limited_to_one_per_minute(env):
    c, p = env
    c.get_ltp(["NSE_1"])
    p.live_token = "REVOKED_ON_DASHBOARD"
    for _ in range(10):
        try:
            c.get_ltp(["NSE_1"])
        except c.INDstocksError:
            pass
    assert p.mints <= 1 + 0, "re-minting within 65 s of the last mint is forbidden"


def test_cloudflare_403_on_data_endpoint_never_mints(env):
    c, p = env
    c.get_ltp(["NSE_1"])
    mints = p.mints
    p.mode = "data_cloudflare"
    for _ in range(5):
        with pytest.raises(c.INDstocksError):
            c.get_ltp(["NSE_1"])
    assert p.mints == mints and p.token_calls == mints


def test_static_dashboard_token_never_calls_generate_token(env, monkeypatch):
    c, p = env
    p.live_token = "DASH"
    monkeypatch.setenv("INDSTOCKS_ACCESS_TOKEN", "DASH")
    monkeypatch.setenv("INDSTOCKS_API_KEY", "")
    monkeypatch.setenv("INDSTOCKS_MPIN", "")
    monkeypatch.setenv("INDSTOCKS_TOTP_SECRET", "")
    base = c.BASE_URL
    c = importlib.reload(c)
    monkeypatch.setattr(c, "BASE_URL", base)
    assert c.credentials_configured()
    assert c.get_ltp(["NSE_1"])["NSE_1"]["live_price"] == 100.0
    assert p.token_calls == 0
    p.live_token = "EXPIRED"
    with pytest.raises(c.INDstocksError) as ei:
        c.get_ltp(["NSE_1"])
    assert "INDSTOCKS_ACCESS_TOKEN" in str(ei.value) and p.token_calls == 0


def test_user_initiated_bypass_is_at_most_once_per_65s(env, monkeypatch):
    c, p = env
    p.mode = "cloudflare"
    with pytest.raises(c.INDstocksError):
        c.get_access_token()
    for _ in range(5):  # the "Test INDstocks now" button mashed repeatedly
        with pytest.raises(c.INDstocksError):
            c.get_access_token(user_initiated=True)
    assert p.token_calls == 1
    c._token_state["last_attempt_at"] -= 70
    with pytest.raises(c.INDstocksError):
        c.get_access_token(user_initiated=True)
    assert p.token_calls == 2


def test_instrument_master_failure_is_not_retried_per_symbol(env):
    c, p = env
    p.mode = "instruments_429"
    p.live_token = "X"
    c.configure_token_store(None)
    c._token_cache.update({"token": "X", "expires_at": time.time() + 3600})
    for i in range(250):
        with pytest.raises(c.INDstocksError):
            c.resolve_equity_scrip_code_strict(f"SYM{i}")
    assert p.calls["instruments"] <= 4 * (1 + c.MAX_RATE_LIMIT_RETRIES) + 1 or p.calls["instruments"] <= 2, p.calls
    assert p.calls["instruments"] <= 2, f"instrument master was downloaded {p.calls['instruments']}x for 250 symbols"


def test_two_racing_minters_only_one_mints(env):
    c, p = env
    store = FakeSharedStore()
    c.configure_token_store(store)
    out = []

    def worker():
        try:
            out.append(c.get_access_token())
        except Exception as e:  # noqa
            out.append(e)
    ts = [threading.Thread(target=worker) for _ in range(4)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert p.mints == 1
    assert all(o == "TOKEN1" for o in out), out


def test_documented_depth_shapes_parse_with_comma_quantities():
    from research.scalper.scalper_live_paper import _parse_depth
    depth = {"depth": [{"buy": {"quantity": "2,318", "price": "788.60"}, "sell": {"quantity": "1,792", "price": "789.15"}}] * 5}
    d = _parse_depth({"live_price": 788.8, "market_depth": depth})
    assert d["valid_levels"] == 5 and d["bid1_qty"] == 2318.0 and d["ask1"] == 789.15
