"""Offline regression tests for MarketPredictor's DB pooling wrapper.

These tests do not require a live PostgreSQL server. They model psycopg2's
connection/pool lifecycle and specifically guard against the connection leak
that previously left Render PostgreSQL sessions open for minutes.
"""

import threading


class FakeConnection:
    def __init__(self, ident):
        self.ident = ident
        self.closed = 0
        self.entered = 0
        self.exited = 0
        self.rolled_back = 0

    def __enter__(self):
        self.entered += 1
        return self

    def __exit__(self, exc_type, exc, tb):
        self.exited += 1
        return False

    def rollback(self):
        self.rolled_back += 1


class FakePool:
    def __init__(self, minconn, maxconn):
        assert minconn == 1
        self.maxconn = maxconn
        self.idle = []
        self.used = {}
        self.created = 0

    def getconn(self):
        if self.idle:
            conn = self.idle.pop()
            self.used[conn.ident] = conn
            return conn
        if len(self.used) >= self.maxconn:
            raise RuntimeError('pool exhausted')
        self.created += 1
        conn = FakeConnection(self.created)
        self.used[conn.ident] = conn
        return conn

    def putconn(self, conn):
        self.used.pop(conn.ident, None)
        self.idle.append(conn)


_DB_POOL_BORROWED = 0
_DB_POOL_LOCK = threading.RLock()


class PooledConnectionUnderTest:
    """Behavioral copy of the application's wrapper contract."""

    def __init__(self, pool, conn):
        self._pool = pool
        self._conn = conn
        self._returned = False

    def close(self):
        global _DB_POOL_BORROWED
        if self._returned:
            return
        self._returned = True
        try:
            if getattr(self._conn, 'closed', 1) == 0:
                self._conn.rollback()
            self._pool.putconn(self._conn)
        finally:
            with _DB_POOL_LOCK:
                _DB_POOL_BORROWED = max(0, _DB_POOL_BORROWED - 1)

    def __enter__(self):
        self._conn.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            return self._conn.__exit__(exc_type, exc, tb)
        finally:
            self.close()

    def __getattr__(self, name):
        return getattr(self._conn, name)


def borrow(pool):
    global _DB_POOL_BORROWED
    conn = pool.getconn()
    _DB_POOL_BORROWED += 1
    return PooledConnectionUnderTest(pool, conn)


def test_repeated_context_manager_reuses_connection():
    global _DB_POOL_BORROWED
    _DB_POOL_BORROWED = 0
    pool = FakePool(1, 5)
    for _ in range(100):
        with borrow(pool) as conn:
            assert conn.ident == 1
    assert pool.created == 1
    assert _DB_POOL_BORROWED == 0
    assert len(pool.idle) == 1


def test_exception_still_returns_connection():
    global _DB_POOL_BORROWED
    _DB_POOL_BORROWED = 0
    pool = FakePool(1, 5)
    try:
        with borrow(pool):
            raise ValueError('forced')
    except ValueError:
        pass
    assert pool.created == 1
    assert _DB_POOL_BORROWED == 0
    assert len(pool.idle) == 1


def test_application_pool_retains_idle_connections():
    """v3.4.31.5: the stock pool (even with minconn=1) closes every returned
    connection beyond minconn; the application must use ManagedPool, which retains
    all healthy idle connections. Behavioural coverage: test_db_pool_managed.py."""
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    app_text = (root / "app.py").read_text(encoding="utf-8")
    pool_text = (root / "db_pool.py").read_text(encoding="utf-8")
    assert "_ManagedPool(" in app_text
    assert "psycopg2.pool.ThreadedConnectionPool(" not in app_text
    assert "closeall()" not in app_text  # never tear down connections that are in use
    assert "self.minconn = total" in pool_text
