"""Regression tests for db_pool.ManagedPool (v3.4.31.5).

They run against a real PostgreSQL server because the bug is about *real* session
churn.  Set TEST_DATABASE_URL (e.g. postgresql://mp:mp@127.0.0.1/mp); the tests
are skipped when it is unset/unreachable.  Session churn is measured on the
server via pg_stat_database.sessions, i.e. the same thing the Render logs show as
"connection authorized" lines.
"""

import os
import threading
import time

import psycopg2
import pytest

from db_pool import ManagedPool, PoolExhausted, PooledConnection, thread_cell

DSN = os.environ.get('TEST_DATABASE_URL')


def _reachable() -> bool:
    if not DSN:
        return False
    try:
        psycopg2.connect(DSN, connect_timeout=2).close()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _reachable(), reason='TEST_DATABASE_URL not set/reachable')


def _sessions_total() -> int:
    c = psycopg2.connect(DSN)
    try:
        with c.cursor() as cur:
            cur.execute("SELECT sessions FROM pg_stat_database WHERE datname=current_database()")
            return int(cur.fetchone()[0])
    finally:
        c.close()


def _server_backends() -> int:
    c = psycopg2.connect(DSN)
    try:
        with c.cursor() as cur:
            cur.execute("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()")
            return int(cur.fetchone()[0])
    finally:
        c.close()


def borrow(pool, timeout=5.0):
    cell = thread_cell()
    raw, sem = pool.acquire(timeout, nested=cell[0] > 0)
    cell[0] += 1
    return PooledConnection(pool, raw, sem, cell)


def _query(conn):
    with conn, conn.cursor() as cur:
        cur.execute('SELECT 1')
        return cur.fetchone()[0]


def test_concurrent_bursts_reuse_connections_instead_of_churning():
    """The real-world failure: minconn=1 closed every extra connection after each
    burst and re-opened it on the next one."""
    pool = ManagedPool(6, 0, DSN)
    try:
        before = _sessions_total()

        def worker():
            for _ in range(30):
                c = borrow(pool)
                try:
                    _query(c)
                    time.sleep(0.005)
                finally:
                    c.close()

        for _ in range(5):  # five bursts of 6 concurrent workers
            ts = [threading.Thread(target=worker) for _ in range(6)]
            [t.start() for t in ts]
            [t.join() for t in ts]
            time.sleep(0.05)
        opened = _sessions_total() - before
        # Every worker in the first burst may open one; later bursts must reuse them.
        assert opened <= 6, f'{opened} new PostgreSQL sessions for 900 checkouts; expected <= pool size'
        assert pool.snapshot()['opened'] <= 7  # 1 eager + up to 6
    finally:
        pool.retire()


def test_stock_minconn1_pool_does_churn_control():
    """Control experiment proving the test above detects the original bug."""
    import psycopg2.pool as pp
    pool = pp.ThreadedConnectionPool(1, 6, DSN)
    try:
        before = _sessions_total()

        def worker():
            for _ in range(30):
                c = pool.getconn()
                try:
                    with c.cursor() as cur:
                        cur.execute('SELECT 1')
                    c.rollback()
                    time.sleep(0.005)
                finally:
                    pool.putconn(c)

        for _ in range(5):
            ts = [threading.Thread(target=worker) for _ in range(6)]
            [t.start() for t in ts]
            [t.join() for t in ts]
            time.sleep(0.05)
        assert _sessions_total() - before > 12, 'stock pool unexpectedly did not churn'
    finally:
        pool.closeall()


def test_exhaustion_waits_and_never_closes_in_use_connections():
    pool = ManagedPool(2, 0, DSN)
    try:
        a, b = borrow(pool), borrow(pool)
        raw_a, raw_b = a._conn, b._conn
        with pytest.raises(PoolExhausted):
            borrow(pool, timeout=0.3)
        # The in-flight connections are untouched (old code called closeall()).
        assert raw_a.closed == 0 and raw_b.closed == 0
        assert _query(a) == 1 and _query(b) == 1

        got = []
        t = threading.Thread(target=lambda: got.append(borrow(pool, timeout=3.0)))
        t.start()
        time.sleep(0.2)
        a.close()  # frees a slot; the waiter must be served from it
        t.join(2)
        assert got and _query(got[0]) == 1
        got[0].close(); b.close()
        assert pool.snapshot()['in_use'] == 0
    finally:
        pool.retire()


def test_nested_checkouts_cannot_deadlock_a_saturated_pool():
    """All primary slots held, each holder needs a second connection."""
    pool = ManagedPool(3, 3, DSN)
    try:
        results, errors = [], []
        barrier = threading.Barrier(3)

        def holder():
            outer = borrow(pool)           # primary slot
            try:
                barrier.wait(2)            # everyone now holds one
                inner = borrow(pool, timeout=2.0)  # nested slot, must not wait on primary
                try:
                    results.append(_query(inner))
                finally:
                    inner.close()
            except Exception as exc:       # pragma: no cover
                errors.append(exc)
            finally:
                outer.close()

        ts = [threading.Thread(target=holder) for _ in range(3)]
        [t.start() for t in ts]
        [t.join(5) for t in ts]
        assert not errors, errors
        assert results == [1, 1, 1]
    finally:
        pool.retire()


def test_dead_idle_connection_is_replaced_not_propagated():
    """Server-side kill of an idle pooled connection (restart/NAT timeout) must be
    healed on checkout, without raising and without touching other connections."""
    pool = ManagedPool(3, 0, DSN, validate_after=0.0)
    try:
        c = borrow(pool)
        pid = c.get_backend_pid()
        c.close()
        killer = psycopg2.connect(DSN)
        killer.autocommit = True
        with killer.cursor() as cur:
            cur.execute('SELECT pg_terminate_backend(%s)', (pid,))
        killer.close()
        time.sleep(0.2)
        c2 = borrow(pool)
        try:
            assert _query(c2) == 1
            assert c2.get_backend_pid() != pid
        finally:
            c2.close()
        assert pool.snapshot()['stale_discarded'] >= 1
    finally:
        pool.retire()


def test_with_block_scopes_transaction_only_and_loop_reuse_is_safe():
    """`with conn:` in a loop / inside helpers used to return the connection to the
    pool on the first exit, leaving later iterations on a shared connection."""
    pool = ManagedPool(2, 0, DSN)
    try:
        c = borrow(pool)
        for _ in range(3):
            assert _query(c) == 1          # same wrapper, three `with conn:` blocks
        assert pool.snapshot()['in_use'] == 1
        c.close()
        assert pool.snapshot()['in_use'] == 0
        with pytest.raises(psycopg2.InterfaceError):
            c.cursor()                     # use-after-return is loud, never silent
    finally:
        pool.retire()


def test_leaked_wrapper_is_returned_by_safety_net():
    pool = ManagedPool(2, 0, DSN)
    try:
        def leak():
            c = borrow(pool)
            _query(c)                      # no close()

        leak()
        import gc; gc.collect()
        assert pool.snapshot()['in_use'] == 0
        assert thread_cell()[0] == 0
    finally:
        pool.retire()


def test_failed_transaction_is_rolled_back_before_reuse():
    pool = ManagedPool(1, 0, DSN)
    try:
        c = borrow(pool)
        with pytest.raises(psycopg2.Error):
            with c, c.cursor() as cur:
                cur.execute('SELECT * FROM table_that_does_not_exist')
        c.close()
        c2 = borrow(pool)
        try:
            assert _query(c2) == 1
        finally:
            c2.close()
        assert pool.snapshot()['opened'] == 1  # the same connection was reused
    finally:
        pool.retire()


def test_idle_connections_are_recycled_after_ttl_only():
    pool = ManagedPool(3, 0, DSN, idle_ttl=0.3)
    try:
        cs = [borrow(pool) for _ in range(3)]
        [c.close() for c in cs]
        assert _server_backends() >= 3
        time.sleep(0.5)
        c = borrow(pool); c.close()        # any checkin triggers recycling
        assert pool.snapshot()['idle_recycled'] >= 2
    finally:
        pool.retire()
