"""Resilient PostgreSQL connection pool for MarketPredictor (v3.4.31.5).

Why this exists
---------------
psycopg2's stock ``ThreadedConnectionPool`` has three behaviours that, combined
with this application's access pattern, produced a constant connect/disconnect
loop in the PostgreSQL logs:

1. **Idle retention is capped at ``minconn``.**  ``putconn()`` keeps a returned
   connection only while ``len(idle) < minconn``; every other returned
   connection is *closed*.  With ``minconn=1`` any two concurrent requests
   guarantee one real ``close()`` and, on the next burst, one brand-new
   TCP+TLS+auth handshake.  (``minconn=0`` was the worse version of the same bug.)

2. **Exhaustion raised ``PoolError`` and the caller then tore the pool down.**
   ``get_db_connection`` treated *any* ``getconn()`` failure as "stale pool" and
   called ``closeall()``, which also closes connections that other threads are
   actively using.  Those threads then fail, reconnect, and so on.

3. **Nested checkouts deadlock a small pool.**  ~28 functions hold one pooled
   connection while calling helpers that borrow another (up to 3 deep, e.g.
   dashboard -> evaluate_open_positions_for_exits -> get_open_positions).  With
   4 request threads and a pool of 5, "everyone holds one and waits for a
   second" is reachable, which is exactly what triggers (2).

What this pool does instead
---------------------------
* Retains **every** healthy idle connection (up to the pool size) and recycles
  one only after ``idle_ttl`` seconds of disuse.
* **Waits** (bounded) for a free slot instead of failing or tearing anything down.
* Gives threads that *already hold* a connection a separate **nested reserve**,
  so hold-and-wait can never starve them.
* Validates a connection that sat idle longer than ``validate_after`` seconds
  with ``SELECT 1`` before handing it out, discarding only that one connection
  if it died (server restart, idle NAT/proxy timeout).
* Never closes a connection that is in use.
* Exposes counters (``opened`` is the number of real PostgreSQL connections
  ever created) so churn is directly observable from ``/healthz``.
"""

from __future__ import annotations

import sys
import threading
import time
from typing import Any, Dict, Optional, Tuple

import psycopg2
import psycopg2.extensions
import psycopg2.pool


class PoolExhausted(psycopg2.pool.PoolError):
    """No connection slot became free within the allowed wait."""


# Per-thread count of connections currently held.  A mutable cell is used so a
# connection returned from a different thread still decrements its *owner's*
# counter.
_TL = threading.local()


def thread_cell() -> list:
    cell = getattr(_TL, 'cell', None)
    if cell is None:
        cell = _TL.cell = [0]
    return cell


class ManagedPool(psycopg2.pool.ThreadedConnectionPool):
    def __init__(
        self,
        primary_max: int,
        nested_reserve: int,
        *args: Any,
        idle_ttl: float = 300.0,
        validate_after: float = 5.0,
        **kwargs: Any,
    ) -> None:
        import os
        self.owner_pid = os.getpid()
        self.primary_max = max(1, int(primary_max))
        self.nested_reserve = max(0, int(nested_reserve))
        total = self.primary_max + self.nested_reserve
        self.idle_ttl = float(idle_ttl)
        self.validate_after = float(validate_after)
        self._slots = threading.BoundedSemaphore(self.primary_max)
        self._nested_slots = (
            threading.BoundedSemaphore(self.nested_reserve) if self.nested_reserve else None
        )
        self._idle_since: Dict[int, float] = {}
        self.retired = False
        self.stats: Dict[str, Any] = {
            'opened': 0,            # real PostgreSQL connections ever created
            'checkouts': 0,
            'wait_timeouts': 0,     # checkouts that gave up waiting for a slot
            'stale_discarded': 0,   # idle connections found dead and dropped
            'idle_recycled': 0,     # idle connections closed after idle_ttl
            'max_wait_ms': 0.0,
        }
        # minconn=1 -> one eager connection, so bad credentials/DSN fail fast.
        super().__init__(1, total, *args, **kwargs)
        # psycopg2 keeps a returned connection only while len(idle) < minconn.
        # Raising it to the pool size makes the pool retain every healthy one.
        self.minconn = total
        now = time.monotonic()
        for c in list(self._pool):
            self._idle_since[id(c)] = now

    # -- connection creation (counted) -------------------------------------
    def _connect(self, key=None):
        conn = super()._connect(key)
        self.stats['opened'] += 1
        return conn

    # -- checkout / checkin -------------------------------------------------
    def acquire(self, timeout: float, nested: bool = False) -> Tuple[Any, threading.BoundedSemaphore]:
        sem = self._nested_slots if (nested and self._nested_slots is not None) else self._slots
        started = time.monotonic()
        if not sem.acquire(timeout=max(0.0, float(timeout))):
            self.stats['wait_timeouts'] += 1
            raise PoolExhausted(
                f'no database connection became free within {timeout:.1f}s '
                f'({"nested" if nested else "primary"} slots exhausted; {self._brief()})'
            )
        waited_ms = (time.monotonic() - started) * 1000.0
        if waited_ms > self.stats['max_wait_ms']:
            self.stats['max_wait_ms'] = round(waited_ms, 1)
        try:
            conn = self._checkout_healthy()
        except BaseException:
            sem.release()
            raise
        self.stats['checkouts'] += 1
        return conn, sem

    def _checkout_healthy(self):
        for _ in range(self.maxconn + 1):
            conn = self.getconn()  # reuses an idle connection or opens a new one
            since = self._idle_since.pop(id(conn), None)
            if self._usable(conn, since):
                return conn
            self.stats['stale_discarded'] += 1
            self._discard(conn)
        raise psycopg2.OperationalError('could not obtain a healthy database connection')

    def _usable(self, conn, idle_since: Optional[float]) -> bool:
        if conn.closed:
            return False
        if idle_since is None:
            return True  # freshly opened: no need to ping it
        if time.monotonic() - idle_since < self.validate_after:
            return True
        try:
            with conn.cursor() as cur:
                cur.execute('SELECT 1')
                cur.fetchone()
            conn.rollback()
            return True
        except Exception:
            return False

    def _discard(self, conn) -> None:
        try:
            self.putconn(conn, close=True)
        except psycopg2.pool.PoolError:
            self._close_quietly(conn)

    def release(self, conn, sem, discard: bool = False) -> None:
        try:
            close = bool(discard or self.retired or conn.closed)
            try:
                self.putconn(conn, close=close)
            except psycopg2.pool.PoolError:
                self._close_quietly(conn)
            else:
                if not close:
                    self._idle_since[id(conn)] = time.monotonic()
        finally:
            sem.release()
        self._recycle_idle()

    # -- maintenance --------------------------------------------------------
    def _recycle_idle(self) -> None:
        """Close idle connections unused for longer than idle_ttl."""
        if self.idle_ttl <= 0:
            return
        now = time.monotonic()
        victims = []
        with self._lock:
            if self.closed:
                return
            keep = []
            for c in self._pool:
                t = self._idle_since.get(id(c), now)
                if c.closed or (now - t) > self.idle_ttl:
                    victims.append(c)
                else:
                    keep.append(c)
            if victims:
                self._pool[:] = keep
            live = {id(c) for c in self._pool}
            for k in [k for k in self._idle_since if k not in live]:
                del self._idle_since[k]
        for c in victims:
            self.stats['idle_recycled'] += 1
            self._close_quietly(c)

    def retire(self) -> None:
        """Stop reusing connections; close idle ones now, in-use ones on return."""
        self.retired = True
        with self._lock:
            idle = list(self._pool)
            self._pool[:] = []
            self._idle_since.clear()
        for c in idle:
            self._close_quietly(c)

    @staticmethod
    def _close_quietly(conn) -> None:
        try:
            conn.close()
        except Exception:
            pass

    def _brief(self) -> str:
        return f'in_use={len(self._used)} idle={len(self._pool)} max={self.maxconn}'

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                'in_use': len(self._used),
                'idle': len(self._pool),
                'max_connections': self.maxconn,
                'primary_max': self.primary_max,
                'nested_reserve': self.nested_reserve,
                **self.stats,
            }


class PooledConnection:
    """Drop-in wrapper: ``close()`` returns the connection to the pool.  ``with conn:``
    only scopes a transaction.  Once returned, the wrapper refuses further use so
    it can never touch a connection that another thread now owns."""

    def __init__(self, pool: ManagedPool, conn, sem, cell: list, on_return=None) -> None:
        object.__setattr__(self, '_pool', pool)
        object.__setattr__(self, '_conn', conn)
        object.__setattr__(self, '_sem', sem)
        object.__setattr__(self, '_cell', cell)
        object.__setattr__(self, '_on_return', on_return)
        object.__setattr__(self, '_returned', False)
        try:
            f = sys._getframe(2)
            origin = f'{f.f_code.co_filename.rsplit("/", 1)[-1]}:{f.f_lineno} in {f.f_code.co_name}'
        except Exception:
            origin = '?'
        object.__setattr__(self, '_origin', origin)

    def close(self) -> None:
        if self._returned:
            return
        object.__setattr__(self, '_returned', True)
        conn = self._conn
        broken = False
        try:
            try:
                if conn.closed == 0:
                    conn.rollback()  # never return an open/failed transaction
            except Exception:
                broken = True
            self._pool.release(conn, self._sem, discard=broken)
        finally:
            self._cell[0] = max(0, self._cell[0] - 1)
            cb = self._on_return
            if cb is not None:
                try:
                    cb()
                except Exception:
                    pass

    def __enter__(self):
        self._live()
        self._conn.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        # psycopg2 semantics: ``with conn:`` scopes a *transaction* (commit on
        # success, rollback on error) and does NOT close the connection.  The
        # previous wrapper also returned the connection to the pool here, so a
        # second ``with conn:`` in a loop, or a helper that received ``conn`` as
        # an argument, ran on a connection another thread might already own.
        # Return to the pool only via close().
        return self._conn.__exit__(exc_type, exc, tb)

    def _live(self) -> None:
        if self._returned:
            raise psycopg2.InterfaceError(
                f'database connection already returned to the pool (borrowed at {self._origin}); '
                'acquire a new one with get_db_connection()'
            )

    def __getattr__(self, name: str):
        # Only reached for attributes not found on the wrapper itself.
        self._live()
        return getattr(self._conn, name)

    def __del__(self) -> None:  # safety net for a missed close(): refcounting returns it
        try:
            if not self._returned:
                print(f'[DB_POOL] WARNING connection borrowed at {self._origin} was never closed; returning it now', flush=True)
                self.close()
        except Exception:
            pass
