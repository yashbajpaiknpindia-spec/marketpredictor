"""R2-backed cold archive for MarketPredictor large PostgreSQL datasets.

Design:
- PostgreSQL remains the hot/control plane.
- Cloudflare R2 is the durable data plane for large raw snapshots/candle cache.
- Archive is opt-in by credentials but safe-by-default: a source row is never
  deleted until the corresponding R2 object is uploaded, HEAD-verified, and
  recorded as VERIFIED in PostgreSQL.
- Large objects are gzip JSONL and bounded into deterministic parts.
- Replay candle cache supports read-through from R2 after hot-cache cleanup.
"""

from __future__ import annotations

import base64
import datetime as _dt
import gzip
import hashlib
import io
import json
import os
import threading
import time
import tempfile
import uuid
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

try:
    import boto3
except Exception:  # pragma: no cover - dependency is present in production builds.
    boto3 = None


ARCHIVE_SCHEMA_VERSION = "marketpredictor-r2-v1"
SNAPSHOT_TABLE = "scalper_live_snapshots"
CANDLE_TABLE = "intraday_replay_candle_cache"
MANIFEST_TABLE = "data_archive_objects"
DEFAULT_THRESHOLD_MB = 450
DEFAULT_INTERVAL_HOURS = 4
DEFAULT_PREFIX = "marketpredictor"
SNAPSHOT_PART_ROWS = 25_000


def _env(*names: str, default: str = "") -> str:
    for name in names:
        value = os.environ.get(name)
        if value not in (None, ""):
            return str(value)
    return default


def _env_bool(name: str, default: bool = False) -> bool:
    value = _env(name, default="1" if default else "0").strip().lower()
    return value in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    try:
        return float(_env(name, default=str(default)))
    except Exception:
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(_env(name, default=str(default))))
    except Exception:
        return default


def _utc_now() -> _dt.datetime:
    return _dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None)


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (_dt.datetime, _dt.date, _dt.time)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {"__base64__": base64.b64encode(bytes(value)).decode("ascii")}
    if isinstance(value, dict):
        return {str(k): _json_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(v) for v in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _object_key(prefix: str, dataset: str, partition: str, part: int, suffix: str = "jsonl.gz") -> str:
    safe_partition = partition.replace(" ", "_").replace("/", "_")
    return f"{prefix}/v1/{dataset}/{safe_partition}/part-{part:04d}.{suffix}"


class R2ArchiveStore:
    def __init__(self, db_connect, logger=None):
        self.db_connect = db_connect
        self.logger = logger
        self._client = None
        self._last_verified_at = None
        self._last_run_at = None
        self._last_error = None
        self._last_result: Dict[str, Any] = {}
        self._lock = threading.RLock()

    @property
    def enabled(self) -> bool:
        explicit = _env("SCALPER_ARCHIVE_ENABLED", default="")
        configured = all([
            _env("SCALPER_ARCHIVE_ENDPOINT_URL", "R2_ENDPOINT_URL"),
            _env("SCALPER_ARCHIVE_BUCKET", "R2_BUCKET"),
            _env("SCALPER_ARCHIVE_ACCESS_KEY_ID", "R2_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID"),
            _env("SCALPER_ARCHIVE_SECRET_ACCESS_KEY", "R2_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY"),
        ])
        return configured and (True if explicit == "" else _env_bool("SCALPER_ARCHIVE_ENABLED"))

    @property
    def threshold_bytes(self) -> int:
        return _env_int("SCALPER_ARCHIVE_THRESHOLD_MB", DEFAULT_THRESHOLD_MB) * 1024 * 1024

    @property
    def interval_seconds(self) -> int:
        return max(3600, _env_int("SCALPER_ARCHIVE_INTERVAL_HOURS", DEFAULT_INTERVAL_HOURS) * 3600)

    @property
    def bucket(self) -> str:
        return _env("SCALPER_ARCHIVE_BUCKET", "R2_BUCKET")

    @property
    def prefix(self) -> str:
        return _env("SCALPER_ARCHIVE_PREFIX", default=DEFAULT_PREFIX).strip("/")

    def _client_or_raise(self):
        if boto3 is None:
            raise RuntimeError("boto3 is not installed")
        endpoint = _env("SCALPER_ARCHIVE_ENDPOINT_URL", "R2_ENDPOINT_URL")
        access = _env("SCALPER_ARCHIVE_ACCESS_KEY_ID", "R2_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID")
        secret = _env("SCALPER_ARCHIVE_SECRET_ACCESS_KEY", "R2_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY")
        region = _env("SCALPER_ARCHIVE_REGION", "R2_REGION", default="auto")
        if not endpoint or not self.bucket or not access or not secret:
            raise RuntimeError("R2 archive credentials/configuration are incomplete")
        with self._lock:
            if self._client is None:
                self._client = boto3.client(
                    "s3",
                    endpoint_url=endpoint,
                    aws_access_key_id=access,
                    aws_secret_access_key=secret,
                    region_name=region,
                )
        return self._client

    def verify_connection(self, force: bool = False) -> Dict[str, Any]:
        endpoint = _env("SCALPER_ARCHIVE_ENDPOINT_URL", "R2_ENDPOINT_URL")
        if not self.enabled:
            return {
                "enabled": False,
                "configured": False,
                "connected": False,
                "bucket": self.bucket or None,
                "endpoint_host": endpoint.split("//", 1)[-1].split("/", 1)[0] if endpoint else None,
                "error": "R2 archive configuration is incomplete or disabled",
            }
        now = time.monotonic()
        if not force and self._last_verified_at and now - self._last_verified_at < 300:
            return {"enabled": True, "configured": True, "connected": True, "bucket": self.bucket, "error": None}
        try:
            client = self._client_or_raise()
            client.head_bucket(Bucket=self.bucket)
            self._last_verified_at = now
            self._last_error = None
            self._log("[R2_ARCHIVE] connectivity verified bucket=%s threshold_mb=%s interval_hours=%s",
                      self.bucket, self.threshold_bytes // (1024 * 1024), self.interval_seconds // 3600)
            return {
                "enabled": True, "configured": True, "connected": True,
                "bucket": self.bucket, "prefix": self.prefix, "threshold_mb": self.threshold_bytes // (1024 * 1024),
                "interval_hours": self.interval_seconds // 3600, "error": None,
            }
        except Exception as exc:
            self._last_error = str(exc)[:400]
            self._log("[R2_ARCHIVE] connectivity verification failed: %s", self._last_error, error=True)
            return {"enabled": True, "configured": True, "connected": False, "bucket": self.bucket, "error": self._last_error}

    def _log(self, message: str, *args, error: bool = False):
        try:
            if self.logger is not None:
                (self.logger.error if error else self.logger.info)(message, *args)
        except Exception:
            pass

    def ensure_manifest(self, conn) -> None:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {MANIFEST_TABLE} (
                    id BIGSERIAL PRIMARY KEY,
                    archive_id VARCHAR(80) NOT NULL,
                    dataset_type VARCHAR(80) NOT NULL,
                    source_table VARCHAR(100) NOT NULL,
                    source_partition VARCHAR(200) NOT NULL,
                    part_index INT NOT NULL DEFAULT 1,
                    object_key TEXT NOT NULL,
                    row_count BIGINT NOT NULL DEFAULT 0,
                    byte_size BIGINT NOT NULL DEFAULT 0,
                    sha256 CHAR(64) NOT NULL,
                    status VARCHAR(20) NOT NULL,
                    metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    source_first_id BIGINT,
                    source_last_id BIGINT,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    verified_at TIMESTAMP,
                    source_deleted_at TIMESTAMP,
                    error TEXT,
                    UNIQUE (dataset_type, source_partition, part_index)
                )
            """)
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{MANIFEST_TABLE}_lookup ON {MANIFEST_TABLE}(dataset_type, source_partition, status)")
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{MANIFEST_TABLE}_created
                ON {MANIFEST_TABLE}(created_at DESC)
            """)
        conn.commit()

    def _archivable_db_bytes(self, conn) -> Dict[str, int]:
        names = [SNAPSHOT_TABLE, CANDLE_TABLE]
        sizes: Dict[str, int] = {}
        with conn.cursor() as cur:
            for table in names:
                try:
                    cur.execute("SELECT COALESCE(pg_total_relation_size(%s::regclass),0)", (table,))
                    sizes[table] = int(cur.fetchone()[0] or 0)
                except Exception:
                    sizes[table] = 0
        return sizes

    def _active_candle_writer(self, conn) -> bool:
        checks = [
            ("intraday_replay_runs", "status IN ('running','queued','pending','resuming')"),
            ("rapid_forensics_runs", "status IN ('running','queued','pending','resuming')"),
            ("market_data_jobs", "status IN ('running','queued','pending','acquiring')"),
            ("historical_export_jobs", "status IN ('running','queued','pending')"),
        ]
        with conn.cursor() as cur:
            for table, predicate in checks:
                try:
                    cur.execute(f"SELECT EXISTS(SELECT 1 FROM {table} WHERE {predicate})")
                    if cur.fetchone()[0]:
                        return True
                except Exception:
                    # Missing auxiliary tables must not disable the archive.
                    continue
        return False

    def _verified_parts(self, conn, dataset_type: str, partition: str) -> Set[int]:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT part_index FROM {MANIFEST_TABLE} WHERE dataset_type=%s AND source_partition=%s AND status='VERIFIED'",
                (dataset_type, partition),
            )
            return {int(row[0]) for row in cur.fetchall()}

    def _upload_file_verified(self, file_path: str, key: str, metadata: Dict[str, str], sha256: str, byte_size: int) -> None:
        client = self._client_or_raise()
        meta = {str(k).lower().replace("_", "-"): str(v) for k, v in metadata.items()}
        meta["archive-sha256"] = sha256
        meta["archive-schema"] = ARCHIVE_SCHEMA_VERSION
        client.upload_file(
            file_path,
            self.bucket,
            key,
            ExtraArgs={
                "ContentType": "application/x-ndjson",
                "ContentEncoding": "gzip",
                "Metadata": meta,
            },
        )
        head = client.head_object(Bucket=self.bucket, Key=key)
        actual_size = int(head.get("ContentLength") or 0)
        actual_sha = str((head.get("Metadata") or {}).get("archive-sha256") or "")
        if actual_size != int(byte_size) or actual_sha != sha256:
            raise RuntimeError(
                f"R2 verification mismatch key={key} expected_size={byte_size} actual_size={actual_size} "
                f"expected_sha={sha256} actual_sha={actual_sha or 'missing'}"
            )

    def _write_manifest(self, conn, archive_id: str, dataset_type: str, source_table: str,
                        partition: str, part: int, key: str, row_count: int, byte_size: int,
                        sha256: str, metadata: Dict[str, Any], first_id: Optional[int],
                        last_id: Optional[int], status: str, error: Optional[str] = None) -> None:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {MANIFEST_TABLE}
                    (archive_id,dataset_type,source_table,source_partition,part_index,object_key,row_count,byte_size,sha256,status,metadata,source_first_id,source_last_id,error)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (dataset_type,source_partition,part_index)
                DO UPDATE SET
                    archive_id=EXCLUDED.archive_id, object_key=EXCLUDED.object_key, row_count=EXCLUDED.row_count,
                    byte_size=EXCLUDED.byte_size, sha256=EXCLUDED.sha256, status=EXCLUDED.status,
                    metadata=EXCLUDED.metadata, source_first_id=EXCLUDED.source_first_id,
                    source_last_id=EXCLUDED.source_last_id, verified_at=CASE WHEN EXCLUDED.status='VERIFIED' THEN NOW() ELSE {MANIFEST_TABLE}.verified_at END,
                    error=EXCLUDED.error
                """,
                (
                    archive_id, dataset_type, source_table, partition, part, key, row_count,
                    byte_size, sha256, status, json.dumps(_json_value(metadata)),
                    first_id, last_id, error,
                ),
            )
        conn.commit()

    def _archive_snapshot_session(self, session_id: int) -> Dict[str, Any]:
        dataset = "scalper_live_snapshots"
        partition = f"session={int(session_id)}"
        conn = self.db_connect(timeout_seconds=30.0)
        if conn is None:
            return {"ok": False, "error": "database unavailable", "session_id": session_id}
        try:
            self.ensure_manifest(conn)
            completed = self._verified_parts(conn, dataset, partition)
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT COUNT(*), MIN(id), MAX(id), COALESCE(MIN(captured_at), NOW()), COALESCE(MAX(captured_at), NOW()) "
                    f"FROM {SNAPSHOT_TABLE} WHERE session_id=%s", (int(session_id),)
                )
                count, min_id, max_id, first_at, last_at = cur.fetchone()
            count = int(count or 0)
            if count == 0:
                return {"ok": True, "session_id": session_id, "rows": 0, "deleted": 0}
            # Deterministic row-number parts keep retries idempotent.
            archive_id = f"snap-{session_id}-{uuid.uuid4().hex[:12]}"
            rows_done = 0
            part_index = 0
            current_part_path = None
            try:
                with conn.cursor(name=f"r2snap_{os.getpid()}_{threading.get_ident()}_{session_id}") as cur:
                    cur.itersize = SNAPSHOT_PART_ROWS
                    cur.execute(
                        f"""
                        SELECT id, session_id, captured_at, ticker, scrip_code, ltp, volume,
                               bid_prices::text, bid_qtys::text, ask_prices::text, ask_qtys::text,
                               depth_payload, depth_payload_codec, spread_pct, imbalance_l1, imbalance_l5,
                               microprice, microprice_edge_pct, ofi_proxy, book_pressure, depth_total_qty,
                               signal_direction, signal_side, signal_confidence, raw_direction, raw_confidence,
                               edge_pass, score_pass, l2_pass, rejection_reason, expected_move_pct,
                               remaining_edge_pct, l2_mode, data_honesty
                        FROM {SNAPSHOT_TABLE}
                        WHERE session_id=%s
                        ORDER BY id
                        """,
                        (int(session_id),),
                    )
                    while True:
                        batch = cur.fetchmany(SNAPSHOT_PART_ROWS)
                        if not batch:
                            break
                        part_index += 1
                        if part_index in completed:
                            rows_done += len(batch)
                            continue
                        fd, current_part_path = tempfile.mkstemp(prefix="r2snap_", suffix=".jsonl.gz")
                        os.close(fd)
                        digest = hashlib.sha256()
                        written = 0
                        first_source_id = int(batch[0][0])
                        last_source_id = int(batch[-1][0])
                        with gzip.open(current_part_path, "wb", compresslevel=5) as gz:
                            for row in batch:
                                data = {
                                    "archive_schema": ARCHIVE_SCHEMA_VERSION,
                                    "source_table": SNAPSHOT_TABLE,
                                    "id": row[0],
                                    "session_id": row[1],
                                    "captured_at": row[2],
                                    "ticker": row[3],
                                    "scrip_code": row[4],
                                    "ltp": row[5],
                                    "volume": row[6],
                                    "bid_prices_json": row[7],
                                    "bid_qtys_json": row[8],
                                    "ask_prices_json": row[9],
                                    "ask_qtys_json": row[10],
                                    "depth_payload": row[11],
                                    "depth_payload_codec": row[12],
                                    "spread_pct": row[13],
                                    "imbalance_l1": row[14],
                                    "imbalance_l5": row[15],
                                    "microprice": row[16],
                                    "microprice_edge_pct": row[17],
                                    "ofi_proxy": row[18],
                                    "book_pressure": row[19],
                                    "depth_total_qty": row[20],
                                    "signal_direction": row[21],
                                    "signal_side": row[22],
                                    "signal_confidence": row[23],
                                    "raw_direction": row[24],
                                    "raw_confidence": row[25],
                                    "edge_pass": row[26],
                                    "score_pass": row[27],
                                    "l2_pass": row[28],
                                    "rejection_reason": row[29],
                                    "expected_move_pct": row[30],
                                    "remaining_edge_pct": row[31],
                                    "l2_mode": row[32],
                                    "data_honesty": row[33],
                                }
                                line = (json.dumps(_json_value(data), ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")
                                gz.write(line)
                                digest.update(line)
                                written += 1
                        byte_size = os.path.getsize(current_part_path)
                        sha = digest.hexdigest()
                        key = _object_key(self.prefix, dataset, partition, part_index)
                        meta = {
                            "dataset": dataset, "source-table": SNAPSHOT_TABLE, "session-id": str(session_id),
                            "row-count": str(written), "first-source-id": str(first_source_id), "last-source-id": str(last_source_id),
                            "captured-from": str(first_at), "captured-to": str(last_at),
                        }
                        self._upload_file_verified(current_part_path, key, meta, sha, byte_size)
                        self._write_manifest(
                            conn, archive_id, dataset, SNAPSHOT_TABLE, partition, part_index, key,
                            written, byte_size, sha, meta, first_source_id, last_source_id, "VERIFIED"
                        )
                        rows_done += written
                        os.remove(current_part_path)
                        current_part_path = None
                if rows_done >= count:
                    with conn, conn.cursor() as cur:
                        cur.execute(f"DELETE FROM {SNAPSHOT_TABLE} WHERE session_id=%s", (int(session_id),))
                        deleted = int(cur.rowcount or 0)
                        cur.execute(f"UPDATE {MANIFEST_TABLE} SET source_deleted_at=NOW() WHERE dataset_type=%s AND source_partition=%s AND status='VERIFIED' AND source_deleted_at IS NULL", (dataset, partition))
                    return {"ok": True, "session_id": session_id, "rows": count, "deleted": deleted, "parts": part_index}
                return {"ok": False, "session_id": session_id, "error": "not all snapshot parts were verified", "rows": rows_done}
            finally:
                if current_part_path and os.path.exists(current_part_path):
                    os.remove(current_part_path)
        except Exception as exc:
            self._log("[R2_ARCHIVE] snapshot archive failed session=%s: %s", session_id, str(exc)[:400], error=True)
            return {"ok": False, "session_id": session_id, "error": str(exc)[:400]}
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _archive_candle_day(self, market: str, interval: str, day) -> Dict[str, Any]:
        dataset = "intraday_replay_candle_cache"
        partition = f"market={market}|interval={interval}|date={day}"
        conn = self.db_connect(timeout_seconds=30.0)
        if conn is None:
            return {"ok": False, "error": "database unavailable", "date": str(day)}
        path = None
        try:
            self.ensure_manifest(conn)
            if self._verified_parts(conn, dataset, partition):
                return {"ok": True, "date": str(day), "already_verified": True}
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT COUNT(*), COALESCE(MIN(ticker),''), COALESCE(MAX(ticker),''), COALESCE(SUM(pg_column_size(candles)),0) "
                    f"FROM {CANDLE_TABLE} WHERE market=%s AND interval=%s AND trading_date=%s",
                    (market, interval, day),
                )
                count, _mint, _maxt, raw_bytes = cur.fetchone()
                count = int(count or 0)
                raw_bytes = int(raw_bytes or 0)
                if count == 0:
                    return {"ok": True, "date": str(day), "rows": 0}
                cur.execute(
                    f"SELECT DISTINCT ticker FROM {CANDLE_TABLE} WHERE market=%s AND interval=%s AND trading_date=%s ORDER BY ticker",
                    (market, interval, day),
                )
                tickers = [str(r[0]) for r in cur.fetchall()]
            fd, path = tempfile.mkstemp(prefix="r2candle_", suffix=".jsonl.gz")
            os.close(fd)
            digest = hashlib.sha256()
            written = 0
            with conn.cursor(name=f"r2candle_{os.getpid()}_{threading.get_ident()}") as cur:
                cur.itersize = 128
                cur.execute(
                    f"SELECT ticker, market, interval, trading_date, candles::text, fetched_at "
                    f"FROM {CANDLE_TABLE} WHERE market=%s AND interval=%s AND trading_date=%s ORDER BY ticker",
                    (market, interval, day),
                )
                with gzip.open(path, "wb", compresslevel=5) as gz:
                    for ticker, mkt, iv, trading_day, candles_text, fetched_at in cur:
                        data = {
                            "archive_schema": ARCHIVE_SCHEMA_VERSION,
                            "source_table": CANDLE_TABLE,
                            "ticker": ticker,
                            "market": mkt,
                            "interval": iv,
                            "trading_date": trading_day,
                            "candles_json": candles_text,
                            "fetched_at": fetched_at,
                        }
                        line = (json.dumps(_json_value(data), ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")
                        gz.write(line)
                        digest.update(line)
                        written += 1
            byte_size = os.path.getsize(path)
            sha = digest.hexdigest()
            key = _object_key(self.prefix, dataset, partition, 1)
            meta = {
                "dataset": dataset, "source-table": CANDLE_TABLE, "market": market, "interval": interval,
                "trading-date": str(day), "row-count": str(written), "raw-bytes": str(raw_bytes),
                "tickers": json.dumps(tickers, separators=(",", ":")),
            }
            self._upload_file_verified(path, key, meta, sha, byte_size)
            self._write_manifest(
                conn, f"candle-{market}-{interval}-{day}-{uuid.uuid4().hex[:8]}", dataset, CANDLE_TABLE,
                partition, 1, key, written, byte_size, sha, meta, None, None, "VERIFIED"
            )
            # Delete only the verified source partition. Coverage/provenance remains in
            # market_data_coverage, so the dataset is still auditable and can be restored.
            with conn, conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {CANDLE_TABLE} WHERE market=%s AND interval=%s AND trading_date=%s",
                    (market, interval, day),
                )
                deleted = int(cur.rowcount or 0)
                cur.execute(
                    f"UPDATE {MANIFEST_TABLE} SET source_deleted_at=NOW() WHERE dataset_type=%s AND source_partition=%s AND status='VERIFIED' AND source_deleted_at IS NULL",
                    (dataset, partition),
                )
            return {"ok": True, "date": str(day), "rows": written, "deleted": deleted, "key": key}
        except Exception as exc:
            self._log("[R2_ARCHIVE] candle archive failed %s/%s/%s: %s", market, interval, day, str(exc)[:400], error=True)
            return {"ok": False, "date": str(day), "error": str(exc)[:400]}
        finally:
            if path and os.path.exists(path):
                try:
                    os.remove(path)
                except Exception:
                    pass
            try:
                conn.close()
            except Exception:
                pass

    def archive_if_needed(self, force: bool = False) -> Dict[str, Any]:
        with self._lock:
            self._last_run_at = _utc_now().isoformat()
        if not self.enabled:
            return {"ok": True, "enabled": False, "archived": False, "reason": "not_configured"}
        verification = self.verify_connection()
        if not verification.get("connected"):
            return {"ok": False, "enabled": True, "archived": False, "error": verification.get("error")}
        conn = self.db_connect(timeout_seconds=30.0)
        if conn is None:
            return {"ok": False, "enabled": True, "archived": False, "error": "database_unavailable"}
        try:
            self.ensure_manifest(conn)
            sizes = self._archivable_db_bytes(conn)
            total = sum(sizes.values())
            conn.commit()
            if not force and total < self.threshold_bytes:
                result = {"ok": True, "enabled": True, "archived": False, "reason": "below_threshold",
                          "archivable_bytes": total, "archivable_mb": round(total / 1024 / 1024, 2), "table_bytes": sizes}
                self._last_result = result
                return result
            self._log("[R2_ARCHIVE] trigger reached archivable_mb=%.2f threshold_mb=%s",
                      total / 1024 / 1024, self.threshold_bytes // (1024 * 1024))
        finally:
            try:
                conn.close()
            except Exception:
                pass

        results = {"snapshot_sessions": [], "candle_days": [], "truncated": []}
        # Archive completed live snapshots first. Never touch a running session.
        conn = self.db_connect(timeout_seconds=30.0)
        if conn is not None:
            try:
                self.ensure_manifest(conn)
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT id
                        FROM scalper_live_sessions
                        WHERE status IN ('complete','completed','stopped')
                          AND finished_at IS NOT NULL
                          AND finished_at < (NOW() - INTERVAL '30 minutes')
                          AND snapshot_count > 0
                        ORDER BY finished_at ASC, id ASC
                    """)
                    session_ids = [int(r[0]) for r in cur.fetchall()]
            except Exception:
                session_ids = []
            finally:
                conn.close()
            for session_id in session_ids:
                results["snapshot_sessions"].append(self._archive_snapshot_session(session_id))

        # Candle cache is only archived when no known writer is active.
        conn = self.db_connect(timeout_seconds=30.0)
        candle_safe = False
        try:
            if conn is not None:
                self.ensure_manifest(conn)
                candle_safe = not self._active_candle_writer(conn)
                if candle_safe:
                    with conn.cursor() as cur:
                        cur.execute(f"""
                            SELECT market, interval, trading_date
                            FROM {CANDLE_TABLE}
                            WHERE trading_date < CURRENT_DATE
                              AND fetched_at < (NOW() - INTERVAL '1 hour')
                            GROUP BY market, interval, trading_date
                            ORDER BY trading_date ASC, market ASC, interval ASC
                        """)
                        days = cur.fetchall()
                else:
                    days = []
        except Exception:
            days = []
        finally:
            if conn is not None:
                try: conn.close()
                except Exception: pass
        if candle_safe:
            for market, interval, day in days:
                results["candle_days"].append(self._archive_candle_day(str(market), str(interval), day))

        # When an archive has made a whole hot table empty, TRUNCATE returns the physical
        # space to PostgreSQL immediately. This is only done for an empty table and only
        # when there is no active candle writer, so it cannot remove unarchived data.
        conn = self.db_connect(timeout_seconds=30.0)
        if conn is not None:
            try:
                if candle_safe:
                    with conn, conn.cursor() as cur:
                        cur.execute(f"SELECT COUNT(*) FROM {CANDLE_TABLE}")
                        if int(cur.fetchone()[0] or 0) == 0:
                            cur.execute(f"TRUNCATE TABLE {CANDLE_TABLE}")
                            results["truncated"].append(CANDLE_TABLE)
                with conn.cursor() as cur:
                    cur.execute(f"SELECT COUNT(*) FROM {SNAPSHOT_TABLE}")
                    snapshot_empty = int(cur.fetchone()[0] or 0) == 0
                    cur.execute("SELECT EXISTS(SELECT 1 FROM scalper_live_sessions WHERE status='running')")
                    running_session = bool(cur.fetchone()[0])
                    if snapshot_empty and not running_session:
                        cur.execute(f"TRUNCATE TABLE {SNAPSHOT_TABLE}")
                        results["truncated"].append(SNAPSHOT_TABLE)
                conn.commit()
            except Exception as exc:
                try: conn.rollback()
                except Exception: pass
                self._log("[R2_ARCHIVE] reclaim/truncate step skipped: %s", str(exc)[:300])
            finally:
                conn.close()

        result = {"ok": True, "enabled": True, "archived": True, "results": results,
                  "completed_at": _utc_now().isoformat()}
        with self._lock:
            self._last_result = result
            self._last_error = None
        self._log("[R2_ARCHIVE] archive cycle complete snapshots=%s candle_partitions=%s truncated=%s",
                  len(results["snapshot_sessions"]), len(results["candle_days"]), results["truncated"])
        return result

    def status(self) -> Dict[str, Any]:
        v = self.verify_connection()
        with self._lock:
            return {
                **v,
                "last_run_at": self._last_run_at,
                "last_error": self._last_error,
                "last_result": self._last_result,
                "threshold_mb": self.threshold_bytes // (1024 * 1024),
                "interval_hours": self.interval_seconds // 3600,
                "schema_version": ARCHIVE_SCHEMA_VERSION,
            }

    def archived_candle_presence(self, tickers: Sequence[str], market: str, interval: str,
                                 trading_dates: Sequence[Any]) -> Dict[str, Set[Any]]:
        out: Dict[str, Set[Any]] = {str(t).upper(): set() for t in tickers}
        if not self.enabled or not out or not trading_dates:
            return out
        conn = self.db_connect(timeout_seconds=10.0)
        if conn is None:
            return out
        try:
            self.ensure_manifest(conn)
            for day in trading_dates:
                partition = f"market={market}|interval={interval}|date={day}"
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT metadata FROM {MANIFEST_TABLE} WHERE dataset_type='intraday_replay_candle_cache' AND source_partition=%s AND status='VERIFIED' LIMIT 1",
                        (partition,),
                    )
                    row = cur.fetchone()
                if not row:
                    continue
                metadata = row[0] if isinstance(row[0], dict) else json.loads(row[0] or "{}")
                raw_tickers = metadata.get("tickers") or "[]"
                try:
                    archived = {str(x).upper() for x in json.loads(raw_tickers)}
                except Exception:
                    archived = set()
                for ticker in out:
                    if ticker in archived:
                        out[ticker].add(day)
            return out
        except Exception:
            return out
        finally:
            conn.close()

    def load_archived_candle_rows(self, tickers: Sequence[str], market: str, interval: str,
                                  trading_dates: Sequence[Any]) -> Dict[str, Dict[Any, Any]]:
        out: Dict[str, Dict[Any, Any]] = {str(t).upper(): {} for t in tickers}
        if not self.enabled or not out or not trading_dates:
            return out
        wanted = set(out)
        client = self._client_or_raise()
        conn = self.db_connect(timeout_seconds=10.0)
        if conn is None:
            return out
        try:
            self.ensure_manifest(conn)
            for day in trading_dates:
                partition = f"market={market}|interval={interval}|date={day}"
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT object_key,sha256 FROM {MANIFEST_TABLE} WHERE dataset_type='intraday_replay_candle_cache' AND source_partition=%s AND status='VERIFIED' ORDER BY part_index",
                        (partition,),
                    )
                    objects = cur.fetchall()
                for key, expected_sha in objects:
                    obj = client.get_object(Bucket=self.bucket, Key=key)
                    body = obj["Body"]
                    raw = gzip.GzipFile(fileobj=body)
                    for raw_line in raw:
                        row = json.loads(raw_line.decode("utf-8"))
                        ticker = str(row.get("ticker") or "").upper()
                        if ticker not in wanted:
                            continue
                        candle_json = row.get("candles_json")
                        try:
                            candles = json.loads(candle_json) if isinstance(candle_json, str) else candle_json
                        except Exception:
                            continue
                        trading_day = row.get("trading_date")
                        if isinstance(trading_day, str):
                            try:
                                trading_day = _dt.date.fromisoformat(trading_day[:10])
                            except Exception:
                                pass
                        out[ticker][trading_day] = candles
                    body.close()
            return out
        finally:
            conn.close()


class R2ArchiveWorker:
    def __init__(self, db_connect, logger=None):
        self.store = R2ArchiveStore(db_connect, logger=logger)
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None

    def start(self):
        if self.thread and self.thread.is_alive():
            return
        self.thread = threading.Thread(target=self._run, name="r2-archive-worker", daemon=True)
        self.thread.start()

    def _run(self):
        # Verify once at boot, then only archive on the explicit 4-hour cadence.
        try:
            self.store.verify_connection(force=True)
        except Exception:
            pass
        next_run = time.monotonic()
        while not self.stop_event.is_set():
            now = time.monotonic()
            if now >= next_run:
                try:
                    self.store.archive_if_needed(force=False)
                except Exception as exc:
                    self.store._last_error = str(exc)[:400]
                    self.store._log("[R2_ARCHIVE] worker cycle failed: %s", str(exc)[:400], error=True)
                next_run = now + self.store.interval_seconds
            self.stop_event.wait(min(60, max(1, next_run - time.monotonic())))

    def status(self):
        return self.store.status()

    def stop(self):
        self.stop_event.set()
