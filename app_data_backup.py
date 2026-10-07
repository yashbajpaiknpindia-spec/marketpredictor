"""Disk-backed full application-data backup/restore for MarketPredictor.

The backup is a gzip-compressed tar archive containing one CSV per public base table
plus a manifest. It deliberately excludes environment secrets/API keys and source
code. CSV is produced with PostgreSQL COPY so JSON/arrays/newlines/NULL values remain
round-trippable without constructing the entire database in Python memory.
"""
from __future__ import annotations

import csv
import datetime as _dt
import hashlib
import json
import os
import re
import tarfile
import tempfile
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from psycopg2 import sql

BACKUP_FORMAT_VERSION = 1
PUBLIC_SCHEMA = "public"
_TABLE_NAME_RE = re.compile(r"[^A-Za-z0-9_.-]+")


def _safe_name(schema: str, table: str) -> str:
    return _TABLE_NAME_RE.sub("_", f"{schema}__{table}") + ".csv"


def _list_tables(conn) -> List[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_type = 'BASE TABLE'
            ORDER BY table_schema, table_name
            """,
            (PUBLIC_SCHEMA,),
        )
        return [{"schema": r[0], "table": r[1]} for r in cur.fetchall()]


def _columns(conn, schema: str, table: str) -> List[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type, udt_name, ordinal_position, is_nullable,
                   identity_generation, column_default
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema, table),
        )
        return [
            {
                "name": r[0],
                "data_type": r[1],
                "udt_name": r[2],
                "ordinal_position": int(r[3]),
                "is_nullable": r[4],
                "identity_generation": r[5],
                "column_default": r[6],
            }
            for r in cur.fetchall()
        ]


def _table_key(schema: str, table: str) -> str:
    return f"{schema}.{table}"


def _fk_edges(conn) -> List[Tuple[str, str]]:
    """Return (parent, child) table edges for a safe parent-first import order."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT
                ccu.table_schema, ccu.table_name,
                tc.table_schema, tc.table_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.constraint_column_usage ccu
              ON ccu.constraint_name = tc.constraint_name
             AND ccu.constraint_schema = tc.constraint_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema = %s
              AND ccu.table_schema = %s
            ORDER BY ccu.table_schema, ccu.table_name, tc.table_schema, tc.table_name
            """,
            (PUBLIC_SCHEMA, PUBLIC_SCHEMA),
        )
        return [(_table_key(r[0], r[1]), _table_key(r[2], r[3])) for r in cur.fetchall()]


def _topological_order(table_keys: Iterable[str], edges: Iterable[Tuple[str, str]]) -> List[str]:
    nodes = set(table_keys)
    children: Dict[str, set] = {n: set() for n in nodes}
    indegree: Dict[str, int] = {n: 0 for n in nodes}
    for parent, child in edges:
        if parent not in nodes or child not in nodes:
            continue
        if child not in children[parent]:
            children[parent].add(child)
            indegree[child] += 1
    ready = sorted([n for n, d in indegree.items() if d == 0])
    out: List[str] = []
    while ready:
        node = ready.pop(0)
        out.append(node)
        for child in sorted(children[node]):
            indegree[child] -= 1
            if indegree[child] == 0:
                ready.append(child)
        ready.sort()
    if len(out) != len(nodes):
        cycle = sorted(n for n, d in indegree.items() if d > 0)
        raise RuntimeError(
            "Cannot safely restore because the database contains a circular foreign-key dependency: "
            + ", ".join(cycle[:12])
        )
    return out


def _normalized_file_roots(file_roots: Optional[Iterable[Tuple[str, str]]]) -> List[Tuple[str, Path]]:
    out: List[Tuple[str, Path]] = []
    for label, raw_path in (file_roots or []):
        if not raw_path:
            continue
        root = Path(raw_path).expanduser().resolve()
        # Keep missing roots in the map: restore may need to recreate a data
        # directory that did not exist yet on a fresh deployment.
        out.append((str(label), root))
    return out


def _archive_application_files(archive: tarfile.TarFile, tmp_dir: Path, file_roots: List[Tuple[str, Path]]) -> List[Dict[str, Any]]:
    manifest: List[Dict[str, Any]] = []
    for label, root in file_roots:
        if not root.exists() or not root.is_dir():
            continue
        for path in sorted(root.rglob('*')):
            if not path.is_file() or path.is_symlink():
                continue
            rel = path.relative_to(root).as_posix()
            archive_name = f"files/{label}/{rel}"
            # Avoid absolute/traversal paths even if an administrator supplied a
            # surprising root/filename through an environment variable.
            if '..' in Path(archive_name).parts or Path(archive_name).is_absolute():
                raise RuntimeError(f"Unsafe application data file path: {archive_name}")
            archive.add(path, arcname=archive_name)
            h = hashlib.sha256()
            size = 0
            with open(path, 'rb') as fh:
                for chunk in iter(lambda: fh.read(1024 * 1024), b''):
                    h.update(chunk); size += len(chunk)
            manifest.append({'root_label': label, 'relative_path': rel, 'archive_name': archive_name, 'sha256': h.hexdigest(), 'bytes': size})
    return manifest


def create_backup(
    get_db_connection: Callable[..., Any],
    output_path: str,
    metadata: Optional[Dict[str, Any]] = None,
    progress_cb: Optional[Callable[[int, int, str], None]] = None,
    file_roots: Optional[Iterable[Tuple[str, str]]] = None,
) -> Dict[str, Any]:
    """Create a complete public-schema data backup without loading the DB into RAM."""
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    tmp_dir = Path(tempfile.mkdtemp(prefix="marketpredictor-backup-"))
    try:
        conn = get_db_connection(timeout_seconds=30)
        if conn is None:
            raise RuntimeError("DATABASE_URL not set.")
        try:
            # One repeatable-read snapshot makes a multi-table backup internally
            # consistent even while the live application is writing new rows.
            try:
                conn.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
            except Exception:
                pass
            tables = _list_tables(conn)
            table_manifest: List[Dict[str, Any]] = []
            with tarfile.open(output, mode="w:gz", compresslevel=6) as archive:
                total = len(tables)
                for index, item in enumerate(tables, 1):
                    schema, table = item["schema"], item["table"]
                    filename = _safe_name(schema, table)
                    csv_path = tmp_dir / filename
                    cols = _columns(conn, schema, table)
                    ident = sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(table))
                    copy_sql = sql.SQL(
                        "COPY {} TO STDOUT WITH (FORMAT csv, HEADER true, NULL '\\N', FORCE_QUOTE *)"
                    ).format(ident)
                    with open(csv_path, "wb") as fh:
                        with conn.cursor() as cur:
                            cur.copy_expert(copy_sql.as_string(cur), fh)
                    digest = hashlib.sha256()
                    size = 0
                    with open(csv_path, "rb") as fh:
                        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                            digest.update(chunk)
                            size += len(chunk)
                    archive.add(csv_path, arcname=f"tables/{filename}")
                    table_manifest.append(
                        {
                            "schema": schema,
                            "table": table,
                            "archive_name": f"tables/{filename}",
                            "columns": cols,
                            "csv_sha256": digest.hexdigest(),
                            "csv_bytes": size,
                        }
                    )
                    if progress_cb:
                        progress_cb(index, max(total, 1), table)

                normalized_roots = _normalized_file_roots(file_roots)
                file_manifest = _archive_application_files(archive, tmp_dir, normalized_roots)
                manifest = {
                    "format": "marketpredictor-full-data-backup",
                    "format_version": BACKUP_FORMAT_VERSION,
                    "created_at_utc": _dt.datetime.now(_dt.timezone.utc).isoformat(),
                    "includes": "all public-schema base-table rows",
                    "excludes": [
                        "DATABASE_URL and all environment secrets",
                        "application source code and deployed filesystem files",
                    ],
                    "metadata": metadata or {},
                    "tables": table_manifest,
                    "files": file_manifest,
                }
                payload = json.dumps(manifest, ensure_ascii=False, indent=2, default=str).encode("utf-8")
                info = tarfile.TarInfo("manifest.json")
                info.size = len(payload)
                info.mtime = int(_dt.datetime.now(_dt.timezone.utc).timestamp())
                import io
                archive.addfile(info, io.BytesIO(payload))
        finally:
            conn.close()
        return {
            "ok": True,
            "path": str(output),
            "table_count": len(tables),
            "file_count": len(file_manifest),
            "bytes": output.stat().st_size if output.exists() else 0,
            "format_version": BACKUP_FORMAT_VERSION,
        }
    finally:
        for p in sorted(tmp_dir.rglob("*"), reverse=True):
            try:
                if p.is_file() or p.is_symlink():
                    p.unlink()
                elif p.is_dir():
                    p.rmdir()
            except Exception:
                pass
        try:
            tmp_dir.rmdir()
        except Exception:
            pass


def _safe_extract_member(archive: tarfile.TarFile, member: tarfile.TarInfo, root: Path) -> Path:
    if not member.isfile():
        raise RuntimeError(f"Backup contains an unsupported archive entry: {member.name}")
    rel = Path(member.name)
    if rel.is_absolute() or ".." in rel.parts:
        raise RuntimeError(f"Unsafe backup archive path: {member.name}")
    target = (root / rel).resolve()
    if root.resolve() not in target.parents and target != root.resolve():
        raise RuntimeError(f"Unsafe backup archive path: {member.name}")
    return target


def restore_backup(
    get_db_connection: Callable[..., Any],
    input_path: str,
    progress_cb: Optional[Callable[[int, int, str], None]] = None,
    file_roots: Optional[Iterable[Tuple[str, str]]] = None,
) -> Dict[str, Any]:
    """Replace all exported public-schema tables with the backup contents.

    This is intentionally strict: the target database table inventory must match the
    backup exactly. That prevents a partial restore from silently deleting only some
    parts of the application state.
    """
    source = Path(input_path)
    if not source.exists():
        raise RuntimeError("Backup file not found.")
    tmp_dir = Path(tempfile.mkdtemp(prefix="marketpredictor-restore-"))
    try:
        with tarfile.open(source, mode="r:gz") as archive:
            members = archive.getmembers()
            manifest_member = next((m for m in members if m.name == "manifest.json"), None)
            if manifest_member is None:
                raise RuntimeError("Invalid MarketPredictor backup: manifest.json is missing.")
            manifest_file = archive.extractfile(manifest_member)
            manifest_payload = manifest_file.read() if manifest_file is not None else b""
            manifest = json.loads(manifest_payload.decode("utf-8"))
            if manifest.get("format") != "marketpredictor-full-data-backup" or int(manifest.get("format_version") or 0) != BACKUP_FORMAT_VERSION:
                raise RuntimeError("Unsupported MarketPredictor backup format/version.")
            table_entries = manifest.get("tables") or []
            if not isinstance(table_entries, list) or not table_entries:
                raise RuntimeError("Backup contains no application tables.")
            for member in members:
                if member.name == "manifest.json":
                    continue
                _safe_extract_member(archive, member, tmp_dir)
            for entry in table_entries:
                p = (tmp_dir / entry["archive_name"]).resolve()
                if tmp_dir.resolve() not in p.parents or not p.exists():
                    raise RuntimeError(f"Backup table payload is missing: {entry.get('archive_name')}")
                digest = hashlib.sha256()
                with open(p, "rb") as fh:
                    for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                        digest.update(chunk)
                if digest.hexdigest() != entry.get("csv_sha256"):
                    raise RuntimeError(f"Backup integrity check failed for {entry.get('schema')}.{entry.get('table')}.")

        normalized_roots = dict(_normalized_file_roots(file_roots))
        file_entries = manifest.get('files') or []
        for entry in file_entries:
            label = str(entry.get('root_label') or '')
            rel = str(entry.get('relative_path') or '')
            if label not in normalized_roots or not rel or Path(rel).is_absolute() or '..' in Path(rel).parts:
                raise RuntimeError(f"Backup contains an unsafe or unknown application file root/path: {label}/{rel}")
            archive_member = str(entry.get('archive_name') or '')
            member_path = (tmp_dir / archive_member).resolve()
            if tmp_dir.resolve() not in member_path.parents or not member_path.exists():
                raise RuntimeError(f"Backup application file payload is missing: {archive_member}")
            h = hashlib.sha256()
            with open(member_path, 'rb') as fh:
                for chunk in iter(lambda: fh.read(1024 * 1024), b''):
                    h.update(chunk)
            if h.hexdigest() != entry.get('sha256'):
                raise RuntimeError(f"Backup integrity check failed for application file {archive_member}.")

        conn = get_db_connection(timeout_seconds=30)
        if conn is None:
            raise RuntimeError("DATABASE_URL not set.")
        try:
            current_tables = _list_tables(conn)
            current_keys = {_table_key(x["schema"], x["table"]) for x in current_tables}
            backup_keys = {_table_key(x["schema"], x["table"]) for x in table_entries}
            missing_in_target = sorted(backup_keys - current_keys)
            missing_in_backup = sorted(current_keys - backup_keys)
            if missing_in_target or missing_in_backup:
                raise RuntimeError(
                    "Backup table inventory does not match this application database. "
                    f"Missing in target: {missing_in_target[:8]}; missing in backup: {missing_in_backup[:8]}."
                )

            edges = _fk_edges(conn)
            order = _topological_order(current_keys, edges)
            manifest_by_key = {_table_key(e["schema"], e["table"]): e for e in table_entries}
            with conn:
                with conn.cursor() as cur:
                    truncate_parts = [sql.SQL("{}.{}").format(sql.Identifier(PUBLIC_SCHEMA), sql.Identifier(k.split(".", 1)[1])) for k in order]
                    cur.execute(sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE").format(sql.SQL(", ").join(truncate_parts)).as_string(cur))
                    total = len(order)
                    for index, key in enumerate(order, 1):
                        entry = manifest_by_key[key]
                        table = entry["table"]
                        columns = [c["name"] for c in entry.get("columns") or []]
                        if not columns:
                            raise RuntimeError(f"Backup has no columns for {key}.")
                        ident = sql.SQL("{}.{}").format(sql.Identifier(PUBLIC_SCHEMA), sql.Identifier(table))
                        col_sql = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
                        copy_sql = sql.SQL(
                            "COPY {} ({}) FROM STDIN WITH (FORMAT csv, HEADER true, NULL '\\N')"
                        ).format(ident, col_sql)
                        p = tmp_dir / entry["archive_name"]
                        with open(p, "rb") as fh:
                            cur.copy_expert(copy_sql.as_string(cur), fh)
                        if progress_cb:
                            progress_cb(index, max(total, 1), key)

                    # Restore sequences behind serial/identity columns to max(id)+1.
                    cur.execute(
                        """
                        SELECT table_schema, table_name, column_name
                        FROM information_schema.columns
                        WHERE table_schema = %s
                          AND (identity_generation IS NOT NULL OR column_default LIKE 'nextval(%%')
                        ORDER BY table_schema, table_name, ordinal_position
                        """,
                        (PUBLIC_SCHEMA,),
                    )
                    seq_cols = cur.fetchall()
                    for schema, table, column in seq_cols:
                        seq_name = None
                        cur.execute("SELECT pg_get_serial_sequence(%s, %s)", (f"{schema}.{table}", column))
                        row = cur.fetchone()
                        if row:
                            seq_name = row[0]
                        if not seq_name:
                            continue
                        ident = sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(table))
                        col = sql.Identifier(column)
                        cur.execute(sql.SQL("SELECT MAX({}) FROM {}").format(col, ident))
                        max_value = cur.fetchone()[0]
                        if max_value is None:
                            cur.execute("SELECT setval(%s, 1, false)", (seq_name,))
                        else:
                            cur.execute("SELECT setval(%s, %s, true)", (seq_name, int(max_value)))
            # Restore managed filesystem data after the database transaction has
            # committed. Only files recorded in this backup are touched. Existing
            # files inside those managed roots that are not in the backup are removed
            # so a true full restore cannot silently keep stale research/cache data.
            backup_files_by_root = {}
            for entry in file_entries:
                backup_files_by_root.setdefault(str(entry['root_label']), set()).add(str(entry['relative_path']))
            for label, root in normalized_roots.items():
                allowed = backup_files_by_root.get(label, set())
                if root.exists() and root.is_dir():
                    existing_paths = sorted(root.rglob('*'), reverse=True)
                else:
                    existing_paths = []
                for existing in existing_paths:
                    if existing.is_file() and not existing.is_symlink():
                        if existing.relative_to(root).as_posix() not in allowed:
                            try: existing.unlink()
                            except OSError: pass
                    elif existing.is_dir():
                        try: existing.rmdir()
                        except OSError: pass
                for entry in file_entries:
                    if entry.get('root_label') != label:
                        continue
                    rel = Path(str(entry['relative_path']))
                    target = (root / rel).resolve()
                    if root not in target.parents and target != root:
                        raise RuntimeError(f"Unsafe restore target: {label}/{rel}")
                    target.parent.mkdir(parents=True, exist_ok=True)
                    source_member = (tmp_dir / str(entry['archive_name'])).resolve()
                    with open(source_member, 'rb') as src, open(target, 'wb') as dst:
                        for chunk in iter(lambda: src.read(1024 * 1024), b''):
                            dst.write(chunk)

            return {
                "ok": True,
                "table_count": len(table_entries),
                "file_count": len(file_entries),
                "manifest_metadata": manifest.get("metadata") or {},
                "message": "All application database tables and managed application-data files were restored from the backup.",
            }
        finally:
            conn.close()
    finally:
        for p in sorted(tmp_dir.rglob("*"), reverse=True):
            try:
                if p.is_file() or p.is_symlink():
                    p.unlink()
                elif p.is_dir():
                    p.rmdir()
            except Exception:
                pass
        try:
            tmp_dir.rmdir()
        except Exception:
            pass
