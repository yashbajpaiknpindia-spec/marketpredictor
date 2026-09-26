"""Bounded HTTP and disk-backed report helpers for historical exports."""
import csv
import gzip
import io
import json
import sqlite3
import tempfile
from contextlib import contextmanager

import ijson


@contextmanager
def response_file(response, max_bytes=128 * 1024 * 1024):
    """Consume a stream=True response to disk, including HTTP transfer decoding.

    Never access response.content/text: instrument catalogues include huge derivative
    universes. The temporary file is deleted even on HTTP/parser/disk errors.
    """
    try:
        response.raise_for_status()
        with tempfile.TemporaryFile(mode="w+b") as f:
            size = 0
            for chunk in response.iter_content(chunk_size=64 * 1024):
                size += len(chunk)
                if size > max_bytes:
                    raise ValueError("Provider response exceeds the safe download size limit")
                f.write(chunk)
            f.seek(0)
            yield f
    finally:
        response.close()


def json_master_rows(file):
    """Iterate a gzip or plain JSON array without decompressing the whole file."""
    magic = file.read(2)
    file.seek(0)
    with gzip.GzipFile(fileobj=file) if magic == b"\x1f\x8b" else _borrow(file) as src:
        # Validate top-level shape without constructing the root object.
        first = src.read(1)
        while first and first.isspace():
            first = src.read(1)
        if first != b"[":
            raise ValueError("Instrument master must be a JSON array")
        src.seek(0)
        yield from ijson.items(src, "item")


def response_json(response):
    """Bound decoded HTTP bytes before constructing a candle/event JSON object."""
    with response_file(response, max_bytes=8 * 1024 * 1024) as src:
        return json.load(src)


def response_error(response):
    """Preserve useful provider diagnostics without downloading a whole error page."""
    try:
        return next(response.iter_content(chunk_size=300), b"").decode("utf-8", errors="replace")[:300]
    finally:
        response.close()


@contextmanager
def _borrow(file):
    yield file


class DiskRows:
    """Append and iterate report records with a fixed SQLite page cache."""
    def __init__(self, path):
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA cache_size=-1024")
        self.conn.execute("PRAGMA temp_store=FILE")
        self.conn.execute("CREATE TABLE rows (symbol TEXT, day TEXT, payload TEXT)")
        self.count = 0
        self.incomplete = 0

    def append(self, row):
        self.conn.execute("INSERT INTO rows VALUES (?,?,?)",
                          (row.get("symbol", ""), row.get("date", ""), json.dumps(row)))
        self.count += 1
        self.incomplete += row.get("complete") is False
        if self.count % 1000 == 0:
            self.conn.commit()

    def __len__(self):
        return self.count

    def __iter__(self):
        for (payload,) in self.conn.execute("SELECT payload FROM rows ORDER BY symbol, day"):
            yield json.loads(payload)

    def close(self):
        self.conn.close()


def write_csv_entry(zf, name, columns, rows):
    with zf.open(name, "w", force_zip64=True) as raw:
        with io.TextIOWrapper(raw, encoding="utf-8", newline="") as text:
            writer = csv.DictWriter(text, fieldnames=columns, extrasaction="ignore", lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)
