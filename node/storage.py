from __future__ import annotations

import asyncio
import logging
import sqlite3
import threading
import time

logger = logging.getLogger("distrocache.node.storage")

# Bump when the table shape changes. On a mismatch the cache is dropped and recreated.
SCHEMA_VERSION = 2

# A stored write version: (physical_ms, logical, coordinator_id), compared as a tuple.
Version = tuple[int, int, str]

# A stored row: (value, expires_at, version, tombstone). A tombstone has an empty value.
Entry = tuple[bytes, float | None, Version, bool]

# How long a tombstone is kept before garbage collection, when none is configured.
DEFAULT_TOMBSTONE_GC_SECONDS = 3600


class Storage:
    def __init__(
        self,
        db_path: str,
        max_items: int | None = None,
        tombstone_gc_seconds: int = DEFAULT_TOMBSTONE_GC_SECONDS,
    ) -> None:
        self._db_path = db_path
        self._max_items = max_items
        self._tombstone_gc_seconds = tombstone_gc_seconds
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    async def initialize(self) -> None:
        await asyncio.to_thread(self._initialize_sync)

    def _initialize_sync(self) -> None:
        with self._lock:
            current = self._conn.execute("PRAGMA user_version").fetchone()[0]
            if current != SCHEMA_VERSION:
                # It is a cache: a schema change drops the old data rather than migrating it.
                # Replicas and read repair refill it. A fresh file is version 0, not a mismatch.
                if current != 0:
                    logger.warning(
                        "cache schema v%s != expected v%s; dropping and recreating %s",
                        current,
                        SCHEMA_VERSION,
                        self._db_path,
                    )
                self._conn.execute("DROP TABLE IF EXISTS cache_entries")
                self._conn.execute(
                    """
                    CREATE TABLE cache_entries (
                        key TEXT PRIMARY KEY,
                        value BLOB NOT NULL,
                        expires_at REAL,
                        last_access REAL NOT NULL,
                        version_ms INTEGER NOT NULL,
                        version_logical INTEGER NOT NULL,
                        version_coordinator TEXT NOT NULL,
                        tombstone INTEGER NOT NULL DEFAULT 0,
                        deleted_at REAL
                    )
                    """
                )
                self._conn.execute("CREATE INDEX idx_cache_expires ON cache_entries(expires_at)")
                self._conn.execute("CREATE INDEX idx_cache_access ON cache_entries(last_access)")
                self._conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
                self._conn.commit()

    @staticmethod
    def _row_to_entry(row: tuple) -> Entry:
        value, expires_at, v_ms, v_logical, v_coord, tombstone = row
        return value, expires_at, (int(v_ms), int(v_logical), str(v_coord)), bool(tombstone)

    _SELECT = (
        "SELECT value, expires_at, version_ms, version_logical, version_coordinator, tombstone "
        "FROM cache_entries WHERE key = ?"
    )

    async def get(self, key: str) -> Entry | None:
        """Read for a client GET: returns the row (value or tombstone) and bumps last_access."""
        return await asyncio.to_thread(self._get_sync, key)

    def _get_sync(self, key: str) -> Entry | None:
        now = time.time()
        with self._lock:
            row = self._conn.execute(self._SELECT, (key,)).fetchone()
            if not row:
                return None
            entry = self._row_to_entry(row)
            if self._is_reapable(entry, now):
                self._conn.execute("DELETE FROM cache_entries WHERE key = ?", (key,))
                self._conn.commit()
                return None
            self._conn.execute("UPDATE cache_entries SET last_access = ? WHERE key = ?", (now, key))
            self._conn.commit()
            return entry

    async def get_entry(self, key: str) -> Entry | None:
        """Like get() but does not bump last_access (direct inspection, repair, rebalance)."""
        return await asyncio.to_thread(self._get_entry_sync, key)

    def _get_entry_sync(self, key: str) -> Entry | None:
        now = time.time()
        with self._lock:
            row = self._conn.execute(self._SELECT, (key,)).fetchone()
            if not row:
                return None
            entry = self._row_to_entry(row)
            if self._is_reapable(entry, now):
                self._conn.execute("DELETE FROM cache_entries WHERE key = ?", (key,))
                self._conn.commit()
                return None
            return entry

    async def upsert(
        self,
        key: str,
        value: bytes,
        expires_at: float | None,
        version: Version,
        tombstone: bool = False,
    ) -> bool:
        """Last-writer-wins store for values and tombstones.

        Returns True if the incoming version was newer than what was stored and was written.
        """
        return await asyncio.to_thread(
            self._upsert_sync, key, value, expires_at, version, tombstone
        )

    def _upsert_sync(
        self,
        key: str,
        value: bytes,
        expires_at: float | None,
        version: Version,
        tombstone: bool,
    ) -> bool:
        now = time.time()
        with self._lock:
            row = self._conn.execute(
                "SELECT version_ms, version_logical, version_coordinator "
                "FROM cache_entries WHERE key = ?",
                (key,),
            ).fetchone()
            if row is not None:
                stored = (int(row[0]), int(row[1]), str(row[2]))
                if version <= stored:
                    return False
            self._conn.execute(
                """
                INSERT OR REPLACE INTO cache_entries
                    (key, value, expires_at, last_access, version_ms, version_logical,
                     version_coordinator, tombstone, deleted_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    key,
                    b"" if tombstone else value,
                    expires_at,
                    now,
                    version[0],
                    version[1],
                    version[2],
                    1 if tombstone else 0,
                    now if tombstone else None,
                ),
            )
            self._prune(now)
            self._enforce_max()
            self._conn.commit()
            return True

    async def delete(self, key: str) -> bool:
        """Hard-remove a row (used by rebalance hand-off, not by client deletes)."""
        return await asyncio.to_thread(self._delete_sync, key)

    def _delete_sync(self, key: str) -> bool:
        with self._lock:
            cursor = self._conn.execute("DELETE FROM cache_entries WHERE key = ?", (key,))
            self._conn.commit()
            return cursor.rowcount > 0

    async def delete_if_version(self, key: str, version: Version) -> bool:
        """Compare-and-delete: remove the row only if its version is still `version`.

        A client write that changed the row since it was read bumps the version, so this is a
        no-op and the newer write survives the rebalance hand-off.
        """
        return await asyncio.to_thread(self._delete_if_version_sync, key, version)

    def _delete_if_version_sync(self, key: str, version: Version) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT version_ms, version_logical, version_coordinator "
                "FROM cache_entries WHERE key = ?",
                (key,),
            ).fetchone()
            if row is None:
                return False
            stored = (int(row[0]), int(row[1]), str(row[2]))
            if stored != version:
                return False  # changed since the snapshot; keep it
            self._conn.execute("DELETE FROM cache_entries WHERE key = ?", (key,))
            self._conn.commit()
            return True

    async def count(self) -> int:
        return await asyncio.to_thread(self._count_sync)

    def _count_sync(self) -> int:
        # The connection is shared across to_thread workers; every use must hold the lock.
        with self._lock:
            cursor = self._conn.execute("SELECT COUNT(*) FROM cache_entries")
            return int(cursor.fetchone()[0])

    async def list_entries(self) -> list[tuple[str, bytes, float | None, Version, bool]]:
        return await asyncio.to_thread(self._list_entries_sync)

    def _list_entries_sync(self) -> list[tuple[str, bytes, float | None, Version, bool]]:
        now = time.time()
        with self._lock:
            self._prune(now)
            self._conn.commit()
            rows = self._conn.execute(
                "SELECT key, value, expires_at, version_ms, version_logical, version_coordinator, "
                "tombstone FROM cache_entries",
            ).fetchall()
            return [
                (
                    row[0],
                    row[1],
                    row[2],
                    (int(row[3]), int(row[4]), str(row[5])),
                    bool(row[6]),
                )
                for row in rows
            ]

    def _is_reapable(self, entry: Entry, now: float) -> bool:
        """True if a read should treat the row as gone and delete it on the way out."""
        _value, expires_at, _version, tombstone = entry
        if tombstone:
            return False  # tombstones are reclaimed only by _prune's GC, never on a read
        return expires_at is not None and now >= expires_at

    def _prune(self, now: float) -> None:
        # Live entries past their TTL.
        self._conn.execute(
            "DELETE FROM cache_entries WHERE tombstone = 0 "
            "AND expires_at IS NOT NULL AND expires_at <= ?",
            (now,),
        )
        # Tombstones past the GC window.
        self._conn.execute(
            "DELETE FROM cache_entries WHERE tombstone = 1 "
            "AND deleted_at IS NOT NULL AND deleted_at + ? <= ?",
            (self._tombstone_gc_seconds, now),
        )

    def _enforce_max(self) -> None:
        if not self._max_items:
            return
        cursor = self._conn.execute("SELECT COUNT(*) FROM cache_entries")
        total = int(cursor.fetchone()[0])
        if total <= self._max_items:
            return
        excess = total - self._max_items
        # Only live rows are evictable: a tombstone must survive its GC window so a recent
        # delete is never lost to eviction. Tombstones are reclaimed by _prune instead.
        cursor = self._conn.execute(
            "SELECT key FROM cache_entries WHERE tombstone = 0 ORDER BY last_access ASC LIMIT ?",
            (excess,),
        )
        keys = [row[0] for row in cursor.fetchall()]
        if not keys:
            return
        self._conn.executemany("DELETE FROM cache_entries WHERE key = ?", [(k,) for k in keys])
