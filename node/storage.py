from __future__ import annotations

import asyncio
import logging
import sqlite3
import threading
import time

logger = logging.getLogger("distrocache.node.storage")

# Bump when the table shape changes. On a mismatch the cache is dropped and recreated.
SCHEMA_VERSION = 1

# A stored write version: (physical_ms, logical, coordinator_id), compared as a tuple.
Version = tuple[int, int, str]


class Storage:
    def __init__(self, db_path: str, max_items: int | None = None) -> None:
        self._db_path = db_path
        self._max_items = max_items
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
                        version_coordinator TEXT NOT NULL
                    )
                    """
                )
                self._conn.execute("CREATE INDEX idx_cache_expires ON cache_entries(expires_at)")
                self._conn.execute("CREATE INDEX idx_cache_access ON cache_entries(last_access)")
                self._conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
                self._conn.commit()

    async def get(self, key: str) -> bytes | None:
        return await asyncio.to_thread(self._get_sync, key)

    def _get_sync(self, key: str) -> bytes | None:
        now = time.time()
        with self._lock:
            cursor = self._conn.execute(
                "SELECT value, expires_at FROM cache_entries WHERE key = ?",
                (key,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            value, expires_at = row
            if expires_at is not None and now >= expires_at:
                self._conn.execute("DELETE FROM cache_entries WHERE key = ?", (key,))
                self._conn.commit()
                return None
            self._conn.execute(
                "UPDATE cache_entries SET last_access = ? WHERE key = ?",
                (now, key),
            )
            self._conn.commit()
            return value

    async def get_entry(self, key: str) -> tuple[bytes, float | None, Version] | None:
        return await asyncio.to_thread(self._get_entry_sync, key)

    def _get_entry_sync(self, key: str) -> tuple[bytes, float | None, Version] | None:
        now = time.time()
        with self._lock:
            row = self._conn.execute(
                "SELECT value, expires_at, version_ms, version_logical, version_coordinator "
                "FROM cache_entries WHERE key = ?",
                (key,),
            ).fetchone()
            if not row:
                return None
            value, expires_at, v_ms, v_logical, v_coord = row
            if expires_at is not None and now >= expires_at:
                self._conn.execute("DELETE FROM cache_entries WHERE key = ?", (key,))
                self._conn.commit()
                return None
            return value, expires_at, (int(v_ms), int(v_logical), str(v_coord))

    async def upsert(
        self,
        key: str,
        value: bytes,
        expires_at: float | None,
        version: Version,
    ) -> bool:
        """Last-writer-wins store. Returns True if the incoming version was newer and written."""
        return await asyncio.to_thread(self._upsert_sync, key, value, expires_at, version)

    def _upsert_sync(
        self,
        key: str,
        value: bytes,
        expires_at: float | None,
        version: Version,
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
                     version_coordinator)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (key, value, expires_at, now, version[0], version[1], version[2]),
            )
            self._prune_expired(now)
            self._enforce_max()
            self._conn.commit()
            return True

    async def delete(self, key: str) -> bool:
        return await asyncio.to_thread(self._delete_sync, key)

    def _delete_sync(self, key: str) -> bool:
        with self._lock:
            cursor = self._conn.execute("DELETE FROM cache_entries WHERE key = ?", (key,))
            self._conn.commit()
            return cursor.rowcount > 0

    async def count(self) -> int:
        return await asyncio.to_thread(self._count_sync)

    def _count_sync(self) -> int:
        # The connection is shared across to_thread workers; every use must hold the lock.
        with self._lock:
            cursor = self._conn.execute("SELECT COUNT(*) FROM cache_entries")
            return int(cursor.fetchone()[0])

    async def list_entries(self) -> list[tuple[str, bytes, float | None, Version]]:
        return await asyncio.to_thread(self._list_entries_sync)

    def _list_entries_sync(self) -> list[tuple[str, bytes, float | None, Version]]:
        now = time.time()
        with self._lock:
            self._prune_expired(now)
            self._conn.commit()
            rows = self._conn.execute(
                "SELECT key, value, expires_at, version_ms, version_logical, version_coordinator "
                "FROM cache_entries",
            ).fetchall()
            return [
                (row[0], row[1], row[2], (int(row[3]), int(row[4]), str(row[5]))) for row in rows
            ]

    def _prune_expired(self, now: float) -> None:
        self._conn.execute(
            "DELETE FROM cache_entries WHERE expires_at IS NOT NULL AND expires_at <= ?",
            (now,),
        )

    def _enforce_max(self) -> None:
        if not self._max_items:
            return
        cursor = self._conn.execute("SELECT COUNT(*) FROM cache_entries")
        total = int(cursor.fetchone()[0])
        if total <= self._max_items:
            return
        excess = total - self._max_items
        cursor = self._conn.execute(
            "SELECT key FROM cache_entries ORDER BY last_access ASC LIMIT ?",
            (excess,),
        )
        keys = [row[0] for row in cursor.fetchall()]
        if not keys:
            return
        self._conn.executemany("DELETE FROM cache_entries WHERE key = ?", [(k,) for k in keys])
