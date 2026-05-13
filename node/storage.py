from __future__ import annotations

import asyncio
import sqlite3
import threading
import time
from typing import Optional


class Storage:
    def __init__(
        self,
        db_path: str,
        ttl_seconds: Optional[int] = None,
        max_items: Optional[int] = None,
    ) -> None:
        self._db_path = db_path
        self._ttl_seconds = ttl_seconds
        self._max_items = max_items
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")

    async def initialize(self) -> None:
        await asyncio.to_thread(self._initialize_sync)

    def _initialize_sync(self) -> None:
        with self._lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS cache_entries (
                    key TEXT PRIMARY KEY,
                    value BLOB NOT NULL,
                    expires_at REAL,
                    last_access REAL NOT NULL,
                    version INTEGER NOT NULL
                )
                """
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_cache_expires ON cache_entries(expires_at)"
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_cache_access ON cache_entries(last_access)"
            )
            self._conn.commit()

    async def get(self, key: str) -> Optional[bytes]:
        return await asyncio.to_thread(self._get_sync, key)

    def _get_sync(self, key: str) -> Optional[bytes]:
        now = time.time()
        with self._lock:
            cursor = self._conn.execute(
                "SELECT value, expires_at, version FROM cache_entries WHERE key = ?",
                (key,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            value, expires_at, _version = row
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

    async def get_entry(self, key: str) -> Optional[tuple[bytes, Optional[float], int]]:
        return await asyncio.to_thread(self._get_entry_sync, key)

    def _get_entry_sync(self, key: str) -> Optional[tuple[bytes, Optional[float], int]]:
        now = time.time()
        with self._lock:
            cursor = self._conn.execute(
                "SELECT value, expires_at, version FROM cache_entries WHERE key = ?",
                (key,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            value, expires_at, version = row
            if expires_at is not None and now >= expires_at:
                self._conn.execute("DELETE FROM cache_entries WHERE key = ?", (key,))
                self._conn.commit()
                return None
            return value, expires_at, int(version)

    async def put(self, key: str, value: bytes) -> None:
        await asyncio.to_thread(self._put_sync, key, value)

    def _put_sync(self, key: str, value: bytes) -> None:
        now = time.time()
        expires_at = now + self._ttl_seconds if self._ttl_seconds else None
        with self._lock:
            cursor = self._conn.execute(
                "SELECT version FROM cache_entries WHERE key = ?",
                (key,),
            )
            row = cursor.fetchone()
            version = (row[0] if row else 0) + 1
            self._conn.execute(
                """
                INSERT OR REPLACE INTO cache_entries (key, value, expires_at, last_access, version)
                VALUES (?, ?, ?, ?, ?)
                """,
                (key, value, expires_at, now, version),
            )
            self._prune_expired(now)
            self._enforce_max()
            self._conn.commit()

    async def upsert_replica(
        self,
        key: str,
        value: bytes,
        expires_at: Optional[float],
        version: int,
    ) -> bool:
        return await asyncio.to_thread(self._upsert_replica_sync, key, value, expires_at, version)

    def _upsert_replica_sync(
        self,
        key: str,
        value: bytes,
        expires_at: Optional[float],
        version: int,
    ) -> bool:
        now = time.time()
        with self._lock:
            cursor = self._conn.execute(
                "SELECT version FROM cache_entries WHERE key = ?",
                (key,),
            )
            row = cursor.fetchone()
            if row and row[0] >= version:
                return False
            self._conn.execute(
                """
                INSERT OR REPLACE INTO cache_entries (key, value, expires_at, last_access, version)
                VALUES (?, ?, ?, ?, ?)
                """,
                (key, value, expires_at, now, version),
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
        cursor = self._conn.execute("SELECT COUNT(*) FROM cache_entries")
        return int(cursor.fetchone()[0])

    async def list_entries(self) -> list[tuple[str, bytes, Optional[float], int]]:
        return await asyncio.to_thread(self._list_entries_sync)

    def _list_entries_sync(self) -> list[tuple[str, bytes, Optional[float], int]]:
        now = time.time()
        with self._lock:
            self._prune_expired(now)
            self._conn.commit()
            cursor = self._conn.execute(
                "SELECT key, value, expires_at, version FROM cache_entries",
            )
            rows = cursor.fetchall()
            return [(row[0], row[1], row[2], int(row[3])) for row in rows]

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
