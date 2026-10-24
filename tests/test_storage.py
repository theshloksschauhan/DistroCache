import asyncio
import sqlite3
import threading
from pathlib import Path
from types import TracebackType

from node.storage import SCHEMA_VERSION, Storage, Version


def _value(entry: tuple[bytes, float | None, Version] | None) -> bytes | None:
    return None if entry is None else entry[0]


class RecordingLock:
    """Wraps a threading.Lock and counts how often it is entered."""

    def __init__(self, inner: threading.Lock) -> None:
        self.inner = inner
        self.entered = 0

    def __enter__(self) -> "RecordingLock":
        self.inner.acquire()
        self.entered += 1
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.inner.release()


def test_count_holds_the_connection_lock(tmp_path: Path) -> None:
    storage = Storage(str(tmp_path / "cache.db"), max_items=0)
    asyncio.run(storage.initialize())
    asyncio.run(storage.upsert("a", b"1", None, (1, 0, "coordinator")))
    lock = RecordingLock(storage._lock)
    storage._lock = lock  # type: ignore[assignment]

    count = asyncio.run(storage.count())

    assert count == 1
    assert lock.entered == 1, "count() used the shared sqlite connection without holding the lock"


def test_upsert_is_last_writer_wins(tmp_path: Path) -> None:
    storage = Storage(str(tmp_path / "cache.db"))
    asyncio.run(storage.initialize())

    assert asyncio.run(storage.upsert("k", b"old", None, (1, 0, "c"))) is True
    # An older version is rejected and does not change the stored value.
    assert asyncio.run(storage.upsert("k", b"stale", None, (0, 9, "c"))) is False
    assert _value(asyncio.run(storage.get("k"))) == b"old"
    # A newer version wins.
    assert asyncio.run(storage.upsert("k", b"new", None, (2, 0, "c"))) is True
    assert _value(asyncio.run(storage.get("k"))) == b"new"

    storage.close()


def test_upsert_of_the_same_version_is_idempotent(tmp_path: Path) -> None:
    # This is what makes a retried write safe: the same version applies once, then is a no-op.
    storage = Storage(str(tmp_path / "cache.db"))
    asyncio.run(storage.initialize())

    assert asyncio.run(storage.upsert("k", b"v", None, (5, 0, "c"))) is True
    assert asyncio.run(storage.upsert("k", b"v", None, (5, 0, "c"))) is False
    assert asyncio.run(storage.count()) == 1

    storage.close()


def test_schema_mismatch_drops_and_recreates(tmp_path: Path) -> None:
    db_path = tmp_path / "cache.db"
    # An old schema with a row and a wrong user_version.
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE cache_entries (key TEXT PRIMARY KEY, value BLOB)")
    conn.execute("INSERT INTO cache_entries (key, value) VALUES ('stale', 'x')")
    conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION + 99}")
    conn.commit()
    conn.close()

    storage = Storage(str(db_path))
    asyncio.run(storage.initialize())

    # The old data is gone and the new schema accepts a versioned write.
    assert asyncio.run(storage.count()) == 0
    assert asyncio.run(storage.upsert("k", b"v", None, (1, 0, "c"))) is True
    storage.close()
