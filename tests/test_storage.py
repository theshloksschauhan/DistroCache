import asyncio
import threading
from pathlib import Path
from types import TracebackType

import pytest

from node.storage import Storage


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


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="Storage._count_sync reads the shared connection without the lock (fixed in Stage 1)",
)
def test_count_holds_the_connection_lock(tmp_path: Path) -> None:
    storage = Storage(str(tmp_path / "cache.db"), ttl_seconds=0, max_items=0)
    asyncio.run(storage.initialize())
    asyncio.run(storage.put("a", b"1"))
    lock = RecordingLock(storage._lock)
    storage._lock = lock  # type: ignore[assignment]

    count = asyncio.run(storage.count())

    assert count == 1
    assert lock.entered == 1, "count() used the shared sqlite connection without holding the lock"
