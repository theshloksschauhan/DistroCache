"""A hybrid logical clock for the coordinator.

An HLC value is the tuple (physical_ms, logical, coordinator_id). It stays close to wall-clock
time through `physical_ms`, orders writes that land in the same millisecond through `logical`,
and stays totally ordered across coordinators through `coordinator_id`. Last-writer-wins
everywhere in DistroCache compares these tuples.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True, order=True)
class HLC:
    # order=True makes dataclass compare instances field by field, i.e. as this tuple:
    # physical time, then the logical counter, then the coordinator id as a stable tie-break.
    physical_ms: int
    logical: int
    coordinator_id: str


class HybridClock:
    """Mints strictly increasing HLC stamps, one per write."""

    def __init__(self, coordinator_id: str, now_ms: Callable[[], int] | None = None) -> None:
        self._coordinator_id = coordinator_id
        self._now_ms = now_ms or (lambda: int(time.time() * 1000))
        self._last_ms = 0
        self._logical = 0

    def tick(self) -> HLC:
        """Return a stamp greater than every stamp returned before.

        No `await` happens here, so the single-threaded event loop runs this to completion
        without interleaving two ticks; no lock is needed.
        """
        wall = self._now_ms()
        if wall > self._last_ms:
            # The clock advanced: adopt it and restart the logical counter.
            self._last_ms = wall
            self._logical = 0
        else:
            # Same millisecond, or the OS clock went backwards: keep the last time and
            # bump the counter so the stamp still increases.
            self._logical += 1
        return HLC(self._last_ms, self._logical, self._coordinator_id)
