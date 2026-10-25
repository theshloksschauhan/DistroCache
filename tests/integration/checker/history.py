"""The recorded-history data model shared by the workload and the checks.

Nothing here touches a cluster or the network, so this module (and :mod:`checks`) can be
unit-tested with hand-built histories.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# A write version as the coordinator's HLC tuple: (physical_ms, logical, coordinator_id).
Version = tuple[int, int, str]

# Operation kinds.
PUT = "put"
GET = "get"
DELETE = "delete"

# Outcomes of an operation as observed by the client.
OK = "ok"  # 200: a put/delete was acked, or a get returned a value
NOT_FOUND = "not_found"  # 404: the get found no live value (all responders missed or a tombstone)
FAIL = "fail"  # 503 (below quorum) or a transport error: the result is genuinely unknown
ERROR = "error"  # an unexpected status such as a 5xx that is not 503 (a real defect)


@dataclass
class Operation:
    """One client operation and everything observed about it.

    A put/delete records the ``value`` and ``version`` the coordinator assigned (on OK). A get
    records the ``value`` and ``version`` it read back (on OK). Times are ``time.monotonic``
    seconds taken in the client, so happens-before within this process is exact.
    """

    op: str
    key: str
    invoke: float
    complete: float
    outcome: str
    value: str | None = None
    version: Version | None = None
    status: int | None = None
    worker: int = -1

    @property
    def is_acked_write(self) -> bool:
        return self.op == PUT and self.outcome == OK and self.version is not None

    @property
    def is_acked_delete(self) -> bool:
        return self.op == DELETE and self.outcome == OK and self.version is not None

    @property
    def is_read_hit(self) -> bool:
        return self.op == GET and self.outcome == OK and self.version is not None

    @property
    def is_read_miss(self) -> bool:
        return self.op == GET and self.outcome == NOT_FOUND


@dataclass
class ReplicaEntry:
    """What one replica holds for a key in the final scan (after healing)."""

    node: str
    version: Version
    tombstone: bool


@dataclass
class History:
    """The full record of a run: every operation, plus the final per-replica scan.

    ``final_scan[key]`` lists what each replica held for that key after the cluster healed, taken
    by reading every node directly (bypassing the coordinator), so convergence and durability can
    be judged against ground truth rather than a quorum read.
    """

    ops: list[Operation] = field(default_factory=list)
    final_scan: dict[str, list[ReplicaEntry]] = field(default_factory=dict)

    def keys(self) -> set[str]:
        return {op.key for op in self.ops} | set(self.final_scan)

    def final_winner(self, key: str) -> ReplicaEntry | None:
        """The highest-versioned entry any replica holds for ``key`` (value or tombstone)."""
        entries = self.final_scan.get(key, [])
        if not entries:
            return None
        return max(entries, key=lambda e: e.version)
