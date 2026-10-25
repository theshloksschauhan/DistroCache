"""A consistency checker for DistroCache.

Concurrent clients run random get/put/delete operations against a real cluster while a nemesis
kills and restarts nodes. Every operation is recorded with its invoke time, completion time,
result and version. After the cluster heals, :mod:`checks` verifies five safety properties
against the recorded history and a final per-replica scan.

The check functions in :mod:`checks` are pure (history in, violations out) and are unit-tested
with hand-built passing and violating histories in ``tests/test_checker.py`` — a checker that
cannot fail proves nothing, so its ability to detect each violation is tested directly.
"""
