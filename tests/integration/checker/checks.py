"""The five safety checks, as pure functions over a :class:`History`.

Each check returns a :class:`CheckResult` listing any violations it found. The checks are
observers: they do not know N/R/W. Whether a violation is *expected* depends on the
configuration, and that decision lives with the caller:

* Under ``R + W > N`` (e.g. R=2/W=2/N=3) every check must pass — the integration test asserts a
  clean report.
* Under ``R + W <= N`` (e.g. R=1/W=1/N=3) the guarantees do not hold, so some checks are
  expected to report violations — the failure demo asserts the report is *not* clean, which is
  what proves the checker can actually fail.

The read-your-writes (check 3) and no-resurrection (check 4) properties are the ones that depend
on quorum overlap, so they are the ones the R=1/W=1 run surfaces.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .history import DELETE, PUT, History, Version


@dataclass
class CheckResult:
    name: str
    violations: list[str] = field(default_factory=list)
    checked: int = 0  # how many facts this check actually examined (0 => it proved nothing here)

    @property
    def ok(self) -> bool:
        return not self.violations


@dataclass
class CheckReport:
    results: list[CheckResult]

    @property
    def ok(self) -> bool:
        return all(r.ok for r in self.results)

    def failed(self) -> list[CheckResult]:
        return [r for r in self.results if not r.ok]

    def summary(self) -> str:
        lines = []
        for r in self.results:
            status = "PASS" if r.ok else f"FAIL ({len(r.violations)})"
            lines.append(f"  [{status}] {r.name}  (examined {r.checked})")
            for v in r.violations[:5]:
                lines.append(f"        - {v}")
            if len(r.violations) > 5:
                lines.append(f"        ... and {len(r.violations) - 5} more")
        return "\n".join(lines)


_MIN_VERSION: Version = (-1, -1, "")


def _max_acked_write_versions(history: History) -> dict[str, Version]:
    out: dict[str, Version] = {}
    for op in history.ops:
        if op.is_acked_write and op.version is not None:
            cur = out.get(op.key)
            if cur is None or op.version > cur:
                out[op.key] = op.version
    return out


def _max_acked_delete_versions(history: History) -> dict[str, Version]:
    out: dict[str, Version] = {}
    for op in history.ops:
        if op.is_acked_delete and op.version is not None:
            cur = out.get(op.key)
            if cur is None or op.version > cur:
                out[op.key] = op.version
    return out


def check_no_acked_write_lost(history: History) -> CheckResult:
    """(1) Each key's final winner has a version >= the highest acked write, unless a later
    acked delete removed it."""
    result = CheckResult("no acked write lost")
    writes = _max_acked_write_versions(history)
    deletes = _max_acked_delete_versions(history)
    for key, w in writes.items():
        result.checked += 1
        winner = history.final_winner(key)
        if winner is None:
            result.violations.append(f"{key}: acked write {w} but no replica holds the key")
            continue
        if winner.tombstone:
            # A final tombstone is only acceptable if an acked delete at >= the write explains it.
            if deletes.get(key, _MIN_VERSION) >= w:
                continue
            result.violations.append(
                f"{key}: final state is a tombstone {winner.version} but no acked delete "
                f">= the acked write {w} -- the write was lost to a delete"
            )
        elif winner.version < w:
            result.violations.append(
                f"{key}: final value version {winner.version} is older than acked write {w}"
            )
    return result


def check_no_fabricated_read(history: History) -> CheckResult:
    """(2) Every value a read returns must have been the value of some put attempt for that key."""
    result = CheckResult("no fabricated read")
    written: dict[str, set[str]] = {}
    for op in history.ops:
        # Count every put attempt, acked or not: even a put that returned 503 may have written
        # its value to some replicas, so the value is legitimately readable.
        if op.op == PUT and op.value is not None:
            written.setdefault(op.key, set()).add(op.value)
    for op in history.ops:
        if op.is_read_hit and op.value is not None:
            result.checked += 1
            if op.value not in written.get(op.key, set()):
                result.violations.append(
                    f"{op.key}: read returned value {op.value!r} that was never written"
                )
    return result


def _acked_events(history: History) -> dict[str, list[tuple[float, Version, bool]]]:
    """Per key, the (complete_time, version, is_delete) of every acked write and delete."""
    events: dict[str, list[tuple[float, Version, bool]]] = {}
    for op in history.ops:
        if op.is_acked_write and op.version is not None:
            events.setdefault(op.key, []).append((op.complete, op.version, False))
        elif op.is_acked_delete and op.version is not None:
            events.setdefault(op.key, []).append((op.complete, op.version, True))
    return events


def check_read_your_writes(history: History) -> CheckResult:
    """(3) A read that starts after a write's ack returns that version or newer.

    Holds only under R + W > N (quorum overlap); under R=1/W=1 this is exactly what breaks.

    A read **hit** must return a version >= the highest acked write/delete that *completed before
    the read was invoked*: returning something older means the read missed a committed event,
    which quorum overlap forbids. (Returning something *newer* — from a write still in flight when
    the read started — is always fine.)

    A read **miss** (404) is harder to judge because the 404 carries no version: it may be a
    tombstone that legitimately won. So a miss is only flagged when there is a committed value
    before the read *and no delete was even attempted* for that key before the read finished — if
    no delete exists, nothing could have produced a superseding tombstone, so the committed value
    had to be visible. This stays sound under concurrency (a delete acked just after the read
    began still counts as a possible explanation) at the cost of not flagging every missed write;
    the stale-hit rule and :func:`check_no_resurrected_delete` cover those.
    """
    result = CheckResult("read-your-writes")
    events = _acked_events(history)
    delete_invokes: dict[str, list[float]] = {}
    for op in history.ops:
        if op.op == DELETE:
            delete_invokes.setdefault(op.key, []).append(op.invoke)

    for op in history.ops:
        if op.is_read_hit:
            assert op.version is not None
            prior = [v for (t, v, _is_del) in events.get(op.key, []) if t <= op.invoke]
            if not prior:
                continue
            result.checked += 1
            required = max(prior)
            if op.version < required:
                result.violations.append(
                    f"{op.key}: read returned {op.version}, older than acked event {required} "
                    f"that committed before the read started"
                )
        elif op.is_read_miss:
            prior_writes = [
                v for (t, v, is_del) in events.get(op.key, []) if t <= op.invoke and not is_del
            ]
            if not prior_writes:
                continue  # no committed value the read was obliged to see
            result.checked += 1
            w_star = max(prior_writes)
            explained = any(iv < op.complete for iv in delete_invokes.get(op.key, []))
            if not explained:
                result.violations.append(
                    f"{op.key}: read at {op.invoke:.3f} missed committed write {w_star}, and no "
                    f"delete was attempted that could explain a tombstone"
                )
    return result


def check_no_resurrected_delete(history: History) -> CheckResult:
    """(4) Once a delete is acked, no later read returns a value older than that delete.

    A read that starts after an acked delete and returns a value with a version <= the delete's
    is a resurrection. (A value with a *newer* version is a legitimate write after the delete.)
    """
    result = CheckResult("no resurrected delete")
    deletes: dict[str, list[tuple[float, Version]]] = {}
    for op in history.ops:
        if op.is_acked_delete and op.version is not None:
            deletes.setdefault(op.key, []).append((op.complete, op.version))
    for op in history.ops:
        if not op.is_read_hit or op.version is None:
            continue
        prior_deletes = [v for (t, v) in deletes.get(op.key, []) if t <= op.invoke]
        if not prior_deletes:
            continue
        result.checked += 1
        latest_delete = max(prior_deletes)
        if op.version <= latest_delete:
            result.violations.append(
                f"{op.key}: read returned value at {op.version} <= acked delete "
                f"{latest_delete} that completed before the read -- the delete was resurrected"
            )
    return result


def check_replicas_converge(history: History) -> CheckResult:
    """(5) After the final read pass, every replica holding a key agrees on its version."""
    result = CheckResult("replicas converge")
    for key, entries in history.final_scan.items():
        if len(entries) <= 1:
            continue
        result.checked += 1
        versions = {e.version for e in entries}
        tombstones = {e.tombstone for e in entries}
        if len(versions) > 1 or len(tombstones) > 1:
            detail = ", ".join(
                f"{e.node}={'T' if e.tombstone else 'V'}{e.version}" for e in entries
            )
            result.violations.append(f"{key}: replicas diverge after the read pass: {detail}")
    return result


ALL_CHECKS = (
    check_no_acked_write_lost,
    check_no_fabricated_read,
    check_read_your_writes,
    check_no_resurrected_delete,
    check_replicas_converge,
)


def run_all(history: History) -> CheckReport:
    return CheckReport([check(history) for check in ALL_CHECKS])
