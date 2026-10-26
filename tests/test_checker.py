"""Unit tests for the consistency checker's five checks.

Each check is exercised with a hand-built passing history and a hand-built violating history, so
the suite proves the checker can detect each violation. A checker that cannot fail proves
nothing; these tests are that proof at the logic level, and the R=1/W=1 integration run is the
proof end-to-end.
"""

from __future__ import annotations

from tests.integration.checker import checks
from tests.integration.checker.history import (
    DELETE,
    FAIL,
    GET,
    NOT_FOUND,
    OK,
    PUT,
    History,
    Operation,
    ReplicaEntry,
    Version,
)

C = "coord"


def v(ms: int, logical: int = 0) -> Version:
    return (ms, logical, C)


def put(
    key: str, value: str, ver: Version, *, invoke: float, complete: float, outcome: str = OK
) -> Operation:
    status = 200 if outcome == OK else 503
    return Operation(
        PUT,
        key,
        invoke,
        complete,
        outcome,
        value=value,
        version=ver if outcome == OK else None,
        status=status,
    )


def delete(key: str, ver: Version, *, invoke: float, complete: float) -> Operation:
    return Operation(DELETE, key, invoke, complete, OK, version=ver, status=200)


def get_hit(key: str, value: str, ver: Version, *, invoke: float, complete: float) -> Operation:
    return Operation(GET, key, invoke, complete, OK, value=value, version=ver, status=200)


def get_miss(key: str, *, invoke: float, complete: float) -> Operation:
    return Operation(GET, key, invoke, complete, NOT_FOUND, status=404)


def entry(node: str, ver: Version, tombstone: bool = False) -> ReplicaEntry:
    return ReplicaEntry(node=node, version=ver, tombstone=tombstone)


# --- (1) no acked write lost -------------------------------------------------------------------


def test_no_acked_write_lost_passes_when_final_value_is_current() -> None:
    h = History(
        ops=[put("a", "x", v(2), invoke=0.0, complete=1.0)],
        final_scan={"a": [entry("n1", v(2)), entry("n2", v(2))]},
    )
    r = checks.check_no_acked_write_lost(h)
    assert r.ok and r.checked == 1


def test_no_acked_write_lost_passes_when_a_later_delete_removed_it() -> None:
    h = History(
        ops=[
            put("a", "x", v(2), invoke=0.0, complete=1.0),
            delete("a", v(3), invoke=1.5, complete=2.0),
        ],
        final_scan={"a": [entry("n1", v(3), tombstone=True), entry("n2", v(3), tombstone=True)]},
    )
    assert checks.check_no_acked_write_lost(h).ok


def test_no_acked_write_lost_flags_a_vanished_key() -> None:
    h = History(ops=[put("a", "x", v(2), invoke=0.0, complete=1.0)], final_scan={})
    assert not checks.check_no_acked_write_lost(h).ok


def test_no_acked_write_lost_flags_an_older_final_value() -> None:
    h = History(
        ops=[put("a", "x", v(5), invoke=0.0, complete=1.0)],
        final_scan={"a": [entry("n1", v(3))]},
    )
    assert not checks.check_no_acked_write_lost(h).ok


def test_no_acked_write_lost_flags_a_tombstone_with_no_acked_delete() -> None:
    # Final state is a tombstone newer than the write, but no delete was ever acked: the write
    # was removed by something that never committed -> a lost acked write.
    h = History(
        ops=[put("a", "x", v(2), invoke=0.0, complete=1.0)],
        final_scan={"a": [entry("n1", v(3), tombstone=True)]},
    )
    assert not checks.check_no_acked_write_lost(h).ok


# --- (2) no fabricated read --------------------------------------------------------------------


def test_no_fabricated_read_passes_for_a_written_value() -> None:
    h = History(
        ops=[
            put("a", "x", v(1), invoke=0.0, complete=1.0),
            get_hit("a", "x", v(1), invoke=1.5, complete=1.6),
        ]
    )
    r = checks.check_no_fabricated_read(h)
    assert r.ok and r.checked == 1


def test_no_fabricated_read_accepts_a_value_from_a_failed_put() -> None:
    # A put that returned 503 may still have written its value to some replica; a later read of
    # it is legitimate, not fabricated.
    h = History(
        ops=[
            put("a", "y", v(0), invoke=0.0, complete=1.0, outcome=FAIL),
            get_hit("a", "y", v(1), invoke=1.5, complete=1.6),
        ]
    )
    assert checks.check_no_fabricated_read(h).ok


def test_no_fabricated_read_flags_an_unwritten_value() -> None:
    h = History(
        ops=[
            put("a", "x", v(1), invoke=0.0, complete=1.0),
            get_hit("a", "ghost", v(1), invoke=1.5, complete=1.6),
        ]
    )
    assert not checks.check_no_fabricated_read(h).ok


# --- (3) read-your-writes ----------------------------------------------------------------------


def test_read_your_writes_passes_when_the_read_sees_the_write() -> None:
    h = History(
        ops=[
            put("a", "x", v(2), invoke=0.0, complete=1.0),
            get_hit("a", "x", v(2), invoke=2.0, complete=2.1),
        ]
    )
    r = checks.check_read_your_writes(h)
    assert r.ok and r.checked == 1


def test_read_your_writes_ignores_a_read_that_overlapped_the_write() -> None:
    # The read was invoked before the write completed, so the write need not be visible.
    h = History(
        ops=[
            put("a", "x", v(2), invoke=0.0, complete=5.0),
            get_hit("a", "old", v(1), invoke=1.0, complete=1.1),
        ]
    )
    r = checks.check_read_your_writes(h)
    assert r.ok and r.checked == 0


def test_read_your_writes_flags_a_stale_hit() -> None:
    h = History(
        ops=[
            put("a", "x", v(2), invoke=0.0, complete=1.0),
            get_hit("a", "old", v(1), invoke=2.0, complete=2.1),
        ]
    )
    assert not checks.check_read_your_writes(h).ok


def test_read_your_writes_flags_a_miss_after_an_acked_write() -> None:
    h = History(
        ops=[
            put("a", "x", v(2), invoke=0.0, complete=1.0),
            get_miss("a", invoke=2.0, complete=2.1),
        ]
    )
    assert not checks.check_read_your_writes(h).ok


def test_read_your_writes_allows_a_miss_after_an_acked_delete() -> None:
    h = History(
        ops=[
            put("a", "x", v(2), invoke=0.0, complete=1.0),
            delete("a", v(3), invoke=1.2, complete=1.5),
            get_miss("a", invoke=2.0, complete=2.1),
        ]
    )
    assert checks.check_read_your_writes(h).ok


# --- (4) no resurrected delete -----------------------------------------------------------------


def test_no_resurrected_delete_allows_a_newer_write_after_the_delete() -> None:
    h = History(
        ops=[
            delete("a", v(3), invoke=0.0, complete=1.0),
            get_hit("a", "new", v(4), invoke=2.0, complete=2.1),
        ]
    )
    assert checks.check_no_resurrected_delete(h).ok


def test_no_resurrected_delete_flags_a_value_older_than_the_delete() -> None:
    h = History(
        ops=[
            delete("a", v(3), invoke=0.0, complete=1.0),
            get_hit("a", "old", v(2), invoke=2.0, complete=2.1),
        ]
    )
    r = checks.check_no_resurrected_delete(h)
    assert not r.ok and r.checked == 1


def test_no_resurrected_delete_ignores_a_read_that_overlapped_the_delete() -> None:
    h = History(
        ops=[
            delete("a", v(3), invoke=0.0, complete=5.0),
            get_hit("a", "old", v(2), invoke=1.0, complete=1.1),
        ]
    )
    assert checks.check_no_resurrected_delete(h).ok


# --- (5) replicas converge ---------------------------------------------------------------------


def test_replicas_converge_passes_when_all_agree() -> None:
    h = History(final_scan={"a": [entry("n1", v(2)), entry("n2", v(2)), entry("n3", v(2))]})
    r = checks.check_replicas_converge(h)
    assert r.ok and r.checked == 1


def test_replicas_converge_flags_differing_versions() -> None:
    h = History(final_scan={"a": [entry("n1", v(2)), entry("n2", v(1))]})
    assert not checks.check_replicas_converge(h).ok


def test_replicas_converge_flags_a_tombstone_value_split() -> None:
    h = History(final_scan={"a": [entry("n1", v(2)), entry("n2", v(2), tombstone=True)]})
    assert not checks.check_replicas_converge(h).ok


# --- the aggregate report ----------------------------------------------------------------------


def test_run_all_reports_clean_for_a_consistent_history() -> None:
    h = History(
        ops=[
            put("a", "x", v(2), invoke=0.0, complete=1.0),
            get_hit("a", "x", v(2), invoke=2.0, complete=2.1),
        ],
        final_scan={"a": [entry("n1", v(2)), entry("n2", v(2))]},
    )
    report = checks.run_all(h)
    assert report.ok, report.summary()
    assert len(report.results) == 5


def test_run_all_is_not_clean_when_any_check_fails() -> None:
    h = History(
        ops=[
            put("a", "x", v(2), invoke=0.0, complete=1.0),
            get_hit("a", "old", v(1), invoke=2.0, complete=2.1),  # stale: read-your-writes fails
        ],
        final_scan={"a": [entry("n1", v(2))]},
    )
    report = checks.run_all(h)
    assert not report.ok
    assert any("read-your-writes" in r.name for r in report.failed())


# --- the runnable entry point: never exit 0 without a verdict -----------------------------------


def test_main_exits_nonzero_without_a_false_verdict_on_run_error(monkeypatch, capsys) -> None:
    """If the run itself fails, ``main`` must exit non-zero and must NOT print a CLEAN or VIOLATIONS
    verdict, so an aborted run can never be mistaken for a clean "no violations" result."""
    from tests.integration.checker import runner

    def boom(_cfg: object) -> object:
        raise RuntimeError("cluster could not start")

    monkeypatch.setattr(runner, "run_checker", boom)
    code = runner.main(["--duration", "0"])
    out = capsys.readouterr().out
    assert code != 0
    assert "REPORT: CLEAN" not in out
    assert "REPORT: VIOLATIONS FOUND" not in out
    assert "REPORT: ERROR" in out  # the run always ends on an explicit verdict line
