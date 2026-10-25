"""Integration entry point: run the checker against a real cluster under a nemesis.

With N=3, R=2, W=2 (so R + W > N), every safety check must pass. The run lasts ~20 s by
default, enough for the nemesis to kill and restart nodes many times while clients hammer the
cluster; set ``CHECKER_DURATION`` higher for longer local runs.

The complementary proof that the checker can *fail* lives in two places: the hand-built
violating histories in ``tests/test_checker.py`` (unit), and the R=1/W=1 run driven from
``runner.main`` and recorded in ``docs/benchmarks/`` (end-to-end).
"""

from __future__ import annotations

import os

import pytest

from tests.integration.checker.history import ERROR
from tests.integration.checker.runner import CheckerConfig, run_checker

pytestmark = pytest.mark.integration


def test_cluster_is_consistent_under_a_nemesis_with_quorum_overlap() -> None:
    duration = float(os.getenv("CHECKER_DURATION", "15"))
    cfg = CheckerConfig(read_quorum=2, write_quorum=2, replication_factor=3, duration=duration)
    run = run_checker(cfg)

    # The run must actually have exercised the system, or a clean report proves nothing.
    assert run.op_count > 50, f"too few operations recorded: {run.op_count}"
    assert run.nemesis_actions > 0, "the nemesis never killed or restarted a node"

    # No request may crash the coordinator: 200/404/503 are the only acceptable statuses.
    bad = [op for op in run.history.ops if op.outcome == ERROR]
    assert not bad, f"coordinator returned unexpected statuses: {[op.status for op in bad][:5]}"

    # R + W > N, so every safety property must hold.
    assert run.report.ok, "consistency violations under R=2/W=2:\n" + run.report.summary()
