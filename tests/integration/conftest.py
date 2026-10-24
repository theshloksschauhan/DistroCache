from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from tests.integration.harness import Cluster


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo[None]) -> Any:
    # Keep each phase's report on the item so the fixture can tell a real failure
    # (print logs) from a pass or an expected xfail (stay quiet).
    report = yield
    setattr(item, f"rep_{report.when}", report)
    return report


@pytest.fixture
def cluster_factory(
    tmp_path: Path, request: pytest.FixtureRequest
) -> Iterator[Callable[..., Cluster]]:
    clusters: list[Cluster] = []

    def make(nodes: int = 3, **kwargs: Any) -> Cluster:
        cluster = Cluster(tmp_path / f"cluster{len(clusters)}", **kwargs)
        clusters.append(cluster)
        cluster.start(nodes)
        return cluster

    yield make

    failed = any(
        getattr(request.node, f"rep_{phase}", None) is not None
        and getattr(request.node, f"rep_{phase}").failed
        for phase in ("setup", "call")
    )
    for cluster in clusters:
        cluster.close(print_logs=failed)
