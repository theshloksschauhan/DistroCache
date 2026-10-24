from tests.integration.harness import parse_metric

SAMPLE = """\
# HELP distrocache_request_failures_total Total number of failed requests
# TYPE distrocache_request_failures_total counter
distrocache_request_failures_total{operation="get"} 2.0
distrocache_request_failures_total{operation="put"} 1.0
distrocache_request_failures_created{operation="get"} 1.7e+09
distrocache_rebalance_events_total 5.0
"""


def test_parse_metric_filters_by_name_and_labels() -> None:
    assert parse_metric(SAMPLE, "distrocache_request_failures_total", operation="get") == 2.0
    assert parse_metric(SAMPLE, "distrocache_request_failures_total") == 3.0
    assert parse_metric(SAMPLE, "distrocache_rebalance_events_total") == 5.0
    assert parse_metric(SAMPLE, "distrocache_missing_total") == 0.0
