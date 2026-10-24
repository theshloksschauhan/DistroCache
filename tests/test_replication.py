from common.hashing import ConsistentHashRing


def test_replication_respects_node_count() -> None:
    ring = ConsistentHashRing(["n1", "n2"], replicas=16)
    nodes = ring.get_nodes("gamma", 3)
    assert len(nodes) == 2
    assert len(set(nodes)) == 2
