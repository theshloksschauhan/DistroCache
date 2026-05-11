from backend.hashing import ConsistentHashRing


def test_consistent_hash_basic() -> None:
    ring = ConsistentHashRing(["a", "b", "c"], replicas=8)
    first = ring.get_nodes("alpha", 2)
    second = ring.get_nodes("alpha", 2)
    assert first == second
    assert len(first) == 2
    assert len(set(first)) == 2


def test_consistent_hash_remove_node() -> None:
    ring = ConsistentHashRing(["a", "b", "c"], replicas=8)
    ring.remove_node("b")
    nodes = ring.get_nodes("beta", 2)
    assert "b" not in nodes
