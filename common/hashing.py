from __future__ import annotations

import bisect
import hashlib
from dataclasses import dataclass
from typing import Iterable, List


@dataclass(frozen=True)
class RingNode:
    key: str
    hash_value: int


class ConsistentHashRing:
    def __init__(self, nodes: Iterable[str] | None = None, replicas: int = 128) -> None:
        self._replicas = replicas
        self._ring: List[RingNode] = []
        self._nodes: set[str] = set()
        if nodes:
            for node in nodes:
                self.add_node(node)

    def add_node(self, node: str) -> None:
        if node in self._nodes:
            return
        self._nodes.add(node)
        for replica in range(self._replicas):
            key = f"{node}:{replica}"
            self._ring.append(RingNode(key=node, hash_value=self._hash(key)))
        self._ring.sort(key=lambda item: item.hash_value)

    def remove_node(self, node: str) -> None:
        if node not in self._nodes:
            return
        self._nodes.remove(node)
        self._ring = [item for item in self._ring if item.key != node]

    def get_nodes(self, key: str, count: int) -> List[str]:
        if not self._ring or count <= 0:
            return []
        count = min(count, len(self._nodes))
        key_hash = self._hash(key)
        hashes = [item.hash_value for item in self._ring]
        start = bisect.bisect(hashes, key_hash)
        result: List[str] = []
        idx = start
        while len(result) < count:
            if idx >= len(self._ring):
                idx = 0
            node = self._ring[idx].key
            if node not in result:
                result.append(node)
            idx += 1
        return result

    @staticmethod
    def _hash(value: str) -> int:
        digest = hashlib.md5(value.encode("utf-8")).hexdigest()
        return int(digest, 16)
