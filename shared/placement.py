"""Deterministic consistent hashing with virtual nodes and SHA-256 identifiers."""
import bisect
import hashlib


def _hash(text: str) -> int:
    return int.from_bytes(hashlib.sha256(text.encode()).digest(), 'big')


class HashRing:
    def __init__(self, nodes: list[str], virtual_nodes: int = 128):
        if not nodes or len(set(nodes)) != len(nodes):
            raise ValueError('Use a nonempty list of unique node IDs')
        if virtual_nodes < 1:
            raise ValueError('virtual_nodes must be positive')
        self.nodes = sorted(nodes)
        self.virtual_nodes = virtual_nodes
        positions = sorted((_hash(f'{n}:virtual:{i}'), n)
                           for n in self.nodes for i in range(virtual_nodes))
        self.positions = [p for p, _ in positions]
        self.owners = [n for _, n in positions]

    def locate(self, chunk_hash: str) -> str:
        if len(chunk_hash) != 64 or any(c not in '0123456789abcdef' for c in chunk_hash):
            raise ValueError('chunk_hash must be a SHA-256 hexadecimal digest')
        position = int(chunk_hash, 16)
        index = bisect.bisect_left(self.positions, position) % len(self.positions)
        return self.owners[index]

    def locate_replicas(self, chunk_hash: str, count: int) -> list[str]:
        """Future Task 4 helper: choose distinct successor nodes."""
        if count < 1 or count > len(self.nodes):
            raise ValueError('Replica count must be 1..number of nodes')
        index = bisect.bisect_left(self.positions, int(chunk_hash, 16)) % len(self.positions)
        result = []
        for offset in range(len(self.positions)):
            node = self.owners[(index + offset) % len(self.positions)]
            if node not in result:
                result.append(node)
                if len(result) == count:
                    return result
        raise RuntimeError('insufficient ring members')
