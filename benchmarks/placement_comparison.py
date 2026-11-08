"""Measure fraction of chunks whose owner changes on joining or leaving a ring."""
import json
import random
from collections import Counter
from pathlib import Path
from shared.placement import HashRing


def measure(sample_size: int = 10000) -> dict:
    rng = random.Random(2026)
    hashes = [f'{rng.getrandbits(256):064x}' for _ in range(sample_size)]
    four = HashRing(['node-1', 'node-2', 'node-3', 'node-4'])
    five = HashRing(['node-1', 'node-2', 'node-3', 'node-4', 'node-5'])
    three = HashRing(['node-1', 'node-2', 'node-3'])
    original = [four.locate(h) for h in hashes]
    joined = [five.locate(h) for h in hashes]
    removed = [three.locate(h) for h in hashes]
    add_moved = sum(a != b for a, b in zip(original, joined))
    remove_moved = sum(a != b for a, b in zip(original, removed))
    return {
        'samples': sample_size,
        'virtual_nodes_per_node': 128,
        'original_distribution': dict(sorted(Counter(original).items())),
        'after_add_distribution': dict(sorted(Counter(joined).items())),
        'adding_node_5': {'moved_chunks': add_moved, 'moved_percent': round(100*add_moved/sample_size, 2)},
        'removing_node_4': {'moved_chunks': remove_moved, 'moved_percent': round(100*remove_moved/sample_size, 2)},
        'note': 'This is a placement simulation; physically migrating data is not part of Task 3.'
    }


if __name__ == '__main__':
    result = measure()
    path = Path('reports/placement_comparison.json')
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, indent=2), encoding='utf8')
    print(json.dumps(result, indent=2))
    print(f'Saved: {path}')
