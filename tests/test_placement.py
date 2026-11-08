import hashlib
import pytest
from shared.placement import HashRing
from benchmarks.placement_comparison import measure


def test_same_hash_has_same_owner():
    ring = HashRing(['node-1','node-2','node-3','node-4'])
    h = hashlib.sha256(b'example').hexdigest()
    assert ring.locate(h) == ring.locate(h)
    assert ring.locate(h) in ring.nodes


def test_ring_independent_of_node_order():
    one = HashRing(['node-1','node-2','node-3','node-4'])
    two = HashRing(['node-4','node-1','node-3','node-2'])
    for i in range(100):
        h = hashlib.sha256(str(i).encode()).hexdigest()
        assert one.locate(h) == two.locate(h)


def test_virtual_nodes_spread_load():
    result = measure(1000)
    assert result['adding_node_5']['moved_percent'] < 50
    assert result['removing_node_4']['moved_percent'] < 50
    assert len(result['original_distribution']) == 4


def test_replica_targets_unique():
    ring = HashRing(['node-1','node-2','node-3','node-4'])
    targets = ring.locate_replicas('0'*64, 3)
    assert len(targets) == len(set(targets)) == 3


def test_bad_ring_inputs():
    with pytest.raises(ValueError): HashRing([])
    with pytest.raises(ValueError): HashRing(['node-1','node-1'])
    with pytest.raises(ValueError): HashRing(['node-1'], 0)
    with pytest.raises(ValueError): HashRing(['node-1']).locate('not-a-hash')
