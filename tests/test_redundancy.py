import hashlib
import os
import pytest
from shared.redundancy import digest, encode, decode, quorum


def test_quorums():
    for replicas in (1, 2, 3, 4, 5, 6):
        w, r = quorum(replicas)
        assert w + r > replicas
    assert quorum(3) == (2, 2)


@pytest.mark.parametrize('size', [0, 1, 3, 4, 5, 101, 4096, 1024 * 1024 + 3])
def test_erasure_any_two_shards_lost(size):
    data = os.urandom(size)
    coded = encode(data)
    assert len(coded) == 6
    assert len({len(x) for x in coded}) == 1
    assert sum(map(len, coded)) >= len(data) * 1.5
    for missing in [(0, 1), (0, 5), (2, 4), (4, 5)]:
        available = {i: shard for i, shard in enumerate(coded) if i not in missing}
        assert decode(available, size) == data


def test_erasure_fails_with_only_three_shards():
    data = b'data' * 100
    coded = encode(data)
    with pytest.raises(ValueError):
        decode({i: coded[i] for i in range(3)}, len(data))


def test_hash():
    assert digest(b'hello') == hashlib.sha256(b'hello').hexdigest()
