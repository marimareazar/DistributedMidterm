"""Reed-Solomon 4+2 and quorum helpers for Task 4."""
from __future__ import annotations
import hashlib
from zfec import Encoder, Decoder

K, M = 4, 2


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def quorum(n: int) -> tuple[int, int]:
    """Majority W and R: W + R > N."""
    if not 1 <= n <= 6:
        raise ValueError('Replication factor must be 1..6')
    q = n // 2 + 1
    return q, q


def encode(data: bytes, k: int = K, m: int = M) -> list[bytes]:
    if k < 1 or m < 1:
        raise ValueError('Invalid erasure parameters')
    # zfec requires a nonzero, divisible-by-k input. Padding is removed after decode.
    length = max(k, ((len(data) + k - 1) // k) * k)
    padded = data.ljust(length, b'\0')
    block_len = length // k
    blocks = [padded[i * block_len:(i + 1) * block_len] for i in range(k)]
    return list(Encoder(k, k + m).encode(blocks))


def decode(shards: dict[int, bytes], original_size: int, k: int = K, m: int = M) -> bytes:
    if len(shards) < k:
        raise ValueError(f'Need {k} valid shards; got {len(shards)}')
    indices = sorted(shards)[:k]
    blocks = [shards[i] for i in indices]
    data = b''.join(Decoder(k, k + m).decode(blocks, indices))[:original_size]
    return data
