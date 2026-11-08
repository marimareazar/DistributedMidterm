"""Streaming fixed-size and content-defined chunking using a deterministic Gear hash.

The content-defined implementation follows FastCDC's central technique (Gear-based
rolling fingerprint and min/avg/max chunk sizes). It is a simple educational
FastCDC-style implementation, not an optimized implementation of every feature
of the original FastCDC algorithm.
"""
from __future__ import annotations

import hashlib
import math
from pathlib import Path
from typing import BinaryIO, Iterator, Literal

ChunkMode = Literal["fixed", "fastcdc"]
MIB = 1024 * 1024


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _gear_table() -> tuple[int, ...]:
    # Deterministic 64-bit pseudorandom values, identical on every storage node.
    return tuple(int.from_bytes(hashlib.sha256(bytes([i])).digest()[:8], "big") for i in range(256))


GEAR = _gear_table()
MASK64 = (1 << 64) - 1


def fixed_chunks(stream: BinaryIO, chunk_size: int = 4 * MIB) -> Iterator[bytes]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    while chunk := stream.read(chunk_size):
        yield chunk


def fastcdc_chunks(
    stream: BinaryIO,
    min_size: int = 1 * MIB,
    avg_size: int = 4 * MIB,
    max_size: int = 8 * MIB,
) -> Iterator[bytes]:
    """Yield content-defined chunks without reading the whole file into RAM.

    Uses a Gear rolling fingerprint with a natural boundary near avg_size.
    Chunk boundaries are stable for identical content across objects.
    """
    if not (0 < min_size <= avg_size <= max_size):
        raise ValueError("Require 0 < min_size <= avg_size <= max_size")

    # About one cut per avg_size bytes after the minimum, capped at max_size.
    bits = max(1, round(math.log2(avg_size)))
    mask = (1 << bits) - 1
    buffer = bytearray()
    fingerprint = 0

    while block := stream.read(64 * 1024):
        for value in block:
            buffer.append(value)
            fingerprint = ((fingerprint << 1) + GEAR[value]) & MASK64
            length = len(buffer)
            if length >= max_size or (length >= min_size and (fingerprint & mask) == 0):
                yield bytes(buffer)
                buffer.clear()
                fingerprint = 0

    if buffer:
        yield bytes(buffer)


def chunk_file(
    file_path: str | Path,
    mode: ChunkMode = "fixed",
    *,
    chunk_size: int = 4 * MIB,
    min_size: int = 1 * MIB,
    avg_size: int = 4 * MIB,
    max_size: int = 8 * MIB,
) -> Iterator[bytes]:
    """Return file contents as ordered chunks. An empty file has no chunks."""
    with open(file_path, "rb") as stream:
        if mode == "fixed":
            yield from fixed_chunks(stream, chunk_size)
        elif mode == "fastcdc":
            yield from fastcdc_chunks(stream, min_size, avg_size, max_size)
        else:
            raise ValueError(f"Unknown chunking mode: {mode}")


def manifest_for_file(file_path: str | Path, mode: ChunkMode = "fixed", **kwargs) -> dict:
    """Build an ordered chunk manifest for later PostgreSQL persistence."""
    path = Path(file_path)
    ordered = []
    for part in chunk_file(path, mode, **kwargs):
        ordered.append({"sha256": sha256_hex(part), "size": len(part)})
    return {
        "file_name": path.name,
        "size": path.stat().st_size,
        "mode": mode,
        "chunk_count": len(ordered),
        "chunks": ordered,
    }
