import io
import hashlib
import os

import pytest

from shared.chunking import fixed_chunks, fastcdc_chunks, chunk_file, manifest_for_file, sha256_hex


@pytest.mark.parametrize("mode", ["fixed", "fastcdc"])
def test_file_reconstruction_and_hashes(tmp_path, mode):
    original = os.urandom(50_000) + b"repeatable content " * 1000
    path = tmp_path / "sample.bin"
    path.write_bytes(original)
    settings = {"chunk_size": 4096} if mode == "fixed" else {
        "min_size": 1024, "avg_size": 4096, "max_size": 8192
    }
    parts = list(chunk_file(path, mode=mode, **settings))
    assert parts
    assert b"".join(parts) == original
    assert [hashlib.sha256(p).hexdigest() for p in parts] == [sha256_hex(p) for p in parts]
    if mode == "fixed":
        assert all(len(p) == 4096 for p in parts[:-1])
    else:
        assert all(1024 <= len(p) <= 8192 for p in parts[:-1])
    manifest = manifest_for_file(path, mode=mode, **settings)
    assert manifest["chunk_count"] == len(parts)
    assert sum(chunk["size"] for chunk in manifest["chunks"]) == len(original)


def test_empty_file(tmp_path):
    path = tmp_path / "empty.bin"
    path.write_bytes(b"")
    assert list(chunk_file(path, mode="fixed")) == []
    assert list(chunk_file(path, mode="fastcdc")) == []


def test_determinism():
    source = (b"abcdefg" * 3000) + os.urandom(10_000)
    first = list(fastcdc_chunks(io.BytesIO(source), 1024, 4096, 8192))
    second = list(fastcdc_chunks(io.BytesIO(source), 1024, 4096, 8192))
    assert first == second


def test_invalid_sizes_and_mode(tmp_path):
    with pytest.raises(ValueError):
        list(fixed_chunks(io.BytesIO(b"data"), 0))
    with pytest.raises(ValueError):
        list(fastcdc_chunks(io.BytesIO(b"data"), 10, 5, 20))
    path = tmp_path / "x"
    path.write_bytes(b"x")
    with pytest.raises(ValueError):
        list(chunk_file(path, mode="invalid"))


def test_same_file_has_identical_chunk_ids(tmp_path):
    path = tmp_path / "file.bin"
    path.write_bytes(os.urandom(100_000))
    a = manifest_for_file(path, "fastcdc", min_size=1024, avg_size=4096, max_size=8192)
    b = manifest_for_file(path, "fastcdc", min_size=1024, avg_size=4096, max_size=8192)
    assert a["chunks"] == b["chunks"]
