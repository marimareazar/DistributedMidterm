"""Demonstrate chunking, reconstruction, and chunk-level reuse after insertion."""
import json
from pathlib import Path

from shared.chunking import chunk_file, sha256_hex, MIB


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "reports" / "chunking_comparison.json"
DATASET = ROOT / "data" / "chunking_demo.bin"


def compare(path: Path, mode: str) -> dict:
    opts = {"chunk_size": 4 * MIB} if mode == "fixed" else {
        "min_size": MIB, "avg_size": 4 * MIB, "max_size": 8 * MIB
    }
    originals = list(chunk_file(path, mode, **opts))
    original_hashes = set(map(sha256_hex, originals))
    original_bytes = path.read_bytes()
    altered = path.with_name("chunking_demo_edited.bin")
    # Insert 16 KiB in the middle to measure preserved chunks.
    position = len(original_bytes) // 2
    altered.write_bytes(original_bytes[:position] + b"EDIT" * 4096 + original_bytes[position:])
    changed = list(chunk_file(altered, mode, **opts))
    changed_hashes = set(map(sha256_hex, changed))
    assert b"".join(originals) == original_bytes
    assert b"".join(changed) == altered.read_bytes()
    result = {
        "method": mode,
        "original_chunks": len(originals),
        "edited_chunks": len(changed),
        "original_bytes": len(original_bytes),
        "edited_bytes": altered.stat().st_size,
        "reusable_unique_chunks": len(changed_hashes & original_hashes),
        "edited_unique_chunks": len(changed_hashes),
        "reuse_percent": round(100 * len(changed_hashes & original_hashes) / max(1, len(changed_hashes)), 2),
        "reconstruction_verified": True,
    }
    altered.unlink()
    return result


def main():
    DATASET.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    if not DATASET.exists():
        # Deterministic 18 MiB dataset with varied content and no huge RAM demand.
        import random
        rng = random.Random(2026)
        with DATASET.open("wb") as f:
            for _ in range(18):
                f.write(rng.randbytes(MIB))
    results = [compare(DATASET, "fixed"), compare(DATASET, "fastcdc")]
    OUTPUT.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(json.dumps(results, indent=2))
    print(f"\nSaved report results to: {OUTPUT}")


if __name__ == "__main__":
    main()
