"""Deterministic analytical overhead and repair-transfer estimates for workloads.

No files are sent over the network: clearly label output as estimates, not measurements.
Upload workload tests can be run separately with Task 4 endpoints.
"""
import json
import math
import random
from pathlib import Path

MIB = 1024 * 1024


def compare(sizes):
    physical = [min(4 * MIB, max(0, size - start)) for size in sizes
                for start in range(0, size, 4 * MIB)]
    total = sum(sizes)
    # Per-chunk 4+2 RS padding (4-byte alignment); 3-replica each chunk exact.
    encoded = sum(6 * math.ceil(part / 4) for part in physical)
    one_ec_shard = sum(math.ceil(part / 4) for part in physical)
    # To repair one lost replica: read 1 full chunk + write 1 full chunk.
    # To repair one lost EC shard: download 4 shards + upload one shard.
    return {'objects': len(sizes), 'logical_bytes': total, 'chunks': len(physical),
            'replication_3': {'stored_bytes': 3 * total, 'storage_ratio': 3.0,
                              'repair_one_missing_shard_total_read_write_bytes': 2 * total},
            'erasure_4_2': {'stored_bytes': encoded,
                            'storage_ratio': round(encoded / total, 5) if total else 0,
                            'repair_one_missing_shard_total_read_write_bytes': 5 * one_ec_shard}}


def main():
    rng = random.Random(2026)
    small = [rng.randint(1024, 50 * 1024) for _ in range(5000)]
    large = [100 * MIB for _ in range(10)]
    result = {'type': 'analytical_estimates_not_measured_network_traffic',
              'small_5000_files_1_to_50_KiB': compare(small),
              'large_10_files_100_MiB_each': compare(large)}
    folder = Path('reports'); folder.mkdir(exist_ok=True)
    path = folder / 'task4_overhead_comparison.json'
    path.write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))
    print('Saved:', path)


if __name__ == '__main__':
    main()
