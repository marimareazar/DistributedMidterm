"""Real API workload smoke/full test. Full mode meets the PDF example sizes.

WARNING: Full test uploads >1 GB of logical input per protection mode; account
for >4.5 GB of total stored replicas/shards and 10k HTTP uploads. It is slow.
"""
import argparse
import json
import os
import random
import tempfile
import time
import uuid
from pathlib import Path
import httpx

URL = os.getenv('TASK4_URL', 'http://localhost:18100')
MIB = 1024 * 1024


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--quick', action='store_true', help='20 small files and 1 large 4 MiB file')
    p.add_argument('--mode', choices=['replication','erasure'], default='replication')
    args = p.parse_args()
    small_count = 20 if args.quick else 5000
    large_count = 1 if args.quick else 10
    large_size = 4 * MIB if args.quick else 100 * MIB
    rng = random.Random(2026)
    bucket = 'bench-' + uuid.uuid4().hex[:12]
    results = {'kind': 'real_HTTP_upload_benchmark', 'quick': args.quick, 'protection': args.mode,
               'bucket': bucket, 'small': {}, 'large': {}}
    with httpx.Client(timeout=360) as client:
        response = client.post(f'{URL}/task4/buckets', json={'name':bucket,'protection':args.mode})
        response.raise_for_status()
        for label in ['small', 'large']:
            count = small_count if label == 'small' else large_count
            size_total = 0
            new_total = 0
            t0 = time.perf_counter()
            for index in range(count):
                if label == 'small':
                    data = rng.randbytes(rng.randint(1024, 50 * 1024))
                    response = client.post(f'{URL}/task4/buckets/{bucket}/upload',
                        data={'key':f'{label}/{index:05}.bin'},
                        files={'file':('sample.bin',data)})
                else:
                    # Create large file on disk; avoids holding all files in memory.
                    with tempfile.NamedTemporaryFile(delete=False) as fh:
                        path = Path(fh.name)
                        for _ in range(large_size // MIB):
                            fh.write(os.urandom(MIB))
                        if large_size % MIB:
                            fh.write(os.urandom(large_size % MIB))
                    try:
                        with path.open('rb') as fh:
                            response = client.post(f'{URL}/task4/buckets/{bucket}/upload',
                                data={'key':f'{label}/{index:05}.bin'},
                                files={'file':('sample.bin',fh,'application/octet-stream')})
                    finally:
                        path.unlink(missing_ok=True)
                response.raise_for_status()
                info = response.json()
                size_total += info['size']
                new_total += info['new_unique_chunks']
                if (index + 1) % (100 if label == 'small' else 1) == 0:
                    print(f'{label}: {index+1}/{count} completed', flush=True)
            elapsed = time.perf_counter() - t0
            results[label] = {'objects': count, 'bytes_uploaded': size_total,
                'seconds': round(elapsed,3), 'throughput_MiB_per_s': round(size_total/MIB/elapsed,3),
                'new_unique_chunks':new_total}
    reports = Path('reports'); reports.mkdir(exist_ok=True)
    output = reports / f'task4_workload_{args.mode}_{"quick" if args.quick else "full"}.json'
    output.write_text(json.dumps(results,indent=2))
    print(json.dumps(results,indent=2))
    print('Saved:',output)


if __name__ == '__main__':
    main()
