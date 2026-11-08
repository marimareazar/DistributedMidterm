"""Exercise live failure, automatic repair and disk-corruption scrubbing.
Run on the Windows host while Docker Compose is running. Does not delete volumes.
"""
import argparse
import hashlib
import os
import subprocess
import time
import uuid

import httpx

BASE = os.getenv('TASK4_URL', 'http://localhost:18100')
TOKEN = 'local-development-worker-token'


def compose(*args):
    cmd = ['docker', 'compose', *args]
    print('>', ' '.join(cmd), flush=True)
    subprocess.run(cmd, check=True)


def maintenance(client):
    response = client.post(BASE + '/task4/maintenance/run',
                           headers={'X-Worker-Token': TOKEN}, params={'limit': 200})
    response.raise_for_status()
    print('Maintenance:', response.json(), flush=True)
    return response.json()


def wait_online(client, node, expected, timeout=55):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        data = client.get(BASE + '/task4/cluster').json()
        rows = [n for n in data['nodes'] if n['node_id'] == node]
        if rows and rows[0]['healthy'] == expected:
            return
        time.sleep(2)
    raise RuntimeError(f'{node} did not become healthy={expected}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['replication','erasure'], default='replication')
    args = parser.parse_args()
    bucket = 'failure-' + uuid.uuid4().hex[:10]
    payload = (b'REPAIR TEST ' * 32768) + os.urandom(1024)
    node = None
    with httpx.Client(timeout=180) as client:
        result = client.post(BASE + '/task4/buckets', json={'name': bucket, 'protection': args.mode})
        result.raise_for_status()
        uploaded = client.post(BASE + f'/task4/buckets/{bucket}/upload',
                               data={'key': 'failure.bin'}, files={'file': ('failure.bin', payload)})
        uploaded.raise_for_status()
        print('Upload:', uploaded.json())
        meta = client.get(BASE + f'/task4/buckets/{bucket}/objects/failure.bin/metadata').json()
        first = meta['chunks'][0]['shards'][0]
        node = first['node_id']
        service = node.replace('-', '')
        try:
            # Lost node must be reported unhealthy after heartbeat threshold.
            compose('stop', service)
            wait_online(client, node, False)
            received = client.get(BASE + f'/task4/buckets/{bucket}/objects/failure.bin/download')
            received.raise_for_status()
            assert hashlib.sha256(received.content).digest() == hashlib.sha256(payload).digest()
            print('PASS: download survived one failed node')
            maintenance(client)
        finally:
            if node:
                compose('start', node.replace('-', ''))
                wait_online(client, node, True)
        # For erasure, the 6th shard can only be repaired after the failed node returns.
        # For replication, a missing replica can be replaced on a spare live node.
        for _ in range(5):
            repaired = maintenance(client)
            if repaired.get('missing_or_corrupt_shards', 0) == 0:
                break
            time.sleep(2)
        current = client.get(BASE + f'/task4/buckets/{bucket}/objects/failure.bin/metadata').json()
        shard = current['chunks'][0]['shards'][0]
        damaged_node = shard['node_id']
        bad_hash = shard['sha256']
        # Deliberately corrupt the disk bytes. The storage node checks SHA-256 on GET.
        snippet = ("from pathlib import Path; h='" + bad_hash + "'; "
                   "p=Path('/data')/h[:2]/h; p.write_bytes(b'BITROT'); print('corrupted',p)")
        compose('exec', '-T', damaged_node.replace('-', ''), 'python', '-c', snippet)
        raw = client.get(f'http://localhost:{18000+int(damaged_node.split("-")[-1])}/chunks/{bad_hash}')
        assert raw.status_code == 409, (raw.status_code, raw.text[:100])
        print('PASS: chunk integrity rejected corrupt bytes with HTTP 409')
        repaired = maintenance(client)
        assert repaired.get('repaired_shards', 0) >= 1, repaired
        restored = client.get(BASE + f'/task4/buckets/{bucket}/objects/failure.bin/download')
        restored.raise_for_status()
        assert restored.content == payload
        print('PASS: worker recovered corruption and object bytes match')
        print('PASS: Task 4 failure-and-repair scenario complete')


if __name__ == '__main__':
    # Pause autonomous worker to make screenshots and corruption assertions deterministic.
    compose('stop', 'repair-worker')
    try:
        main()
    finally:
        compose('start', 'repair-worker')
