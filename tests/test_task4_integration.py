"""Requires `docker compose up -d --build` with six nodes."""
import hashlib
import os
import uuid
import httpx
import pytest

URL = os.getenv('TASK4_URL', 'http://localhost:18100')


@pytest.mark.integration
@pytest.mark.parametrize('protection', ['replication', 'erasure'])
def test_upload_version_dedup_download_delete(protection):
    bucket = 't4-' + uuid.uuid4().hex[:12]
    payload = (b'example repeated block-' * 12000) + os.urandom(777)
    with httpx.Client(timeout=180) as client:
        r = client.post(f'{URL}/task4/buckets', json={'name': bucket, 'protection': protection, 'replicas': 3})
        assert r.status_code == 201, r.text
        first = client.post(f'{URL}/task4/buckets/{bucket}/upload',
                            data={'key': 'demo/file.bin', 'chunking': 'fixed'},
                            files={'file': ('file.bin', payload)})
        assert first.status_code == 201, first.text
        assert first.json()['new_unique_chunks'] == 1
        second = client.post(f'{URL}/task4/buckets/{bucket}/upload',
                             data={'key': 'demo/file.bin', 'chunking': 'fixed'},
                             files={'file': ('file.bin', payload)})
        assert second.status_code == 201, second.text
        assert second.json()['version'] == 2
        assert second.json()['new_unique_chunks'] == 0
        meta = client.get(f'{URL}/task4/buckets/{bucket}/objects/demo/file.bin/metadata')
        assert meta.status_code == 200
        assert len(meta.json()['chunks'][0]['shards']) == (3 if protection == 'replication' else 6)
        got = client.get(f'{URL}/task4/buckets/{bucket}/objects/demo/file.bin/download')
        assert got.status_code == 200, got.text[:200] if got.status_code != 200 else ''
        assert hashlib.sha256(got.content).digest() == hashlib.sha256(payload).digest()
        assert client.delete(f'{URL}/task4/buckets/{bucket}/objects/demo/file.bin?version=1').status_code == 200
        assert client.get(f'{URL}/task4/buckets/{bucket}/objects/demo/file.bin/download').content == payload
        assert client.delete(f'{URL}/task4/buckets/{bucket}/objects/demo/file.bin?version=2').status_code == 200
