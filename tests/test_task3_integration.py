"""Run against local Docker Compose after building metadata with Task 3 code."""
import hashlib
import os
import uuid
import httpx
import pytest

URL = os.getenv('TASK3_URL', 'http://localhost:18100')


@pytest.mark.integration
def test_versions_dedup_download_and_delete(tmp_path):
    bucket = 'test-' + uuid.uuid4().hex[:12]
    payload = (b'X' * (4 * 1024 * 1024)) + (b'X' * (4 * 1024 * 1024)) + b'last part'
    path = tmp_path / 'sample.bin'
    path.write_bytes(payload)
    with httpx.Client(timeout=120) as client:
        bucket_response = client.post(f'{URL}/task3/buckets', json={'name': bucket})
        assert bucket_response.status_code == 201, bucket_response.text
        with path.open('rb') as stream:
            r = client.post(f'{URL}/task3/buckets/{bucket}/upload',
                            data={'key':'folder/sample.bin','mode':'fixed'},
                            files={'file':('sample.bin', stream, 'application/octet-stream')})
        assert r.status_code == 201, r.text
        first = r.json()
        assert first['version'] == 1
        assert first['chunk_count'] == 3
        assert first['new_unique_chunks'] == 2
        assert first['size'] == len(payload)
        hashes = [x['sha256'] for x in first['chunks']]
        assert hashes[0] == hashes[1]
        with path.open('rb') as stream:
            r = client.post(f'{URL}/task3/buckets/{bucket}/upload',
                            data={'key':'folder/sample.bin','mode':'fixed'},
                            files={'file':('sample.bin', stream, 'application/octet-stream')})
        assert r.status_code == 201, r.text
        second = r.json()
        assert second['version'] == 2
        assert second['new_unique_chunks'] == 0
        meta = client.get(f'{URL}/task3/buckets/{bucket}/objects/folder/sample.bin/metadata')
        assert meta.status_code == 200
        assert meta.json()['version'] == 2
        downloaded = client.get(f'{URL}/task3/buckets/{bucket}/objects/folder/sample.bin/download')
        assert downloaded.status_code == 200
        assert hashlib.sha256(downloaded.content).digest() == hashlib.sha256(payload).digest()
        chunks = client.get(f'{URL}/task3/chunks').json()['chunks']
        for h in set(hashes):
            entry = next(x for x in chunks if x['sha256'] == h)
            assert entry['ref_count'] == (4 if h == hashes[0] else 2)
        assert client.delete(f'{URL}/task3/buckets/{bucket}/objects/folder/sample.bin?version=1').status_code == 200
        assert client.get(f'{URL}/task3/buckets/{bucket}/objects/folder/sample.bin/download').content == payload
        assert client.delete(f'{URL}/task3/buckets/{bucket}/objects/folder/sample.bin?version=2').status_code == 200
        remaining = client.get(f'{URL}/task3/chunks').json()['chunks']
        assert not any(x['sha256'] in set(hashes) for x in remaining)
