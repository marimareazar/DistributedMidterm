"""Task 3: metadata + chunk storage across the four existing nodes.

Task 3 stores ONE physical copy of each distinct chunk. Replication, erasure
coding, quorum, failure repair and physical rebalancing are Task 4 features.
"""
from __future__ import annotations

import hashlib
import os
import re
import tempfile
from collections import Counter
from pathlib import Path
from typing import Literal

import httpx
import psycopg
from psycopg.rows import dict_row
from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from shared.chunking import chunk_file, sha256_hex
from shared.placement import HashRing

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://storage:storagepass@localhost:15433/distributed_storage')
STORAGE_NODES = [node.strip() for node in os.getenv('STORAGE_NODES', 'node-1,node-2,node-3,node-4').split(',') if node.strip()]
NODE_URLS = {n: f'http://{n.replace("-", "")}:8000' for n in STORAGE_NODES}
RING = HashRing(STORAGE_NODES)
router = APIRouter(prefix='/task3', tags=['Task 3: Metadata and placement'])
BUCKET_RE = re.compile(r'^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$')


class CreateBucket(BaseModel):
    name: str = Field(min_length=3, max_length=63)


def connection():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def check_bucket(cur, bucket):
    if not cur.execute('SELECT name FROM buckets WHERE name=%s', (bucket,)).fetchone():
        raise HTTPException(404, f'Bucket {bucket!r} not found')


def validate_key(key: str):
    if not key or len(key) > 1024:
        raise HTTPException(422, 'Object key must contain 1 to 1024 characters')


def current_row(cur, bucket, key, version):
    row = cur.execute('''SELECT id, bucket, object_key AS key, version, size, mode, created_at
        FROM object_versions WHERE bucket=%s AND object_key=%s
        AND (%s::integer IS NULL OR version=%s) ORDER BY version DESC LIMIT 1''',
        (bucket, key, version, version)).fetchone()
    if not row:
        raise HTTPException(404, 'Object or requested version not found')
    return row


def manifest(cur, row):
    parts = cur.execute('''SELECT oc.position, BTRIM(oc.sha256) AS sha256, oc.size,
        c.node_id FROM object_chunks oc JOIN chunks c ON c.sha256=oc.sha256
        WHERE oc.object_version_id=%s ORDER BY oc.position''', (row['id'],)).fetchall()
    return {**row, 'chunk_count': len(parts), 'chunks': parts}


@router.post('/buckets', status_code=201)
def create_bucket(payload: CreateBucket):
    if not BUCKET_RE.fullmatch(payload.name):
        raise HTTPException(422, 'Use lowercase bucket name, 3-63 chars; letters, digits, dots and hyphens')
    with connection() as conn:
        row = conn.execute('''INSERT INTO buckets(name) VALUES (%s) ON CONFLICT DO NOTHING
            RETURNING name,created_at''', (payload.name,)).fetchone()
        if row is None:
            raise HTTPException(409, 'Bucket already exists')
        return row


@router.get('/buckets')
def list_buckets():
    with connection() as conn:
        return {'buckets': conn.execute('SELECT name,created_at FROM buckets ORDER BY name').fetchall()}


@router.get('/placement/{sha256}')
def placement(sha256: str):
    try:
        node = RING.locate(sha256)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    return {'sha256': sha256, 'target_node': node, 'virtual_nodes_per_node': RING.virtual_nodes}


@router.post('/buckets/{bucket}/upload', status_code=201)
def upload_object(
    bucket: str,
    key: str = Form(...),
    mode: Literal['fixed', 'fastcdc'] = Form('fixed'),
    file: UploadFile = File(...),
):
    validate_key(key)
    # Save multipart input in a temporary file: large files need not fit RAM.
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False) as temporary:
            temp_path = Path(temporary.name)
            while block := file.file.read(1024 * 1024):
                temporary.write(block)
        size = temp_path.stat().st_size
        # A single advisory lock prevents concurrent updates from interfering
        # with reference counting and chunk deletion in this coursework version.
        with connection() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT pg_advisory_xact_lock(74003003)')
                check_bucket(cur, bucket)
                chunks = []
                processed = set()
                new_hashes = []
                for data in chunk_file(temp_path, mode=mode):
                    h = sha256_hex(data)
                    chunks.append((h, len(data)))
                    if h in processed:
                        continue
                    processed.add(h)
                    existing = cur.execute('SELECT size,node_id FROM chunks WHERE sha256=%s', (h,)).fetchone()
                    if existing is not None:
                        if existing['size'] != len(data):
                            raise HTTPException(409, 'Hash collision or inconsistent chunk size')
                        url = NODE_URLS[existing['node_id']] + '/chunks/' + h
                        try:
                            response = httpx.head(url, timeout=15)
                        except httpx.HTTPError as exc:
                            raise HTTPException(503, f'Existing chunk node unavailable: {exc}') from exc
                        if response.status_code != 200:
                            raise HTTPException(503, f'Existing chunk inaccessible (HTTP {response.status_code})')
                    else:
                        owner = RING.locate(h)
                        try:
                            response = httpx.put(NODE_URLS[owner]+'/chunks/'+h,
                                                 content=data, timeout=60)
                        except httpx.HTTPError as exc:
                            raise HTTPException(503, f'Storage node unavailable: {exc}') from exc
                        if response.status_code not in (200, 201):
                            raise HTTPException(503, f'Storage node rejected chunk: HTTP {response.status_code}: {response.text[:200]}')
                        cur.execute('INSERT INTO chunks(sha256,size,node_id,ref_count) VALUES (%s,%s,%s,0)',
                                    (h, len(data), owner))
                        new_hashes.append(h)
                row = cur.execute('''INSERT INTO object_counters(bucket,object_key,latest_version)
                    VALUES (%s,%s,1) ON CONFLICT(bucket,object_key) DO UPDATE SET
                    latest_version=object_counters.latest_version+1 RETURNING latest_version''',
                    (bucket, key)).fetchone()
                version = row['latest_version']
                obj = cur.execute('''INSERT INTO object_versions(bucket,object_key,version,size,mode)
                    VALUES (%s,%s,%s,%s,%s) RETURNING id,bucket,object_key AS key,version,size,mode,created_at''',
                    (bucket, key, version, size, mode)).fetchone()
                counts = Counter(h for h, _ in chunks)
                for h, count in counts.items():
                    cur.execute('UPDATE chunks SET ref_count=ref_count+%s WHERE sha256=%s', (count, h))
                for position, (h, part_size) in enumerate(chunks):
                    cur.execute('''INSERT INTO object_chunks(object_version_id,position,sha256,size)
                        VALUES (%s,%s,%s,%s)''', (obj['id'], position, h, part_size))
                result = {**manifest(cur, obj), 'new_unique_chunks': len(new_hashes),
                          'reused_unique_chunks': len(processed)-len(new_hashes)}
                return result
    finally:
        if temp_path:
            temp_path.unlink(missing_ok=True)
        file.file.close()


@router.get('/buckets/{bucket}/objects')
def list_objects(bucket: str, prefix: str = ''):
    with connection() as conn:
        with conn.cursor() as cur:
            check_bucket(cur, bucket)
            return {'objects': cur.execute('''SELECT DISTINCT ON (object_key)
                object_key AS key,version,size,mode,created_at FROM object_versions
                WHERE bucket=%s AND LEFT(object_key,LENGTH(%s))=%s
                ORDER BY object_key,version DESC''', (bucket, prefix, prefix)).fetchall()}


@router.get('/buckets/{bucket}/objects/{key:path}/metadata')
def object_metadata(bucket: str, key: str, version: int | None = Query(None, ge=1)):
    with connection() as conn:
        with conn.cursor() as cur:
            return manifest(cur, current_row(cur, bucket, key, version))


@router.get('/buckets/{bucket}/objects/{key:path}/download')
def download_object(bucket: str, key: str, version: int | None = Query(None, ge=1)):
    with connection() as conn:
        with conn.cursor() as cur:
            item = manifest(cur, current_row(cur, bucket, key, version))
    def generate():
        for part in item['chunks']:
            h = part['sha256']
            try:
                with httpx.stream('GET', NODE_URLS[part['node_id']]+'/chunks/'+h, timeout=60) as resp:
                    resp.raise_for_status()
                    digest = hashlib.sha256()
                    for block in resp.iter_bytes(chunk_size=1024*1024):
                        digest.update(block)
                        yield block
                    if digest.hexdigest() != h:
                        raise RuntimeError('Chunk corruption detected during download')
            except httpx.HTTPError as exc:
                raise RuntimeError(f'Could not fetch {h}: {exc}') from exc
    return StreamingResponse(generate(), media_type='application/octet-stream',
                             headers={'Content-Disposition': 'attachment; filename="download.bin"',
                                      'Content-Length': str(item['size'])})


@router.get('/chunks')
def list_chunks():
    with connection() as conn:
        return {'chunks': conn.execute('SELECT BTRIM(sha256) AS sha256,size,node_id,ref_count FROM chunks ORDER BY sha256').fetchall()}


@router.get('/stats')
def stats():
    with connection() as conn:
        return conn.execute('''SELECT
          (SELECT COUNT(*) FROM buckets) AS bucket_count,
          (SELECT COUNT(*) FROM object_versions) AS object_versions,
          (SELECT COUNT(*) FROM chunks) AS unique_chunks,
          (SELECT COALESCE(SUM(size),0) FROM chunks) AS unique_bytes,
          (SELECT COALESCE(SUM(size),0) FROM object_versions) AS logical_bytes,
          (SELECT COALESCE(SUM(ref_count),0) FROM chunks) AS total_chunk_references''').fetchone()


@router.delete('/buckets/{bucket}/objects/{key:path}')
def delete_object(bucket: str, key: str, version: int | None = Query(None, ge=1)):
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT pg_advisory_xact_lock(74003003)')
            row = current_row(cur, bucket, key, version)
            references = cur.execute('''SELECT BTRIM(sha256) AS sha256, COUNT(*) AS times
                FROM object_chunks WHERE object_version_id=%s GROUP BY sha256''',
                (row['id'],)).fetchall()
            cur.execute('DELETE FROM object_versions WHERE id=%s', (row['id'],))
            zero_refs = []
            for ref in references:
                cur.execute('UPDATE chunks SET ref_count=ref_count-%s WHERE sha256=%s',
                            (ref['times'], ref['sha256']))
                remaining = cur.execute('SELECT node_id,ref_count FROM chunks WHERE sha256=%s',
                                        (ref['sha256'],)).fetchone()
                if remaining['ref_count'] == 0:
                    zero_refs.append((ref['sha256'], remaining['node_id']))
            # Remove unreferenced physical chunks. If deletion fails, preserve a
            # zero-ref metadata row so a later retry can garbage collect it.
            deleted = 0
            for h, node in zero_refs:
                try:
                    response = httpx.delete(NODE_URLS[node]+'/chunks/'+h, timeout=20)
                    if response.status_code in (200, 404):
                        cur.execute('DELETE FROM chunks WHERE sha256=%s AND ref_count=0', (h,))
                        deleted += 1
                except httpx.HTTPError:
                    pass
    return {'deleted_version': row['version'], 'physical_chunks_removed': deleted,
            'physical_deletions_pending': len(zero_refs)-deleted}
