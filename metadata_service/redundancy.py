"""Task 4: replicated/quorum or 4+2 erasure coded objects and background repair.

Separate catalog tables deliberately preserve all existing Task 3 metadata.
"""
from __future__ import annotations

import hashlib
import os
import re
import tempfile
import time
from collections import Counter
from pathlib import Path
from typing import Literal

import httpx
import psycopg
from psycopg.rows import dict_row
from fastapi import APIRouter, File, Form, Header, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from shared.chunking import chunk_file
from shared.placement import HashRing
from shared.redundancy import decode, digest, encode, quorum

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://storage:storagepass@localhost:15433/distributed_storage')
NODE_NAMES = [f'node-{i}' for i in range(1, 7)]
NODE_URLS = {n: f'http://node{i}:8000' for i, n in enumerate(NODE_NAMES, 1)}
RING = HashRing(NODE_NAMES)
TOKEN = os.getenv('WORKER_TOKEN', 'local-development-worker-token')
OFFLINE_SECONDS = int(os.getenv('NODE_FAILURE_SECONDS', '15'))
router = APIRouter(prefix='/task4', tags=['Task 4: Redundancy and recovery'])
NAME_RE = re.compile(r'^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$')


def db():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def initialize():
    with db() as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS t4_buckets (
            name TEXT PRIMARY KEY REFERENCES buckets(name),
            protection TEXT NOT NULL CHECK(protection IN ('replication','erasure')),
            replicas INT NOT NULL DEFAULT 3 CHECK(replicas BETWEEN 1 AND 6),
            created_at TIMESTAMPTZ DEFAULT NOW())''')
        conn.execute('''CREATE TABLE IF NOT EXISTS t4_counters (
            bucket TEXT NOT NULL REFERENCES t4_buckets(name), object_key TEXT NOT NULL,
            latest_version INT NOT NULL, PRIMARY KEY(bucket,object_key))''')
        conn.execute('''CREATE TABLE IF NOT EXISTS t4_versions (
            id BIGSERIAL PRIMARY KEY, bucket TEXT NOT NULL REFERENCES t4_buckets(name),
            object_key TEXT NOT NULL, version INT NOT NULL,
            size BIGINT NOT NULL, chunking TEXT NOT NULL,
            protection TEXT NOT NULL, replicas INT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(bucket,object_key,version))''')
        conn.execute('''CREATE TABLE IF NOT EXISTS t4_chunks (
            id BIGSERIAL PRIMARY KEY, sha256 CHAR(64) NOT NULL, size BIGINT NOT NULL,
            protection TEXT NOT NULL, replicas INT NOT NULL, ref_count BIGINT NOT NULL DEFAULT 0,
            checked_at TIMESTAMPTZ,
            UNIQUE(sha256,protection,replicas))''')
        conn.execute('''CREATE TABLE IF NOT EXISTS t4_parts (
            version_id BIGINT NOT NULL REFERENCES t4_versions(id) ON DELETE CASCADE,
            position INT NOT NULL, chunk_id BIGINT NOT NULL REFERENCES t4_chunks(id),
            PRIMARY KEY(version_id,position))''')
        conn.execute('''CREATE TABLE IF NOT EXISTS t4_shards (
            chunk_id BIGINT NOT NULL REFERENCES t4_chunks(id) ON DELETE CASCADE,
            shard_index INT NOT NULL, node_id TEXT NOT NULL,
            sha256 CHAR(64) NOT NULL, size BIGINT NOT NULL,
            PRIMARY KEY(chunk_id,shard_index))''')
        conn.execute('CREATE INDEX IF NOT EXISTS ix_t4_parts_chunk ON t4_parts(chunk_id)')
        conn.execute('CREATE INDEX IF NOT EXISTS ix_t4_checked ON t4_chunks(checked_at)')
        conn.execute('''CREATE TABLE IF NOT EXISTS t4_maintenance (
            id BIGSERIAL PRIMARY KEY, ran_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            examined INT NOT NULL, corrupted_or_missing INT NOT NULL,
            repaired INT NOT NULL, bytes_transferred BIGINT NOT NULL,
            degraded INT NOT NULL)''')


class BucketRequest(BaseModel):
    name: str
    protection: Literal['replication','erasure'] = 'replication'
    replicas: int = Field(default=3, ge=1, le=6)


def bucket_row(cur, name):
    row = cur.execute('SELECT * FROM t4_buckets WHERE name=%s', (name,)).fetchone()
    if not row:
        raise HTTPException(404, 'Task 4 bucket not found')
    return row


def live_nodes(cur):
    rows = cur.execute('''SELECT node_id, last_seen FROM storage_nodes
        WHERE node_id=ANY(%s) AND last_seen >= NOW()-(%s * INTERVAL '1 second')''',
        (NODE_NAMES, OFFLINE_SECONDS)).fetchall()
    return {row['node_id'] for row in rows}


def node_url(name, sha):
    return NODE_URLS[name] + '/chunks/' + sha.strip()


def read_shard(node, sha):
    try:
        response = httpx.get(node_url(node, sha), timeout=20)
    except httpx.HTTPError:
        return None
    if response.status_code != 200:
        return None
    data = response.content
    return data if digest(data) == sha.strip() else None


def write_shard(node, data):
    h = digest(data)
    try:
        r = httpx.put(node_url(node, h), content=data, timeout=30)
    except httpx.HTTPError:
        return False
    return r.status_code in (200, 201)


def get_shards(cur, chunk_id):
    return cur.execute('''SELECT chunk_id,shard_index,node_id,BTRIM(sha256) AS sha256,size
        FROM t4_shards WHERE chunk_id=%s ORDER BY shard_index''', (chunk_id,)).fetchall()


def get_chunk(cur, chunk, strict=True):
    shards = get_shards(cur, chunk['id'])
    available = {}
    for s in shards:
        data = read_shard(s['node_id'], s['sha256'])
        if data is not None:
            available[s['shard_index']] = data
    if chunk['protection'] == 'replication':
        need = quorum(chunk['replicas'])[1] if strict else 1
        if len(available) < need:
            raise HTTPException(503, f'Replica read quorum unavailable: {len(available)}/{need}')
        data = next(iter(available.values()))
    else:
        if len(available) < 4:
            raise HTTPException(503, f'Erasure recovery needs 4 shards; found {len(available)}')
        try:
            data = decode(available, chunk['size'])
        except Exception as exc:
            raise HTTPException(503, f'Erasure decoding failed: {exc}') from exc
    if digest(data) != chunk['sha256'].strip():
        raise HTTPException(409, 'Reconstructed chunk hash mismatch')
    return data


def publish(cur, data, protection, replicas, live):
    h = digest(data)
    if protection == 'replication':
        q, _ = quorum(replicas)
        targets = RING.locate_replicas(h, replicas)
        parts = [(i, node, data) for i, node in enumerate(targets) if node in live]
        good = [(i, n, b) for i, n, b in parts if write_shard(n, b)]
        if len(good) < q:
            raise HTTPException(503, f'Write quorum not met: {len(good)}/{q}')
    else:
        # Six *different* live nodes are needed for a fully protected 4+2 write.
        targets = RING.locate_replicas(h, 6)
        pieces = encode(data)
        good = [(i, n, pieces[i]) for i, n in enumerate(targets)
                if n in live and write_shard(n, pieces[i])]
        if len(good) < 4:
            raise HTTPException(503, f'Need at least 4 successful erasure shards; wrote {len(good)}')
    row = cur.execute('''INSERT INTO t4_chunks(sha256,size,protection,replicas,ref_count)
        VALUES (%s,%s,%s,%s,0) RETURNING id''',
        (h, len(data), protection, replicas)).fetchone()
    for i, node, part in good:
        cur.execute('''INSERT INTO t4_shards(chunk_id,shard_index,node_id,sha256,size)
            VALUES (%s,%s,%s,%s,%s)''', (row['id'], i, node, digest(part), len(part)))
    return row['id']


@router.post('/buckets', status_code=201)
def create_bucket(payload: BucketRequest):
    if not NAME_RE.fullmatch(payload.name):
        raise HTTPException(422, 'Invalid bucket name')
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT pg_advisory_xact_lock(74004004)')
            if cur.execute('SELECT 1 FROM buckets WHERE name=%s', (payload.name,)).fetchone():
                raise HTTPException(409, 'Bucket already exists')
            cur.execute('INSERT INTO buckets(name) VALUES (%s)', (payload.name,))
            cur.execute('INSERT INTO t4_buckets(name,protection,replicas) VALUES (%s,%s,%s)',
                        (payload.name, payload.protection, payload.replicas))
    return payload.model_dump()


@router.get('/buckets')
def list_buckets():
    with db() as conn:
        return {'buckets': conn.execute('SELECT * FROM t4_buckets ORDER BY name').fetchall()}


@router.post('/buckets/{bucket}/upload', status_code=201)
def upload(bucket: str, key: str = Form(...), chunking: Literal['fixed','fastcdc'] = Form('fixed'),
           file: UploadFile = File(...)):
    if not key or len(key) > 1024:
        raise HTTPException(422, 'Object key must contain 1..1024 characters')
    temp = None
    try:
        with tempfile.NamedTemporaryFile(delete=False) as f:
            temp = Path(f.name)
            while block := file.file.read(1024 * 1024):
                f.write(block)
        with db() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT pg_advisory_xact_lock(74004004)')
                conf = bucket_row(cur, bucket)
                live = live_nodes(cur)
                if len(live) < (quorum(conf['replicas'])[0] if conf['protection']=='replication' else 4):
                    raise HTTPException(503, 'Insufficient live storage nodes')
                parts = []
                newly = 0
                for data in chunk_file(temp, mode=chunking):
                    h = digest(data)
                    row = cur.execute('''SELECT * FROM t4_chunks
                        WHERE sha256=%s AND protection=%s AND replicas=%s''',
                        (h, conf['protection'], conf['replicas'])).fetchone()
                    if row:
                        if row['size'] != len(data):
                            raise HTTPException(409, 'Chunk size mismatch')
                        # Ensure deduplication never silently reuses an unreadable chunk.
                        get_chunk(cur, row, strict=False)
                        chunk_id = row['id']
                    else:
                        chunk_id = publish(cur, data, conf['protection'], conf['replicas'], live)
                        newly += 1
                    parts.append(chunk_id)
                latest = cur.execute('''INSERT INTO t4_counters(bucket,object_key,latest_version)
                    VALUES (%s,%s,1) ON CONFLICT(bucket,object_key) DO UPDATE
                    SET latest_version=t4_counters.latest_version+1
                    RETURNING latest_version''', (bucket, key)).fetchone()['latest_version']
                version = cur.execute('''INSERT INTO t4_versions
                    (bucket,object_key,version,size,chunking,protection,replicas)
                    VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id''',
                    (bucket, key, latest, temp.stat().st_size, chunking,
                     conf['protection'], conf['replicas'])).fetchone()['id']
                for pos, cid in enumerate(parts):
                    cur.execute('INSERT INTO t4_parts(version_id,position,chunk_id) VALUES (%s,%s,%s)',
                                (version, pos, cid))
                for cid, count in Counter(parts).items():
                    cur.execute('UPDATE t4_chunks SET ref_count=ref_count+%s WHERE id=%s', (count, cid))
                return {'bucket': bucket, 'key': key, 'version': latest,
                        'protection': conf['protection'], 'replicas': conf['replicas'],
                        'size': temp.stat().st_size, 'chunks': len(parts),
                        'new_unique_chunks': newly}
    finally:
        if temp:
            temp.unlink(missing_ok=True)
        file.file.close()


def version_and_parts(cur, bucket, key, version):
    row = cur.execute('''SELECT * FROM t4_versions WHERE bucket=%s AND object_key=%s
        AND (%s::int IS NULL OR version=%s) ORDER BY version DESC LIMIT 1''',
        (bucket, key, version, version)).fetchone()
    if not row:
        raise HTTPException(404, 'Object/version not found')
    parts = cur.execute('''SELECT p.position,c.* FROM t4_parts p
        JOIN t4_chunks c ON c.id=p.chunk_id
        WHERE p.version_id=%s ORDER BY p.position''', (row['id'],)).fetchall()
    return row, parts


@router.get('/buckets/{bucket}/objects/{key:path}/metadata')
def metadata(bucket: str, key: str, version: int | None = Query(None, ge=1)):
    with db() as conn:
        with conn.cursor() as cur:
            row, parts = version_and_parts(cur, bucket, key, version)
            return {'bucket': bucket, 'key': key, 'version': row['version'],
                    'size': row['size'], 'protection': row['protection'],
                    'chunks': [{'position': p['position'], 'sha256': p['sha256'].strip(),
                                'shards': get_shards(cur, p['id'])} for p in parts]}


@router.get('/buckets/{bucket}/objects/{key:path}/download')
def download(bucket: str, key: str, version: int | None = Query(None, ge=1)):
    # Rebuild into a temporary file before replying so damaged objects produce
    # an HTTP error instead of a misleading 200 after streaming has begun.
    # This keeps large reconstructed objects off the Python heap.
    temp = None
    try:
        with db() as conn:
            with conn.cursor() as cur:
                row, parts = version_and_parts(cur, bucket, key, version)
                with tempfile.NamedTemporaryFile(delete=False) as stream:
                    temp = Path(stream.name)
                    for part in parts:
                        stream.write(get_chunk(cur, part))
        if temp.stat().st_size != row['size']:
            raise HTTPException(409, 'Object size mismatch')
    except Exception:
        if temp:
            temp.unlink(missing_ok=True)
        raise

    def send_file():
        try:
            with temp.open('rb') as stream:
                while block := stream.read(1024 * 1024):
                    yield block
        finally:
            temp.unlink(missing_ok=True)

    return StreamingResponse(send_file(), media_type='application/octet-stream',
                             headers={'X-Object-Version': str(row['version']),
                                      'Content-Length': str(row['size'])})


@router.get('/buckets/{bucket}/objects')
def objects(bucket: str):
    with db() as conn:
        return {'objects': conn.execute('''SELECT DISTINCT ON (object_key)
            object_key AS key,version,size,protection,created_at FROM t4_versions
            WHERE bucket=%s ORDER BY object_key,version DESC''', (bucket,)).fetchall()}


def referenced_elsewhere(cur, node, sha):
    if cur.execute('''SELECT 1 FROM t4_shards WHERE node_id=%s AND sha256=%s LIMIT 1''',
                   (node, sha)).fetchone():
        return True
    # Task 3 and Task 4 may share a physical SHA-256 file on the same node.
    return bool(cur.execute('SELECT 1 FROM chunks WHERE node_id=%s AND sha256=%s LIMIT 1',
                            (node, sha)).fetchone())


@router.delete('/buckets/{bucket}/objects/{key:path}')
def delete(bucket: str, key: str, version: int | None = Query(None, ge=1)):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT pg_advisory_xact_lock(74004004)')
            row, parts = version_and_parts(cur, bucket, key, version)
            counts = Counter(p['id'] for p in parts)
            cur.execute('DELETE FROM t4_versions WHERE id=%s', (row['id'],))
            removed = 0
            for cid, count in counts.items():
                current = cur.execute('UPDATE t4_chunks SET ref_count=ref_count-%s WHERE id=%s RETURNING ref_count',
                                      (count, cid)).fetchone()
                if current['ref_count'] == 0:
                    shards = get_shards(cur, cid)
                    cur.execute('DELETE FROM t4_chunks WHERE id=%s', (cid,))
                    for s in shards:
                        if not referenced_elsewhere(cur, s['node_id'], s['sha256']):
                            try:
                                httpx.delete(node_url(s['node_id'], s['sha256']), timeout=10)
                            except httpx.HTTPError:
                                pass  # Orphaned physical chunk can be cleaned later.
                    removed += 1
            return {'deleted_version': row['version'], 'unique_chunks_unreferenced': removed}


@router.get('/cluster')
def cluster():
    with db() as conn:
        with conn.cursor() as cur:
            rows = cur.execute('''SELECT node_id, free_space,chunk_count,last_seen,
                (last_seen >= NOW()-(%s * INTERVAL '1 second')) AS healthy
                FROM storage_nodes WHERE node_id=ANY(%s) ORDER BY node_id''',
                (OFFLINE_SECONDS, NODE_NAMES)).fetchall()
            # Healthy heartbeat is not enough to prove shard integrity: background scrubber checks that.
            last = cur.execute('SELECT * FROM t4_maintenance ORDER BY id DESC LIMIT 1').fetchone()
            return {'nodes': rows, 'last_maintenance': last}


def inspect_and_repair(cur, chunk, live):
    """Return (bad_count, repaired_count, bytes_transferred, still_degraded)."""
    shards = get_shards(cur, chunk['id'])
    available = {}
    bad = []
    for shard in shards:
        data = read_shard(shard['node_id'], shard['sha256']) if shard['node_id'] in live else None
        if data is None:
            bad.append(shard)
        else:
            available[shard['shard_index']] = data
    n = chunk['replicas'] if chunk['protection']=='replication' else 6
    missing_indices = [i for i in range(n) if i not in {s['shard_index'] for s in shards}]
    if not bad and not missing_indices:
        return 0, 0, 0, 0
    try:
        if chunk['protection'] == 'replication':
            if not available:
                return len(bad) + len(missing_indices), 0, 0, 1
            original = next(iter(available.values()))
            all_parts = [original] * n
        else:
            if len(available) < 4:
                return len(bad) + len(missing_indices), 0, 0, 1
            original = decode(available, chunk['size'])
            all_parts = encode(original)
        if digest(original) != chunk['sha256'].strip():
            return len(bad) + len(missing_indices), 0, 0, 1
    except Exception:
        return len(bad) + len(missing_indices), 0, 0, 1
    repaired, sent = 0, 0
    reconstruction_reads = (len(original) if chunk['protection'] == 'replication'
                            else sum(len(available[i]) for i in sorted(available)[:4]))
    # Keep healthy shard placement. Failed shard moves to another healthy node.
    healthy_owners = {s['node_id'] for s in shards if s['shard_index'] in available}
    for index in [s['shard_index'] for s in bad] + missing_indices:
        old = next((s for s in shards if s['shard_index'] == index), None)
        candidates = ([old['node_id']] if old and old['node_id'] in live else [])
        candidates += [node for node in RING.locate_replicas(chunk['sha256'].strip(), 6)
                       if node in live and node not in healthy_owners and node not in candidates]
        for target in candidates:
            if target in healthy_owners:
                continue
            data = all_parts[index]
            if write_shard(target, data):
                cur.execute('''INSERT INTO t4_shards(chunk_id,shard_index,node_id,sha256,size)
                    VALUES (%s,%s,%s,%s,%s) ON CONFLICT(chunk_id,shard_index) DO UPDATE SET
                    node_id=EXCLUDED.node_id,sha256=EXCLUDED.sha256,size=EXCLUDED.size''',
                    (chunk['id'], index, target, digest(data), len(data)))
                healthy_owners.add(target)
                repaired += 1
                sent += len(data)
                break
    if repaired:
        sent += reconstruction_reads
    return len(bad) + len(missing_indices), repaired, sent, int(repaired < len(bad) + len(missing_indices))


@router.post('/maintenance/run')
def maintenance(x_worker_token: str | None = Header(None), limit: int = Query(30, ge=1, le=200)):
    if x_worker_token != TOKEN:
        raise HTTPException(403, 'Worker token required')
    with db() as conn:
        with conn.cursor() as cur:
            locked = cur.execute('SELECT pg_try_advisory_xact_lock(74004004) AS ok').fetchone()['ok']
            if not locked:
                return {'busy': True}
            live = live_nodes(cur)
            chunks = cur.execute('''SELECT * FROM t4_chunks ORDER BY checked_at ASC NULLS FIRST,id LIMIT %s''',
                                 (limit,)).fetchall()
            examined = bad = repaired = sent = degraded = 0
            for chunk in chunks:
                failures, restored, traffic, is_degraded = inspect_and_repair(cur, chunk, live)
                examined += 1
                bad += failures
                repaired += restored
                sent += traffic
                degraded += is_degraded
                cur.execute('UPDATE t4_chunks SET checked_at=NOW() WHERE id=%s', (chunk['id'],))
            # Degraded count is for examined chunks; not necessarily the entire catalog.
            cur.execute('''INSERT INTO t4_maintenance(examined,corrupted_or_missing,repaired,bytes_transferred,degraded)
                VALUES (%s,%s,%s,%s,%s)''', (examined, bad, repaired, sent, degraded))
            return {'examined': examined, 'missing_or_corrupt_shards': bad,
                    'repaired_shards': repaired, 'repair_bytes': sent,
                    'degraded_in_batch': degraded, 'live_nodes': sorted(live)}
