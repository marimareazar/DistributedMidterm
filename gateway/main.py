"""Task 5 S3-like gateway over the existing Task 4 metadata service.

This service is a gateway, not a second storage implementation. Task 4 owns
chunk placement, redundancy, reconstruction, and object catalog mutations.
"""
from __future__ import annotations

import json
import os
import re
import tempfile
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timezone, timedelta
from pathlib import Path
from typing import Literal

import httpx
import psycopg
from psycopg.rows import dict_row
from fastapi import Depends, FastAPI, File, Form, Header, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse, Response
from pydantic import BaseModel, Field

from gateway.security import check_auth_and_rate

UPSTREAM = os.getenv('METADATA_URL', 'http://localhost:18100').rstrip('/')
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://storage:storagepass@localhost:15433/distributed_storage')
API_PREFIX = '/api'


def db():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def setup_db():
    with db() as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS t5_object_tags (
          version_id BIGINT PRIMARY KEY REFERENCES t4_versions(id) ON DELETE CASCADE,
          tags JSONB NOT NULL DEFAULT '{}'::jsonb)''')
        conn.execute('CREATE INDEX IF NOT EXISTS ix_t5_tags_gin ON t5_object_tags USING gin(tags)')


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_db()
    yield


app = FastAPI(title='DistributedMidterm S3-like Gateway', version='1.0', lifespan=lifespan)


@app.get('/health')
def health():
    return {'status': 'ok', 'service': 'gateway'}


# All /api routes require authentication and rate limiting.
from fastapi import APIRouter
router = APIRouter(prefix=API_PREFIX, dependencies=[Depends(check_auth_and_rate)])


def upstream_request(method: str, url: str, **kwargs) -> httpx.Response:
    try:
        with httpx.Client(timeout=httpx.Timeout(120.0, connect=10.0)) as client:
            response = client.request(method, UPSTREAM + url, **kwargs)
    except httpx.HTTPError as exc:
        raise HTTPException(503, 'Metadata service unavailable') from exc
    if response.status_code >= 400:
        try:
            message = response.json().get('detail', response.text)
        except ValueError:
            message = response.text
        raise HTTPException(response.status_code, str(message)[:500])
    return response


def response_json(response):
    try:
        return response.json()
    except ValueError as exc:
        raise HTTPException(502, 'Invalid upstream JSON') from exc


class BucketCreate(BaseModel):
    name: str
    protection: Literal['replication', 'erasure'] = 'replication'
    replicas: int = Field(default=3, ge=1, le=6)


@router.post('/buckets', status_code=201)
def create_bucket(payload: BucketCreate):
    return response_json(upstream_request('POST', '/task4/buckets', json=payload.model_dump()))


@router.get('/buckets')
def list_buckets():
    return response_json(upstream_request('GET', '/task4/buckets'))


def version_record(conn, bucket: str, key: str, version: int | None = None):
    row = conn.execute('''SELECT v.id, v.bucket, v.object_key, v.version,v.size,
       v.protection, v.chunking, v.created_at
       FROM t4_versions v WHERE v.bucket=%s AND v.object_key=%s
       AND (%s::int IS NULL OR v.version=%s)
       ORDER BY v.version DESC LIMIT 1''', (bucket, key, version, version)).fetchone()
    if row is None:
        raise HTTPException(404, 'Object/version not found')
    return row


def parse_tags(value: str) -> dict[str, str]:
    try:
        tags = json.loads(value)
    except (ValueError, TypeError) as exc:
        raise HTTPException(422, 'tags must be a JSON object') from exc
    if not isinstance(tags, dict) or len(tags) > 30 or any(
        not isinstance(k, str) or not isinstance(v, str) or len(k)>128 or len(v)>1024
        for k,v in tags.items()
    ):
        raise HTTPException(422, 'tags must be an object with up to 30 string key/value pairs')
    return tags


@router.post('/buckets/{bucket}/objects', status_code=201)
def upload(bucket: str, key: str = Form(...), file: UploadFile = File(...),
           chunking: Literal['fixed','fastcdc'] = Form('fixed'), tags: str = Form('{}')):
    """Multipart upload. File streams from the upload spool to metadata upstream."""
    if not key or len(key)>1024:
        raise HTTPException(422, 'Object key must contain 1..1024 characters')
    parsed_tags = parse_tags(tags)
    file.file.seek(0)
    result = response_json(upstream_request('POST', f'/task4/buckets/{bucket}/upload',
        data={'key': key, 'chunking': chunking},
        files={'file': (file.filename or 'object.bin', file.file, 'application/octet-stream')}))
    # Persist user-defined tags on the newly created object version.
    with db() as conn:
        v = version_record(conn, bucket, key, result['version'])
        conn.execute('''INSERT INTO t5_object_tags(version_id,tags) VALUES (%s,%s::jsonb)
              ON CONFLICT (version_id) DO UPDATE SET tags=EXCLUDED.tags''',
              (v['id'], json.dumps(parsed_tags)))
    return {**result, 'tags': parsed_tags}


@router.get('/buckets/{bucket}/objects')
def list_objects(bucket: str, prefix: str = '', limit: int = Query(100, ge=1, le=1000),
                 offset: int = Query(0, ge=0)):
    """Latest version per key, consistent with Task 4 listing semantics."""
    with db() as conn:
        exists = conn.execute('SELECT 1 FROM t4_buckets WHERE name=%s',(bucket,)).fetchone()
        if not exists:
            raise HTTPException(404, 'Bucket not found')
        results = conn.execute('''WITH latest AS (
             SELECT DISTINCT ON (object_key) id,object_key AS key,version,size,protection,created_at
             FROM t4_versions WHERE bucket=%s ORDER BY object_key,version DESC)
             SELECT latest.*,COALESCE(t.tags,'{}'::jsonb) AS tags FROM latest
             LEFT JOIN t5_object_tags t ON t.version_id=latest.id
             WHERE key LIKE %s ESCAPE '\\' ORDER BY key LIMIT %s OFFSET %s''',
             (bucket, prefix.replace('\\','\\\\').replace('%','\\%').replace('_','\\_')+'%', limit, offset)).fetchall()
    for item in results: item.pop('id', None)
    return {'objects': results, 'limit': limit, 'offset': offset}


@router.get('/buckets/{bucket}/objects/{key:path}/metadata')
def object_metadata(bucket: str, key: str, version: int | None = Query(None, ge=1)):
    result = response_json(upstream_request('GET',
          f'/task4/buckets/{bucket}/objects/{key}/metadata', params={'version': version} if version else None))
    with db() as conn:
        v = version_record(conn, bucket, key, version)
        row = conn.execute('SELECT tags FROM t5_object_tags WHERE version_id=%s', (v['id'],)).fetchone()
    result['tags'] = row['tags'] if row else {}
    result['created_at'] = v['created_at']
    return result


RANGE_RE = re.compile(r'^bytes=(\d*)-(\d*)$')


def parse_range(value: str, size: int) -> tuple[int,int]:
    match = RANGE_RE.fullmatch(value.strip())
    if not match or size == 0:
        raise HTTPException(416, 'Unsatisfiable byte range', headers={'Content-Range':f'bytes */{size}'})
    start_str, end_str = match.groups()
    if not start_str and not end_str:
        raise HTTPException(416, 'Invalid range', headers={'Content-Range':f'bytes */{size}'})
    if start_str:
        start = int(start_str)
        end = min(int(end_str), size - 1) if end_str else size - 1
    else:
        suffix = int(end_str)
        if suffix <= 0:
            raise HTTPException(416, 'Invalid suffix', headers={'Content-Range':f'bytes */{size}'})
        start = max(0, size - suffix)
        end = size - 1
    if start >= size or end < start:
        raise HTTPException(416, 'Unsatisfiable byte range', headers={'Content-Range':f'bytes */{size}'})
    return start,end


def download_to_temporary(bucket, key, version):
    """Stream upstream reconstructed object to disk (not the application heap)."""
    path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False) as stream:
            path = Path(stream.name)
            with httpx.Client(timeout=httpx.Timeout(180.0, connect=10.0)) as client:
                with client.stream('GET', UPSTREAM+f'/task4/buckets/{bucket}/objects/{key}/download',
                                   params={'version':version} if version else None) as upstream:
                    if upstream.status_code >= 400:
                        try: message = upstream.read().decode()[:500]
                        except Exception: message = 'Upstream download failed'
                        raise HTTPException(upstream.status_code, message)
                    for block in upstream.iter_bytes(1024*1024):
                        stream.write(block)
        return path
    except httpx.HTTPError as exc:
        if path: path.unlink(missing_ok=True)
        raise HTTPException(503, 'Metadata service unavailable') from exc
    except Exception:
        if path: path.unlink(missing_ok=True)
        raise


from fastapi.background import BackgroundTasks
from fastapi.responses import StreamingResponse


@router.get('/buckets/{bucket}/objects/{key:path}/download')
def download(bucket: str, key: str, background_tasks: BackgroundTasks, version: int | None = Query(None, ge=1),
             range_header: str | None = Header(None, alias='Range')):
    with db() as conn:
        v = version_record(conn, bucket, key, version)
    size = v['size']
    path = download_to_temporary(bucket, key, version)
    if path.stat().st_size != size:
        path.unlink(missing_ok=True)
        raise HTTPException(502, 'Upstream object size mismatch')
    headers = {'Accept-Ranges': 'bytes', 'X-Object-Version': str(v['version'])}
    if range_header:
        try: start,end = parse_range(range_header, size)
        except Exception:
            path.unlink(missing_ok=True)
            raise
        headers.update({'Content-Range': f'bytes {start}-{end}/{size}', 'Content-Length':str(end-start+1)})
        def limited_stream():
            try:
                with path.open('rb') as f:
                    f.seek(start)
                    remaining = end-start+1
                    while remaining:
                        block = f.read(min(1024*1024,remaining))
                        if not block: break
                        remaining -= len(block)
                        yield block
            finally:
                path.unlink(missing_ok=True)
        return StreamingResponse(limited_stream(), status_code=206, headers=headers,
                                 media_type='application/octet-stream')
    background_tasks.add_task(path.unlink, missing_ok=True)
    return FileResponse(path, media_type='application/octet-stream',
                        headers=headers, background=background_tasks)


@router.delete('/buckets/{bucket}/objects/{key:path}')
def delete_object(bucket: str, key: str, version: int | None = Query(None, ge=1)):
    return response_json(upstream_request('DELETE',
        f'/task4/buckets/{bucket}/objects/{key}', params={'version':version} if version else None))


@router.get('/search')
def search(bucket: str | None = None, prefix: str = '',
           min_size: int | None = Query(None, ge=0), max_size: int | None = Query(None, ge=0),
           created_from: date | None = None, created_to: date | None = None,
           tag_key: str | None = None, tag_value: str | None = None,
           limit: int = Query(100, ge=1, le=500), offset: int = Query(0,ge=0)):
    if (tag_key is None) != (tag_value is None):
        raise HTTPException(422, 'tag_key and tag_value must be provided together')
    if min_size is not None and max_size is not None and min_size>max_size:
        raise HTTPException(422, 'min_size cannot exceed max_size')
    cond = ['TRUE']; args = []
    if bucket: cond.append('v.bucket=%s'); args.append(bucket)
    if prefix:
        cond.append("v.object_key LIKE %s ESCAPE '\\'")
        args.append(prefix.replace('\\','\\\\').replace('%','\\%').replace('_','\\_')+'%')
    if min_size is not None: cond.append('v.size >= %s'); args.append(min_size)
    if max_size is not None: cond.append('v.size <= %s'); args.append(max_size)
    if created_from: cond.append('v.created_at >= %s'); args.append(datetime.combine(created_from,time.min,tzinfo=timezone.utc))
    if created_to: cond.append('v.created_at < %s'); args.append(datetime.combine(created_to,time.min,tzinfo=timezone.utc) + timedelta(days=1))
    if tag_key is not None:
        cond.append('COALESCE(t.tags,\'{}\'::jsonb) @> %s::jsonb')
        args.append(json.dumps({tag_key:tag_value}))
    sql = f'''SELECT v.bucket,v.object_key AS key,v.version,v.size,v.protection,v.created_at,
       COALESCE(t.tags,'{{}}'::jsonb) AS tags FROM t4_versions v
       LEFT JOIN t5_object_tags t ON t.version_id=v.id
       WHERE {' AND '.join(cond)} ORDER BY v.created_at DESC,v.id DESC LIMIT %s OFFSET %s'''
    args.extend([limit,offset])
    with db() as conn:
        rows = conn.execute(sql,args).fetchall()
    return {'results':rows,'limit':limit,'offset':offset}


@router.get('/cluster')
def cluster_status():
    result = response_json(upstream_request('GET','/task4/cluster'))
    nodes = result['nodes']
    # Compute persisted counts of objects with shards on missing/offline nodes.
    with db() as conn:
        degraded = conn.execute('''SELECT COUNT(DISTINCT v.id) AS value FROM t4_versions v
            JOIN t4_parts p ON p.version_id=v.id
            JOIN t4_shards s ON s.chunk_id=p.chunk_id
            LEFT JOIN storage_nodes n ON n.node_id=s.node_id
            WHERE n.node_id IS NULL OR n.last_seen < NOW() - INTERVAL '15 seconds' ''').fetchone()['value']
    result['degraded_objects_estimate'] = degraded
    result['total_free_space_bytes'] = sum(n['free_space'] for n in nodes if n['healthy'])
    result['total_chunks_reported'] = sum(n['chunk_count'] for n in nodes)
    return result


app.include_router(router)

# Task 6: Elasticsearch-backed API. Keep existing Task 5 routes intact.
from gateway.search_es import router as search_es_router
app.include_router(search_es_router)
