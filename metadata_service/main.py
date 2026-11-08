"""Task 3 PostgreSQL catalog + existing node heartbeat API."""
import os
from contextlib import asynccontextmanager
import psycopg
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://storage:storagepass@localhost:15433/distributed_storage')


class Heartbeat(BaseModel):
    node_id: str = Field(min_length=1)
    free_space: int = Field(ge=0)
    chunk_count: int = Field(ge=0)


def initialize_database() -> None:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute('''CREATE TABLE IF NOT EXISTS storage_nodes (
                node_id TEXT PRIMARY KEY, free_space BIGINT NOT NULL CHECK (free_space >= 0),
                chunk_count BIGINT NOT NULL CHECK (chunk_count >= 0),
                last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW())''')
            cur.execute('''CREATE TABLE IF NOT EXISTS buckets (
                name TEXT PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())''')
            cur.execute('''CREATE TABLE IF NOT EXISTS object_counters (
                bucket TEXT NOT NULL REFERENCES buckets(name), object_key TEXT NOT NULL,
                latest_version INTEGER NOT NULL, PRIMARY KEY(bucket, object_key))''')
            cur.execute('''CREATE TABLE IF NOT EXISTS object_versions (
                id BIGSERIAL PRIMARY KEY, bucket TEXT NOT NULL REFERENCES buckets(name),
                object_key TEXT NOT NULL, version INTEGER NOT NULL CHECK (version > 0),
                size BIGINT NOT NULL CHECK (size >= 0),
                mode TEXT NOT NULL CHECK (mode IN ('fixed','fastcdc')),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(bucket, object_key, version))''')
            cur.execute('''CREATE TABLE IF NOT EXISTS chunks (
                sha256 CHAR(64) PRIMARY KEY, size BIGINT NOT NULL CHECK (size > 0),
                node_id TEXT NOT NULL, ref_count BIGINT NOT NULL CHECK (ref_count >= 0))''')
            cur.execute('''CREATE TABLE IF NOT EXISTS object_chunks (
                object_version_id BIGINT NOT NULL REFERENCES object_versions(id) ON DELETE CASCADE,
                position INTEGER NOT NULL CHECK (position >= 0),
                sha256 CHAR(64) NOT NULL REFERENCES chunks(sha256),
                size BIGINT NOT NULL CHECK (size > 0),
                PRIMARY KEY(object_version_id, position))''')
            cur.execute('CREATE INDEX IF NOT EXISTS ix_versions_lookup ON object_versions(bucket, object_key, version DESC)')
            cur.execute('CREATE INDEX IF NOT EXISTS ix_object_chunks_hash ON object_chunks(sha256)')


@asynccontextmanager
async def lifespan(app: FastAPI):
    initialize_database()
    from metadata_service.redundancy import initialize as initialize_task4
    initialize_task4()
    yield


app = FastAPI(title='DistributedMidterm Metadata and Placement', lifespan=lifespan)


@app.get('/health')
def health():
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            conn.execute('SELECT 1')
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail='Database unavailable') from exc
    return {'status': 'ok', 'database': 'connected'}


@app.post('/heartbeat')
def heartbeat(payload: Heartbeat):
    with psycopg.connect(DATABASE_URL) as conn:
        conn.execute('''INSERT INTO storage_nodes(node_id, free_space, chunk_count, last_seen)
            VALUES (%s,%s,%s,NOW()) ON CONFLICT (node_id) DO UPDATE SET
            free_space=EXCLUDED.free_space, chunk_count=EXCLUDED.chunk_count, last_seen=NOW()''',
            (payload.node_id, payload.free_space, payload.chunk_count))
    return {'received': True, 'node_id': payload.node_id}


@app.get('/nodes')
def nodes():
    with psycopg.connect(DATABASE_URL, row_factory=psycopg.rows.dict_row) as conn:
        rows = conn.execute('SELECT node_id, free_space, chunk_count, last_seen FROM storage_nodes ORDER BY node_id').fetchall()
    return {'nodes': rows}


from metadata_service.objects import router as object_router  # noqa: E402
app.include_router(object_router)

from metadata_service.redundancy import router as task4_router
app.include_router(task4_router)


from metadata_service.task7 import router as bonus_router
app.include_router(bonus_router)
