import hashlib
import os
import shutil
import tempfile
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Annotated

import asyncio
import httpx
from fastapi import FastAPI, HTTPException, Path as ApiPath, Request, Response
from fastapi.responses import StreamingResponse

NODE_ID = os.getenv("NODE_ID", "node-1")
STORAGE_DIR = Path(os.getenv("STORAGE_DIR", "/data"))
METADATA_URL = os.getenv("METADATA_URL", "http://metadata:8000").rstrip("/")
HEARTBEAT_INTERVAL = float(os.getenv("HEARTBEAT_INTERVAL", "5"))
BUFFER_SIZE = 1024 * 1024


def chunk_path(sha256: str) -> Path:
    return STORAGE_DIR / sha256[:2] / sha256


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(BUFFER_SIZE), b""):
            digest.update(block)
    return digest.hexdigest()


def node_stats() -> dict:
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    disk = shutil.disk_usage(STORAGE_DIR)
    count = sum(1 for p in STORAGE_DIR.glob("[0-9a-f][0-9a-f]/*") if p.is_file())
    return {"node_id": NODE_ID, "free_space": disk.free, "chunk_count": count}


async def send_heartbeats() -> None:
    async with httpx.AsyncClient(timeout=3) as client:
        while True:
            try:
                await client.post(f"{METADATA_URL}/heartbeat", json=node_stats())
            except (httpx.HTTPError, OSError) as exc:
                print(f"[{NODE_ID}] heartbeat unavailable: {exc}", flush=True)
            await asyncio.sleep(HEARTBEAT_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    task = asyncio.create_task(send_heartbeats())
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


app = FastAPI(title=f"Storage Node {NODE_ID}", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok", **node_stats()}


@app.put("/chunks/{sha256}", status_code=201)
async def put_chunk(request: Request, sha256: Annotated[str, ApiPath(pattern=r"^[a-f0-9]{64}$")]):
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(mode="wb", dir=STORAGE_DIR, delete=False) as tmp:
            temp_path = Path(tmp.name)
            async for data in request.stream():
                digest.update(data)
                tmp.write(data)
        actual_hash = digest.hexdigest()
        if actual_hash != sha256:
            raise HTTPException(status_code=422, detail="SHA-256 mismatch: uploaded content does not match chunk ID")
        destination = chunk_path(sha256)
        destination.parent.mkdir(parents=True, exist_ok=True)
        # Repair existing corrupted content rather than assuming a matching filename is valid.
        if destination.exists() and sha256_file(destination) == sha256:
            return {"node_id": NODE_ID, "hash": sha256, "size": destination.stat().st_size, "stored": False}
        os.replace(temp_path, destination)
        temp_path = None
        return {"node_id": NODE_ID, "hash": sha256, "size": destination.stat().st_size, "stored": True}
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)


def verified_chunk(sha256: str) -> Path:
    path = chunk_path(sha256)
    if not path.is_file():
        raise HTTPException(status_code=404, detail="Chunk not found")
    if sha256_file(path) != sha256:
        raise HTTPException(status_code=409, detail="Chunk corrupted: SHA-256 verification failed")
    return path


@app.get("/chunks/{sha256}")
def get_chunk(sha256: Annotated[str, ApiPath(pattern=r"^[a-f0-9]{64}$")]):
    path = verified_chunk(sha256)

    def generate():
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(BUFFER_SIZE), b""):
                yield block

    return StreamingResponse(generate(), media_type="application/octet-stream", headers={"X-Chunk-SHA256": sha256})


@app.head("/chunks/{sha256}")
def head_chunk(sha256: Annotated[str, ApiPath(pattern=r"^[a-f0-9]{64}$")]):
    path = verified_chunk(sha256)
    return Response(status_code=200, headers={"Content-Length": str(path.stat().st_size), "X-Chunk-SHA256": sha256})


@app.delete("/chunks/{sha256}")
def delete_chunk(sha256: Annotated[str, ApiPath(pattern=r"^[a-f0-9]{64}$")]):
    path = chunk_path(sha256)
    if not path.is_file():
        raise HTTPException(status_code=404, detail="Chunk not found")
    path.unlink()
    return {"node_id": NODE_ID, "hash": sha256, "deleted": True}
