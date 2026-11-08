"""Temporary heartbeat registry for Task 2; PostgreSQL metadata arrives in Task 3."""
from datetime import datetime, timezone
from threading import Lock

from fastapi import FastAPI
from pydantic import BaseModel, Field

app = FastAPI(title="Task 2 Heartbeat Registry")
registry: dict[str, dict] = {}
lock = Lock()


class Heartbeat(BaseModel):
    node_id: str = Field(min_length=1)
    free_space: int = Field(ge=0)
    chunk_count: int = Field(ge=0)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/heartbeat")
def heartbeat(payload: Heartbeat):
    with lock:
        registry[payload.node_id] = {**payload.model_dump(), "last_seen": datetime.now(timezone.utc).isoformat()}
    return {"received": True, "node_id": payload.node_id}


@app.get("/nodes")
def nodes():
    with lock:
        return {"nodes": sorted(registry.values(), key=lambda value: value["node_id"])}
