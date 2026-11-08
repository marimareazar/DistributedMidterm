import hashlib
import os
import time

import httpx

BASE = os.getenv("STORAGE_TEST_BASE", "http://localhost")
NODE_PORTS = (18001, 18002, 18003, 18004)
METADATA_URL = os.getenv("METADATA_TEST_URL", "http://localhost:18100")


def test_each_node_supports_chunk_crud():
    with httpx.Client(timeout=15) as client:
        for index, port in enumerate(NODE_PORTS, start=1):
            url = f"{BASE}:{port}"
            payload = f"distributed midterm test node {index}".encode()
            digest = hashlib.sha256(payload).hexdigest()
            chunk_url = f"{url}/chunks/{digest}"
            assert client.get(f"{url}/health").json()["node_id"] == f"node-{index}"
            result = client.put(chunk_url, content=payload)
            assert result.status_code == 201, result.text
            assert result.json()["hash"] == digest
            assert client.head(chunk_url).status_code == 200
            downloaded = client.get(chunk_url)
            assert downloaded.status_code == 200
            assert downloaded.content == payload
            assert client.put(chunk_url, content=b"wrong hash").status_code == 422
            assert client.delete(chunk_url).status_code == 200
            assert client.get(chunk_url).status_code == 404


def test_heartbeats():
    with httpx.Client(timeout=10) as client:
        for _ in range(12):
            response = client.get(f"{METADATA_URL}/nodes")
            response.raise_for_status()
            nodes = response.json()["nodes"]
            if {entry["node_id"] for entry in nodes} >= {f"node-{i}" for i in range(1, 5)}:
                return
            time.sleep(1)
        raise AssertionError(f"Expected four node heartbeats, received: {nodes}")
