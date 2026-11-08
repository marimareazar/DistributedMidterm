# Task 6 — Complete Web UI and Elasticsearch Search

This package **extends the successful Task 5**. Do not remove the old Python files, volumes or `.env`.

## Added and changed files

- `frontend/` complete React/Vite dashboard, built inside Docker
- `gateway/main.py` (updated Task 5 file to register Task 6 search router)
- `gateway/search_es.py` (Elasticsearch-backed authenticated search)
- `gateway/indexer.py` and `gateway/indexer.Dockerfile` (PostgreSQL-to-Elasticsearch synchronization)
- `compose.yaml` (adds Elasticsearch, indexer and React/Nginx frontend)
- `tests/test_task6_search.py` and `benchmarks/task6_search_demo.py`

## 1. Copy

Copy the contents of this folder into `C:\Users\User\PycharmProjects\DistributedMidterm`, merge directories and replace `compose.yaml` and `gateway/main.py`. Retain your `storage_node`, `metadata_service`, `repair_worker`, `shared`, `requirements.txt`, `.env` and Docker volumes. No new host Python dependencies are required.

## 2. Deploy (PowerShell in project root)

```powershell
docker compose config --quiet
docker compose up -d --build gateway elasticsearch search-indexer frontend
docker compose ps
```

If Elasticsearch fails to start, increase Docker Desktop memory to at least 4 GB. **Do not use `docker compose down -v`**.

## 3. Health checks

```powershell
Invoke-RestMethod http://localhost:18200/health
Invoke-RestMethod http://localhost:19200/_cluster/health
$env:GATEWAY_API_KEY = ((Get-Content .env | Where-Object {$_ -match '^GATEWAY_API_KEY='} | Select-Object -First 1) -split '=',2)[1].Trim()
$headers = @{ 'X-API-Key' = $env:GATEWAY_API_KEY }
Invoke-RestMethod http://localhost:18200/api/search/status -Headers $headers
Invoke-RestMethod -Method Post http://localhost:18200/api/search/reindex -Headers $headers
```

Open **http://localhost:18300**. Paste your `GATEWAY_API_KEY` from `.env` into the login screen. Do not paste it into screenshots. Key is browser session-only and this is a local demo; production would require real users and stronger access controls.

## 4. Features to demonstrate

1. Overview: node health, free disk, chunk counts, degraded object estimate, last maintenance.
2. Object browser: create replication or 4+2 erasure bucket, choose a file, upload with key + tags, list, inspect metadata, download and delete.
3. Elasticsearch tab: search by name or text, bucket, key prefix, size, date, and tag filters. Index auto-refreshes every ~15 s, or press `Reindex now`.
4. Cluster monitor: all six node cards with heartbeat and chunk count, plus last maintenance statistics.

**Elasticsearch index** is independent of PostgreSQL and stores only object metadata (not file bytes). The `search-indexer` regularly reconciles the index with the authoritative PostgreSQL records, including deletions.

## 5. Automated tests

```powershell
python -m pytest tests/test_task6_search.py -v
python -m pytest tests/test_gateway_unit.py tests/test_gateway_integration.py -v
python -m benchmarks.task6_search_demo
```

### Screenshot and report captions

**Figure 38 — Task 6 services deployed.** Capture `docker compose ps` showing `frontend`, `elasticsearch`, `search-indexer`, `gateway`, database, six storage nodes, and repair worker. *We deployed a React frontend and an Elasticsearch-backed search service without changing the existing distributed storage architecture.*

**Figure 39 — React overview dashboard.** Capture overview tiles and chunk distribution bars. *We built a web-based monitoring dashboard showing healthy nodes, free space, chunk distribution and recent repair activity.*

**Figure 40 — Upload and browse objects.** Create a bucket and upload a file, then show it in the browser. *The interface communicates with the FastAPI gateway to manage buckets and upload, list, download, and delete objects.*

**Figure 41 — Elasticsearch object search.** Search `term-paper`, filter by bucket and tag after running the demo. *We synchronized PostgreSQL object metadata into Elasticsearch and enabled full-text and attribute-based search in the UI.*

**Figure 42 — Cluster monitor.** Show six health cards and recent repair statistics. *The dashboard displays per-node health, chunk counts, available storage, and recent repair results.*

**Figure 43 — Backend search tests.** Capture `pytest` and `task6_search_demo` output. *We validated authenticated Elasticsearch searching and verified that newly uploaded objects are indexed and can be found by metadata filters.*

**Figure 44 — Automatic Elasticsearch update.** Delete a test object, press `Reindex now`, then search again to verify that the deleted version disappears from results. *The index synchronization removes entries that no longer exist in PostgreSQL.*

## Note about report precision

- `last_maintenance` is a periodic **snapshot**, not a live repair job progress percentage.
- `degraded_objects_estimate` is based on offline shard locations and can overestimate actual irrecoverable objects.
- Search is eventually consistent: object uploads can take up to 15 seconds to appear without manual reindexing.
- This UI is for local lab testing; browser session API key access is not production-grade authentication.
