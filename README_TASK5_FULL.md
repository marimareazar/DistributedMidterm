# DistributedMidterm — TASK 5: S3-like Gateway

This package **extends your current Task 4 project**. Do not delete the `storage_node`, `metadata_service`, `repair_worker`, `shared`, `data`, or PostgreSQL volumes. The included `compose.yaml` is your full Task 4 configuration plus **one gateway service** on port **18200**. Task 4 metadata remains at port **18100**.

## Deliverables mapped to the midterm PDF

| Required feature | Route / implementation |
| --- | --- |
| FastAPI gateway | `gateway/main.py`, `gateway/Dockerfile` |
| Create/list buckets | `POST /api/buckets`, `GET /api/buckets` |
| Multipart upload | `POST /api/buckets/{bucket}/objects` |
| Full/byte-range download | `GET /api/buckets/{bucket}/objects/{key}/download`; `Range: bytes=0-9` |
| List/delete/metadata | `GET /api/buckets/{bucket}/objects`; `DELETE /api/buckets/{bucket}/objects/{key}`; `.../metadata` |
| Search prefix/size/date/tags | `GET /api/search` query filters |
| Cluster nodes/free space/chunks/degraded estimate | `GET /api/cluster` |
| API key authentication | `X-API-Key` on all `/api` routes |
| Rate limit | fixed 60-second window, default 120 requests/minute per client+key, HTTP 429 |
| Test | pytest unit/integration, PowerShell and curl, demo script, Postman collection |

**Design notes:** The gateway delegates redundancy, chunk placement, reconstruction, and versioning to Task 4 metadata; it stores user tags in a small PostgreSQL `t5_object_tags` table referencing existing `t4_versions`. Task 4 continues running unchanged. Range reads currently stream the *entire reconstructed object to a temporary disk file* and then serve the requested bytes; they are correct, but not an optimized server-side partial read. Rate limiting is in-memory per gateway process (adequate for one gateway; use Redis for multiple replicas). `degraded_objects_estimate` counts objects referenced by unavailable nodes, not corrupted shards awaiting scrubbing. S3-*like* does not mean wire-compatible with AWS S3 clients.

## 1) Copy files

Extract the ZIP; copy the **contents of its inner folder** into your existing `C:\Users\User\PycharmProjects\DistributedMidterm`, merging directories. The only Task 4 file being replaced is `compose.yaml`; `requirements.txt` is identical to Task 4's. **Do not run** `docker compose down -v`.

## 2) Configure key (Windows PowerShell 5.1)

From the project root with `.venv` activated:

```powershell
# Skip the next two lines if you already have a project .env file.
if (-not (Test-Path .env)) {
  "GATEWAY_API_KEY=$((python -c 'import secrets;print(secrets.token_urlsafe(32))').Trim())" | Set-Content .env
}
# Put your existing gateway key into the current PowerShell process for tests/curl.
$env:GATEWAY_API_KEY = ((Get-Content .env | Where-Object { $_ -match '^GATEWAY_API_KEY=' } | Select-Object -First 1) -split '=', 2)[1].Trim()
Write-Host "Gateway key loaded. Length: $($env:GATEWAY_API_KEY.Length)"
```

Keep the key private. `.env` is in `.gitignore` from Task 1.

## 3) Install, build, run

```powershell
python -m pip install -r requirements.txt
docker compose config --quiet
docker compose up -d --build gateway
docker compose ps
Invoke-RestMethod http://localhost:18200/health
```

Other services should stay up; `gateway` should listen on `127.0.0.1:18200`. The first gateway boot creates the `t5_object_tags` table in PostgreSQL. If it restarts, check `docker compose logs --tail=80 gateway`.

**Screenshot 30:** `docker compose ps` and `/health`, showing 10 running services (db, metadata, node1–6, repair-worker, gateway). **Caption:** We deployed a separate FastAPI gateway connected to the existing metadata database and six-node storage cluster.

## 4) Run all tests

```powershell
python -m pytest tests/test_gateway_unit.py -v
python -m pytest tests/test_gateway_integration.py -v
python -m pytest tests/test_redundancy.py tests/test_task4_integration.py -v
```

The two Task 5 integration cases test both replication and erasure buckets. Requires all six nodes healthy. Each integration case creates its own unique bucket name, uploads two versions, downloads full and byte ranges, searches by tags, and deletes the versions. Gateway API keys must match between Docker and PowerShell.

**Screenshot 31:** passing gateway unit/integration tests. **Caption:** We verified API key enforcement, range parsing, uploads, downloads, metadata, versioning, searching, and deletion through pytest.

## 5) Manual commands: bucket, upload, list, metadata, search, download, delete

Use **PowerShell 5.1** and your existing `data/task3_demo.bin` from Task 3.

```powershell
$gateway = 'http://localhost:18200/api'
$h = @{ 'X-API-Key' = $env:GATEWAY_API_KEY }

# Invalid requests return 401
try { Invoke-RestMethod "$gateway/buckets" -ErrorAction Stop } catch { "Unauthenticated: HTTP $([int]$_.Exception.Response.StatusCode)" }

# Create a new bucket (choose a new name if it exists)
Invoke-RestMethod -Method POST -Uri "$gateway/buckets" -Headers $h -ContentType 'application/json' -Body '{"name":"task5-demo","protection":"replication","replicas":3}'

# List
Invoke-RestMethod "$gateway/buckets" -Headers $h | ConvertTo-Json -Depth 6

# Upload with tags (curl.exe reads API key)
curl.exe -sS -X POST -H "X-API-Key: $env:GATEWAY_API_KEY" -F "key=reports/sample.bin" -F "chunking=fixed" -F 'tags={"subject":"midterm","owner":"student"}' -F "file=@data/task3_demo.bin" "$gateway/buckets/task5-demo/objects"

# List objects and metadata
Invoke-RestMethod "$gateway/buckets/task5-demo/objects?prefix=reports/" -Headers $h | ConvertTo-Json -Depth 5
Invoke-RestMethod "$gateway/buckets/task5-demo/objects/reports/sample.bin/metadata" -Headers $h | ConvertTo-Json -Depth 8

# Search: by bucket, prefix, size, and tag
Invoke-RestMethod "$gateway/search?bucket=task5-demo&prefix=reports/&min_size=100&tag_key=subject&tag_value=midterm" -Headers $h | ConvertTo-Json -Depth 6

# Full download and hash verification
curl.exe -sS -H "X-API-Key: $env:GATEWAY_API_KEY" -o data/task5_download.bin "$gateway/buckets/task5-demo/objects/reports/sample.bin/download"
Get-FileHash data/task3_demo.bin -Algorithm SHA256
Get-FileHash data/task5_download.bin -Algorithm SHA256

# Range request (HTTP 206; 20 bytes)
curl.exe -i -H "X-API-Key: $env:GATEWAY_API_KEY" -H "Range: bytes=0-19" "$gateway/buckets/task5-demo/objects/reports/sample.bin/download" --output NUL
# To display headers AND length safely in Windows PowerShell:
$r = Invoke-WebRequest "$gateway/buckets/task5-demo/objects/reports/sample.bin/download" -Headers @{ 'X-API-Key'=$env:GATEWAY_API_KEY; Range='bytes=0-19' } -UseBasicParsing
"HTTP $($r.StatusCode) Content-Range: $($r.Headers['Content-Range']) Content-Length: $($r.Headers['Content-Length'])"

# Cluster status
Invoke-RestMethod "$gateway/cluster" -Headers $h | ConvertTo-Json -Depth 6

# Delete latest version (or specify ?version=1)
Invoke-RestMethod -Method DELETE "$gateway/buckets/task5-demo/objects/reports/sample.bin" -Headers $h
```

For a **date search** use `created_from=2026-11-01&created_to=2026-11-30` (UTC inclusive dates).

**Screenshot 32:** create/list buckets, upload response with protection and version. **Caption:** The gateway provides S3-like bucket creation and multipart upload while reusing the existing distributed storage services.

**Screenshot 33:** list/metadata/search output with tags and filters. **Caption:** We implemented object listing and searching by prefix, size, date, and custom metadata tags stored in PostgreSQL.

**Screenshot 34:** matching SHA-256 values and range HTTP 206 with Content-Range header. **Caption:** The gateway supports complete file reconstruction and byte-range retrieval with verified data integrity.

**Screenshot 35:** cluster JSON displaying nodes, free space, chunk counts, maintenance, and degraded-object estimate. **Caption:** We added an API for monitoring six storage nodes and recent repair activity.

## 6) Rate limiting test (isolated)

To demonstrate HTTP 429 without flooding your normal gateway, temporarily change the environment variable in your `.env` to `RATE_LIMIT_PER_MINUTE=5`, then run:

```powershell
docker compose up -d --force-recreate gateway
1..7 | ForEach-Object {
  $status = & curl.exe -s -o NUL -w '%{http_code}' -H "X-API-Key: $env:GATEWAY_API_KEY" 'http://localhost:18200/api/buckets'
  Write-Host "Request $_ => HTTP $status"
}
```

Expected: the first five requests are HTTP 200, then 429. Afterwards remove the `RATE_LIMIT_PER_MINUTE=5` line from `.env` (or change to `120`) and run `docker compose up -d --force-recreate gateway` again. **Screenshot 36:** requests ending in 429. **Caption:** We enforced a per-client request limit and verified that excess requests are rejected with HTTP 429 Too Many Requests.

## 7) One-command API demo and report result

```powershell
python -m benchmarks.task5_api_demo
```

Outputs `reports/task5_api_demo.json`. **Screenshot 37:** compact results showing unauthenticated HTTP 401, file hash match, range HTTP 206, search count, six healthy nodes, deletion. **Caption:** We performed an end-to-end gateway demonstration and saved structured results for the final project report.

## 8) Git checkpoint

```powershell
git add .
git commit -m 'Implement Task 5 S3-like gateway with authentication and range downloads'
git status
```

## Files to submit

Include the source code (`gateway/`, previous modules), `compose.yaml`, all Dockerfiles, `requirements.txt`, automated tests, benchmark scripts and outputs from `reports/`, plus screenshots. Keep your `.env` (secret key) out of Git or submitted archives; submit `.env.example` instead.

## Troubleshooting

- HTTP 401: ensure PowerShell `$env:GATEWAY_API_KEY` matches the `.env` value used by Docker Compose.
- HTTP 503: check `docker compose ps` and `docker compose logs --tail=100 metadata gateway`.
- Gateway restarting: check database connectivity; metadata and PostgreSQL must be healthy first.
- HTTP 429 during tests: return the rate limit to at least 120 and recreate the gateway; the test suite makes multiple requests.
- Large file downloads: gateway buffers reconstructed data in the container's temporary directory. For production, optimize range reads in metadata/storage nodes and use object-stream slicing.
