# Task 3 — Complete Metadata and Placement Module

This is an **overlay** for your existing `DistributedMidterm` project. Copy the directories/files from this ZIP over the existing directories. **Do not delete `storage_node/`, existing `tests/test_storage_nodes.py`, or your Docker volumes.**

## Implements every item of Task 3 in the midterm

1. Fixed 4 MB chunking and educational FastCDC-style content-defined chunking (`shared/chunking.py`).
2. PostgreSQL bucket, object/version, ordered chunk manifest, and chunk catalog (`metadata_service/main.py`).
3. Consistent hashing ring with 128 virtual nodes per storage node (`shared/placement.py`).
4. Deduplication with persistent reference counts and deletion only at zero references (`metadata_service/objects.py`).
5. Deterministic node add/remove movement benchmark, showing percentages (`benchmarks/placement_comparison.py`).

Integration is provided through `POST /task3/buckets/{bucket}/upload` and versioned GET/DELETE APIs. The Task 3 service places one physical copy of each distinct chunk. **Replication and actual rebalancing/repair are later tasks; the node membership benchmark is a placement simulation.**

## Install / start (PyCharm PowerShell, inside DistributedMidterm)

```powershell
python -m pip install -r requirements.txt
docker compose config --quiet
docker compose up -d --build
docker compose ps
Invoke-RestMethod http://localhost:18100/health
```

Do NOT run `docker compose down -v` (that destroys stored data).

## Automated tests

```powershell
python -m pytest tests/test_chunking.py tests/test_placement.py -v
python -m pytest tests/test_storage_nodes.py -v
python -m pytest tests/test_task3_integration.py -v
```

Integration test creates a uniquely named bucket, uploads an 8 MB+ file twice, verifies two object versions, proves physical chunk reuse, reconstructs the file, checks reference counts, and deletes each version. It requires the Docker services to be running.

## Manual functional demonstration

```powershell
# Make a file containing two identical 4 MB chunks and a short final chunk
python -m benchmarks.make_demo_file

# Create a bucket
Invoke-RestMethod -Method POST -Uri http://localhost:18100/task3/buckets -ContentType 'application/json' -Body '{"name":"midterm-demo"}'

# Upload: Windows PowerShell 5.1 compatible curl.exe (not the curl alias)
curl.exe -sS -X POST -F "key=example/task3_demo.bin" -F "mode=fixed" -F "file=@data/task3_demo.bin" http://localhost:18100/task3/buckets/midterm-demo/upload

# Repeat exactly the same command: should produce version 2, new_unique_chunks = 0
curl.exe -sS -X POST -F "key=example/task3_demo.bin" -F "mode=fixed" -F "file=@data/task3_demo.bin" http://localhost:18100/task3/buckets/midterm-demo/upload

# Read latest object metadata, unique chunks and reference counts
Invoke-RestMethod 'http://localhost:18100/task3/buckets/midterm-demo/objects/example/task3_demo.bin/metadata' | ConvertTo-Json -Depth 8
Invoke-RestMethod 'http://localhost:18100/task3/chunks' | ConvertTo-Json -Depth 7
Invoke-RestMethod 'http://localhost:18100/task3/stats' | ConvertTo-Json

# Download and verify exact SHA-256 match to original
curl.exe -sS -o data/task3_download.bin http://localhost:18100/task3/buckets/midterm-demo/objects/example/task3_demo.bin/download
Get-FileHash data/task3_demo.bin -Algorithm SHA256
Get-FileHash data/task3_download.bin -Algorithm SHA256

# Inspect persisted SQL rows
# The database credentials/name match your existing Task 3 Step 1 setup
docker compose exec -T db psql -U storage -d distributed_storage -c 'SELECT bucket,object_key,version,size,mode FROM object_versions ORDER BY id DESC LIMIT 10;'
docker compose exec -T db psql -U storage -d distributed_storage -c 'SELECT LEFT(sha256,16) AS hash_prefix,node_id,ref_count,size FROM chunks ORDER BY sha256 LIMIT 20;'

# Remove first version only, preserving shared chunks
Invoke-RestMethod -Method DELETE 'http://localhost:18100/task3/buckets/midterm-demo/objects/example/task3_demo.bin?version=1'
Invoke-RestMethod 'http://localhost:18100/task3/stats' | ConvertTo-Json

# Remove final version: physical chunks can now be removed
Invoke-RestMethod -Method DELETE 'http://localhost:18100/task3/buckets/midterm-demo/objects/example/task3_demo.bin?version=2'
Invoke-RestMethod 'http://localhost:18100/task3/stats' | ConvertTo-Json
```


## Placement benchmark

```powershell
python -m benchmarks.placement_comparison
```

Result saved in `reports/placement_comparison.json`. The benchmark runs 10,000 deterministic SHA-256 identifiers, computes the fraction reassigned after adding node-5 and after removing node-4, and also records node-by-node distributions. It does not physically start node-5; this is the ring movement measurement requested in Task 3.

## Screenshots for the final report

- Figure 14: six chunking tests passing (already captured).
- Figure 15: 40% versus 80% reuse in the chunking comparison (already captured).
- Figure 16: Docker services + PostgreSQL object/version SQL output.
  *Caption*: We persisted buckets, versioned object metadata and ordered chunk identifiers in PostgreSQL.
- Figure 17: Upload response version 1 then version 2, showing `new_unique_chunks` decreasing to zero.
  *Caption*: We uploaded identical data twice and used SHA-256 content addressing to reuse existing physical chunks.
- Figure 18: SQL `chunks` table with node assignment and `ref_count`, plus deletion of one version.
  *Caption*: Reference counting preserved shared chunks while objects referenced them; unreferenced chunks were eligible for deletion.
- Figure 19: matching original/download SHA-256 hashes.
  *Caption*: We reconstructed the uploaded object from its ordered chunk list and verified byte-for-byte integrity.
- Figure 20: placement benchmark add/remove results with moved percentages.
  *Caption*: The consistent hashing ring with virtual nodes moved only a fraction of chunk assignments when membership changed.
- Figure 21: integration pytest results.
  *Caption*: Automated integration tests verified versioning, deduplication, reconstruction, reference counts and deletion.

## Architecture note

The metadata service is an interim direct upload/download API for Task 3; Task 5 introduces a dedicated API gateway. Task 4 adds replication, Reed–Solomon erasure coding, node-failure detection, repair and scrubbing.

## Development notes

- The integrated upload processes one chunk at a time after spooling the incoming file to disk. FastCDC is an educational Gear-hash implementation.
- During an interrupted/failed upload, a physical chunk may exist before PostgreSQL commits metadata, leaving an orphan chunk. Later garbage collection can reclaim such orphan chunks.
- Object DELETE may leave a `ref_count=0` catalog row if a physical node is unreachable, so cleanup can be retried.
- Do not change the ring node list while it contains stored data without implementing actual rebalancing first.
