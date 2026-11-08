# DistributedMidterm — Task 4: Redundancy and Recovery

This ZIP is **one complete Task 4 drop-in package**, retaining the compatible code from Tasks 2–3. Extract the files **into your existing** `C:\Users\User\PycharmProjects\DistributedMidterm` directory, merge directories and replace matching source/config files. Do **not** delete the original project, database, or Docker volumes. Existing Task 3 APIs remain `/task3`; Task 4 uses `/task4` and its own database catalog.

## Implemented

- 6 distinct FastAPI node containers; existing 4 nodes and PostgreSQL remain, with two new named volumes.
- Replication per bucket: configurable `replicas` in 1–6; default `N=3`, majority `W=2`, `R=2` (so W + R > N). Each successful write sends distinct copies to live storage nodes. A write without quorum fails; a strict read requires at least R verified copies.
- Erasure 4+2 selectable per bucket using the **zfec** Reed–Solomon encoder/decoder; six distinct shard locations when the cluster is healthy; any four valid shards recover the original chunk. The SHA-256 of every physical shard is verified by the storage node, and the reconstructed original chunk is checked against its original SHA-256.
- PostgreSQL persists object versions, ordered chunks, redundancy mode, shard locations, reference counts and maintenance history. Existing Task 3 data/tables are not dropped.
- Failure detection: after 15 seconds without a heartbeat, node is marked unhealthy. A dedicated `repair-worker` triggers maintenance every 10 seconds; checks broken/missing shards and restores them when enough healthy source shards and target nodes exist.
- Background scrub: each maintenance cycle re-reads up to 30 physical chunk groups, verifies hashes and repairs damaged copies/shards. Manual maintenance endpoint can run 200 groups at once for demonstrations.
- Two workload benchmark generators and report JSON. The analytical benchmark models storage overhead and one-shard failure repair traffic; the HTTP workload benchmark uploads real files.

**Important design limitations:** This is coursework, not production storage. Object uploads hold a PostgreSQL transaction while writing chunks; if a transaction fails after physical PUTs, orphan chunks may need garbage collection in Task 7. Task 4 retrieval reconstructs first into a temporary disk file, so it remains memory-bounded; however the repair process itself loads one chunk group at a time. The six-node configuration can tolerate two erased shards for *reading*, but it cannot restore six **distinct** physical erasure shards while fewer than six nodes are online: full redundancy is restored after recovery or addition of a spare node. Heartbeat detection and repair-history counters are available from `/task4/cluster`.

## Deploy — PowerShell commands (run in DistributedMidterm)

```powershell
python -m pip install -r requirements.txt
docker compose config --quiet
docker compose up -d --build
docker compose ps
```

**Never use `docker compose down -v`**, as that deletes named volumes. Before running erasure integration tests, check six healthy node heartbeats:

```powershell
(Invoke-RestMethod http://localhost:18100/task4/cluster).nodes | Format-Table node_id,healthy,chunk_count,last_seen
docker compose logs --tail=20 repair-worker
```

If `metadata` is restarting, get its errors using `docker compose logs --tail=80 metadata`. If needed, run `docker compose up -d --build metadata node5 node6 repair-worker` again. `zfec` 1.6.0.0 offers Windows and Linux Python 3.11 wheels, so the Python and Docker environments should be able to install it from PyPI when internet is available.

## Tests

```powershell
python -m pytest tests/test_redundancy.py -v
python -m pytest tests/test_task4_integration.py -v
python -m pytest tests/test_storage_nodes.py tests/test_chunking.py tests/test_placement.py tests/test_task3_integration.py -v
```

Integration tests create temporary bucket names and delete uploaded versions. They require all six nodes online. If installing zfec locally fails, run the isolated unit tests using the Docker metadata image and mounted source:

```powershell
docker compose run --rm -v "${PWD}:/app" metadata python -m pytest tests/test_redundancy.py -v
```

## Manual replication demo

```powershell
Invoke-RestMethod -Method POST -Uri 'http://localhost:18100/task4/buckets' -ContentType 'application/json' -Body '{"name":"rep-demo","protection":"replication","replicas":3}'
python -m benchmarks.make_demo_file
curl.exe -sS -X POST -F 'key=rep/test.bin' -F 'chunking=fixed' -F 'file=@data/task3_demo.bin' http://localhost:18100/task4/buckets/rep-demo/upload
Invoke-RestMethod 'http://localhost:18100/task4/buckets/rep-demo/objects/rep/test.bin/metadata' | ConvertTo-Json -Depth 7
curl.exe -sS -o data/rep_download.bin http://localhost:18100/task4/buckets/rep-demo/objects/rep/test.bin/download
Get-FileHash data/task3_demo.bin -Algorithm SHA256
Get-FileHash data/rep_download.bin -Algorithm SHA256
```

The metadata should show three shard entries per unique chunk, on separate nodes. Both file hashes must match. Bucket creation returns HTTP 409 on reruns if `rep-demo` already exists: choose a new name then.

## Manual erasure 4+2 demo

```powershell
Invoke-RestMethod -Method POST -Uri 'http://localhost:18100/task4/buckets' -ContentType 'application/json' -Body '{"name":"ec-demo","protection":"erasure"}'
curl.exe -sS -X POST -F 'key=ec/test.bin' -F 'chunking=fixed' -F 'file=@data/task3_demo.bin' http://localhost:18100/task4/buckets/ec-demo/upload
Invoke-RestMethod 'http://localhost:18100/task4/buckets/ec-demo/objects/ec/test.bin/metadata' | ConvertTo-Json -Depth 7
curl.exe -sS -o data/ec_download.bin http://localhost:18100/task4/buckets/ec-demo/objects/ec/test.bin/download
Get-FileHash data/task3_demo.bin -Algorithm SHA256
Get-FileHash data/ec_download.bin -Algorithm SHA256
```

The metadata should show **six different nodes**, one shard per index 0–5, for each unique chunk; original and download hashes must match.

## Failure and corruption demonstration (automated host-side commands)

```powershell
python -m benchmarks.failure_demo --mode replication
python -m benchmarks.failure_demo --mode erasure
```

Each script creates a distinct bucket, uploads data, temporarily stops the node holding a shard, waits for the heartbeat to mark it failed, downloads the object, runs repair, restarts the node, **corrupts a physical chunk using Docker exec**, verifies that the storage node rejects the corrupted chunk with HTTP 409, initiates scrub-and-repair, and finally verifies the reconstructed content. It pauses the automatic worker during its controlled demonstration and restarts the worker even if the demo fails. **Do not stop or corrupt the PostgreSQL container.**

For evidence of truly automatic periodic repair, leave `repair-worker` running and use:

```powershell
docker compose logs --tail=40 repair-worker
Invoke-RestMethod http://localhost:18100/task4/cluster | ConvertTo-Json -Depth 6
```

## Workloads and comparisons

**Quick real HTTP smoke benchmarks** (fast enough to validate the pipeline):

```powershell
python -m benchmarks.workload_test --quick --mode replication
python -m benchmarks.workload_test --quick --mode erasure
```

**Full workload examples required by the assignment** (significant storage and runtime):

```powershell
python -m benchmarks.workload_test --mode replication
python -m benchmarks.workload_test --mode erasure
```

Full mode tests **5,000 files sized 1–50 KiB and 10 files of 100 MiB each, per mode**. Generates over 1 GiB of logical data per mode and takes substantial disk space; do not run until sufficient free space is available. The output includes measured upload throughput and goes to `reports/task4_workload_*.json`. Benchmark buckets persist: remove them or their objects when disk space is needed.

**Analytical comparison** (storage overhead and one lost copy/shard repair traffic, from exact generated file sizes; not measured network traffic):

```powershell
python -m benchmarks.redundancy_comparison
```

Saved to `reports/task4_overhead_comparison.json`. Note that traffic estimates model *only reconstruction reads + repair writes* and exclude background scrub traffic. For full experimental reporting, show both measured upload results and these analytical estimates, and clearly distinguish them.

## Report screenshots with short captions

1. **Figure 22: Six storage containers + worker.** Screenshot `docker compose ps` and `docker compose config --volumes`. Caption: "We extended the cluster from four to six independent storage nodes, preserving PostgreSQL and the original storage volumes, and deployed a background repair worker."
2. **Figure 23: Replication N=3 with W=2/R=2.** Screenshot replication bucket creation/upload and `/metadata` (three distinct shard locations). Caption: "We stored three verified copies of each chunk and used majority read/write quorums. A write requires two successful copies and a read checks two valid copies."
3. **Figure 24: Erasure coding 4+2.** Screenshot erasure upload and metadata (six shard indices on six nodes). Caption: "We divided each chunk into four data shards and generated two Reed–Solomon parity shards, enabling recovery using any four valid shards."
4. **Figure 25: Redundancy unit and integration tests.** Screenshot both pytest commands passing. Caption: "We validated quorum calculations, reconstruction from missing shards, and successful upload/download with both protection modes."
5. **Figure 26: Failure detection and repair.** Screenshot `failure_demo` output showing a stopped node, object retrieval and repair counts; optionally include `/task4/cluster`. Caption: "The system detected a node outage through missing heartbeats and used healthy shards to preserve object access and initiate recovery."
6. **Figure 27: Scrubber/bit rot recovery.** Screenshot `failure_demo` output showing `409`, repaired shard count and restored object. Caption: "We deliberately corrupted a stored shard; SHA-256 verification rejected the damaged bytes and the repair process rebuilt a healthy copy."
7. **Figure 28: Overhead and repair-traffic comparison.** Screenshot `python -m benchmarks.redundancy_comparison` output and the saved JSON. Caption: "We compared the theoretical storage amplification and repair transfer of triple replication and 4+2 erasure coding for many small files and a few large files."
8. **Figure 29: Two workload throughput benchmarks.** Screenshot `python -m benchmarks.workload_test ...` results. Caption: "We uploaded many small files and a few large files, recording throughput and new unique chunks for both protection modes."
9. **Figure 30: Automatic repair worker.** Screenshot `docker compose logs --tail=40 repair-worker`. Caption: "A background service periodically scans registered chunks for inaccessible or corrupted shards and performs repairs when enough healthy copies are available."

## What to submit

Include all project source files, updated `compose.yaml`, Dockerfiles, test scripts, `reports/*.json`, benchmark scripts, and full-screen screenshots. The full midterm report still needs **write/read/failure-and-repair flowcharts** (prepare these using the integrated design after all phases), plus UI/API tasks that follow. Do not imply that the analytical estimates came from measured network packet traces.
