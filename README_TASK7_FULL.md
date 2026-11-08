# DistributedMidterm — TASK 7 BONUS: Load balancing and fault tolerance

Task 7 in the assignment specifies: (1) Nginx or HAProxy fronting at least two gateways; (2) Kafka/RabbitMQ jobs for repair, rebalancing and garbage collection; (3) Kubernetes StatefulSet with PVs and probes; (4) replicated PostgreSQL or three-member etcd; (5) Prometheus and Grafana monitoring. This package includes code/configuration for all five. **It is a course/local implementation, not a production-grade HA deployment.**

## Important — how this relates to Task 6

Copy the **contents** of this ZIP into your current `DistributedMidterm/`, merging existing folders. Files included here:

- `compose.task7.yaml` is an **additive** compose overlay, not a replacement for your Task 6 compose.yaml.
- `storage_node/main.py` adds one authenticated internal inventory endpoint needed for safe GC.
- `metadata_service/main.py` registers the new Task 7 router.
- `metadata_service/task7.py` implements conservative GC and rebalancing for the existing **six-node fixed ring**.
- `task7/queue` contains durable RabbitMQ queue producers/worker for three job types.
- `task7/nginx` has two-gateway load balancing and frontend reverse proxy.
- `task7/monitoring` contains a Prometheus metrics adapter and provisioned Grafana dashboard.
- `compose.task7-ha.yaml` is a separate **isolated Patroni/etcd high-availability PostgreSQL lab** (not wired to your existing data).
- `k8s/` runs the six-node storage layer as a StatefulSet with per-pod PVCs and probes on Docker Desktop Kubernetes; the included development Postgres there is single-instance. Images are local, and all state starts empty.

**NEVER run `docker compose down -v` against your existing project.** Backup your Task 6 code and PostgreSQL volume before extending. Do not commit `.env`, secrets, or downloaded files.

### 1. Install and validate the overlay

Open PyCharm PowerShell in `C:\Users\User\PycharmProjects\DistributedMidterm` (with `.venv` activated):

```powershell
$dc = @('-f','compose.yaml','-f','compose.task7.yaml')
docker compose @dc config --quiet
python -m pytest tests/test_task7_assets.py -v
```

**Screenshot 45**: Compose configuration and pytest tests. Description: We extended our existing project without removing the working distributed storage services.

### 2. Start bonus services (Docker Compose)

First stop the original polling worker to avoid running both worker styles:

```powershell
docker compose stop repair-worker
$dc = @('-f','compose.yaml','-f','compose.task7.yaml')
docker compose @dc up -d --build metadata node1 node2 node3 node4 node5 node6 gateway2 gateway-lb frontend rabbitmq repair-scheduler repair-queue-worker cluster-metrics nginx-exporter prometheus grafana
docker compose @dc ps
```

The original gateway, DB and Elasticsearch remain running from Task 6. If the previously stopped `repair-worker` is accidentally restarted by a broad `up`, stop it again; our repair queue worker uses the existing metadata advisory lock to prevent simultaneous repair transactions, but running both is unnecessary.

**Screenshot 46**: Docker container list, additional gateway, load balancer, RabbitMQ, Prometheus and Grafana all running. Description: We added redundant API gateway instances, a durable repair queue and monitoring services.

### 3. Load balancing with Nginx and two gateways

```powershell
Invoke-WebRequest http://localhost:18400/health -UseBasicParsing | Select-Object StatusCode, Headers
python -m benchmarks.task7_load_balancer_demo
docker compose @dc logs --tail=20 gateway gateway2 gateway-lb
```

The `X-Load-Balancer: task7-nginx` header identifies the proxy. The React website remains **http://localhost:18300**, but its `/api/` calls now route through this load balancer. The direct old gateway port `18200` still exists for debugging.

Test failure without touching the database:

```powershell
docker compose @dc stop gateway
python -m benchmarks.task7_load_balancer_demo
docker compose @dc start gateway
```

The load balancer should continue serving via `gateway2` while `gateway` is down. **Rate limiting remains per-gateway in-memory** (not globally coordinated). Avoid making claims of global synchronized limits.

**Screenshot 47**: Nginx responding and successful 20-request test. Description: Nginx distributed HTTP requests between two gateway containers and maintained service availability when one gateway stopped.

### 4. RabbitMQ queue and background workers

RabbitMQ UI: **http://localhost:15673** (default demo user/password `midterm` / `midterm-local-password`). Do not expose this outside your computer with default credentials.

To publish each supported job:

```powershell
docker compose @dc exec -T repair-scheduler python publish_job.py repair_scrub
docker compose @dc exec -T repair-scheduler python publish_job.py rebalance
docker compose @dc exec -T repair-scheduler python publish_job.py garbage_collect
docker compose @dc logs --tail=80 repair-queue-worker
```

The scheduler automatically enqueues `repair_scrub` jobs periodically. The worker acknowledges a job only after its metadata endpoint returns successfully. The `rebalance` job moves drifted shards toward preferred positions in the **existing six-node ring**, but does **not** implement adding a 7th node dynamically; that needs a topology change to the Task 4 hash ring. The `garbage_collect` job removes aged, unreferenced physical chunks after checking the authoritative PostgreSQL references. It uses a 5-minute age grace and skips offline nodes. The worker logs report `examined`, `moved`, `deleted` or `repaired` values. A zero count is normal when no work is needed.

**Screenshot 48**: RabbitMQ queued/consumed jobs and worker logs. Description: We used durable RabbitMQ messages to dispatch repair/scrub, shard-rebalancing and orphan-chunk cleanup operations to background workers.

### 5. Prometheus and Grafana

- Prometheus: **http://localhost:19090** — query `storage_node_healthy` or `storage_node_chunks`.
- Grafana: **http://localhost:13000** — default demo login `admin` / `admin-local-change-me`. Open **Dashboards → Distributed Storage → DistributedMidterm — Storage Health & Repair**.

The dashboard shows healthy nodes, chunk counts, free space, object versions, latest repair counts, latest repair traffic, latest degraded-in-batch values, and load balancer requests per second derived from Nginx counters. Repair-related metrics are **snapshot gauges** (not cumulative network repair bytes or end-to-end request latency). The Nginx request-rate panel is a true request counter rate, not storage throughput in MiB/s. The last maintenance "degraded" figure refers only to the inspected batch, not necessarily the entire cluster. Prometheus's own scrape status can be checked in its Targets page.

```powershell
Invoke-WebRequest http://localhost:19090/-/healthy -UseBasicParsing | Select-Object StatusCode
Invoke-RestMethod 'http://localhost:19090/api/v1/query?query=storage_node_healthy' | ConvertTo-Json -Depth 8
```

**Screenshot 49**: Grafana dashboard and healthy Prometheus target. Description: Prometheus periodically collected storage node and repair statistics, while Grafana visualized health, disk usage and repair activity.

### 6. Kubernetes StatefulSet with six persistent volumes

Requires Docker Desktop Kubernetes and enough resources. Kubernetes has **separate empty volumes**; it will not automatically see your Compose data. These manifests are for the course/local cluster. The PostgreSQL manifest is demo-only single instance; the Patroni lab in Section 7 demonstrates PostgreSQL high availability separately.

Build images (Docker Desktop uses its local image store; in a remote cluster, push images to a registry first):

```powershell
docker build -f storage_node/Dockerfile -t distributedmidterm-storage-node:task7 .
docker build -f metadata_service/Dockerfile -t distributedmidterm-metadata:task7 .
docker build -f gateway/Dockerfile -t distributedmidterm-gateway:task7 .
docker build -f repair_worker/Dockerfile -t distributedmidterm-repair-worker:task7 .
kubectl apply -f k8s/
kubectl get pods -n distributed-midterm -w
```

Stop watching when all are Running/Ready using Ctrl+C. Verify persistence and probes:

```powershell
kubectl get pods,statefulsets,deployments,pvc -n distributed-midterm
kubectl describe statefulset storage -n distributed-midterm
kubectl delete pod storage-0 -n distributed-midterm
kubectl get pods,pvc -n distributed-midterm
```

The pod should be recreated with the **same PersistentVolumeClaim**. Node services `node1`..`node6` map `storage-0`..`storage-5` so the Task 4 metadata service can reach them with its existing hostnames. For a quick check of the Kubernetes gateway, use:

```powershell
kubectl port-forward -n distributed-midterm service/gateway 18210:8000
```

In another terminal: `Invoke-RestMethod http://localhost:18210/health`.

**Screenshot 50**: Six StatefulSet pods + six PVCs + liveness/readiness configuration. Description: Kubernetes managed six independent storage pods, retaining their per-node persistent volumes and recreating failed pods.

**CAUTION**: the Kubernetes gateway's Elasticsearch integration isn't deployed by these minimal manifests; the core object API can run, but Elasticsearch routes require a separate Elasticsearch deployment. The Kubernetes gateway replica rate limit also remains process-local.

### 7. Isolated replicated PostgreSQL with Patroni and 3-member etcd

This advanced optional experiment demonstrates database-level failover without risking your existing Task 6 PostgreSQL data. It is **a second Compose project**, backed by **three separate database volumes**, independent of the main `db` container.

```powershell
docker compose -f compose.task7-ha.yaml config --quiet
docker compose -f compose.task7-ha.yaml up -d --build
docker compose -f compose.task7-ha.yaml ps
```

Default demo read/write endpoint **127.0.0.1:15434**, HAProxy admin stats **http://localhost:18408/stats**.

Use psql from HA cluster and confirm a primary exists:

```powershell
docker compose -f compose.task7-ha.yaml exec -T patroni1 /opt/patroni/bin/patronictl -c /etc/patroni.yml list
$env:PGPASSWORD='midterm-ha-pass'
psql -h localhost -p 15434 -U postgres -d postgres -c 'SELECT pg_is_in_recovery();'
```

If local `psql` isn't installed, run the client in the HAProxy network using a temporary `postgres:16-alpine` container or inspect Patroni via `patronictl`. To simulate leader failure, first identify the leader from `patronictl list`, stop only that `patroniN` container, and inspect the new leader. Do **not** run database failover experiments against the `distributedmidterm-db-1` container.

**Screenshot 51**: Patroni leader and replicas before/after stopping the leader; HAProxy pointing at the surviving primary. Description: We configured a three-node PostgreSQL cluster coordinated by a three-member etcd cluster, with HAProxy forwarding database connections to the elected primary.

**Caveat**: this optional lab starts from empty data. It is not a migration, switchover, or upgrade of the main app database. It has not been Docker-runtime-tested in this delivery; inspect logs if the HA images or Patroni configuration needs tuning.

### 8. Final verification and save

```powershell
python -m pytest tests/test_task7_assets.py -v
python -m pytest tests/test_gateway_unit.py tests/test_gateway_integration.py -v
$dc = @('-f','compose.yaml','-f','compose.task7.yaml')
docker compose @dc ps
```

Save screenshots 45–51. Save results to `reports/` and commit the source code:

```powershell
git add .
git commit -m 'Implement Task 7 bonus infrastructure'
git status
```

Do not submit `.env`, passwords, private keys, or Docker volumes in Git. Submit the Compose files, Kubernetes manifests, Dockerfiles, code, test scripts and monitoring configs as part of the assignment.

### Scope and limitations

- **Implemented**: two gateway replicas with Nginx load balancing; durable RabbitMQ jobs and worker; conservative six-node rebalancing and authenticated GC; six-pod StatefulSet with PVC/probes; local Prometheus + Grafana; isolated Patroni/etcd HA setup.
- **Needs live verification**: Docker build, RabbitMQ job completion, Kubernetes pods, and Patroni leader failover on your machine.
- **Not claimed**: automatic migration of your existing PostgreSQL to HA, automatic dynamic 7th-node support, global multi-gateway rate limiting, measured request throughput from Prometheus, or cloud deployment. Those require extra work beyond these local demonstrations.
