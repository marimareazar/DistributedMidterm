"""Metrics exported from real metadata and PostgreSQL state; no invented repair rates."""
import os
import httpx
from fastapi import FastAPI, Response
from prometheus_client import CollectorRegistry, Gauge, generate_latest, CONTENT_TYPE_LATEST
import psycopg

app=FastAPI()
URL=os.getenv('CLUSTER_URL','http://metadata:8000/task4/cluster')
DSN=os.getenv('DATABASE_URL','postgresql://storage:storagepass@db:5432/distributed_storage')

@app.get('/health')
def health(): return {'status':'ok'}

@app.get('/metrics')
def metrics():
    reg=CollectorRegistry()
    node_online=Gauge('storage_node_healthy','Node heartbeat health',['node'],registry=reg)
    chunks=Gauge('storage_node_chunks','Physical chunk count',['node'],registry=reg)
    free=Gauge('storage_node_free_bytes','Available space on node',['node'],registry=reg)
    examined=Gauge('storage_repair_examined_latest','Last maintenance chunk sample size',registry=reg)
    repaired=Gauge('storage_repair_shards_latest','Latest repair job repaired shards',registry=reg)
    bad=Gauge('storage_repair_bad_shards_latest','Latest job missing/corrupted shard count',registry=reg)
    traffic=Gauge('storage_repair_bytes_latest','Latest maintenance transfer bytes',registry=reg)
    degraded=Gauge('storage_degraded_in_latest_batch','Latest maintenance degraded chunks in inspected batch',registry=reg)
    objects=Gauge('storage_object_versions','Active object versions',registry=reg)
    unique=Gauge('storage_unique_chunks','Distinct logical chunks',registry=reg)
    scraping=Gauge('storage_metrics_scrape_success','1 if collector can query dependencies',registry=reg)
    try:
        r=httpx.get(URL,timeout=10); r.raise_for_status(); body=r.json()
        for n in body.get('nodes',[]):
            ident=n['node_id']
            node_online.labels(ident).set(int(bool(n.get('healthy'))))
            chunks.labels(ident).set(int(n.get('chunk_count') or 0))
            free.labels(ident).set(int(n.get('free_space') or 0))
        last=body.get('last_maintenance') or {}
        examined.set(last.get('examined') or 0)
        repaired.set(last.get('repaired') or 0)
        bad.set(last.get('corrupted_or_missing') or 0)
        traffic.set(last.get('bytes_transferred') or 0)
        degraded.set(last.get('degraded') or 0)
        with psycopg.connect(DSN) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT COUNT(*) FROM t4_versions');objects.set(cur.fetchone()[0])
                cur.execute('SELECT COUNT(*) FROM t4_chunks');unique.set(cur.fetchone()[0])
        scraping.set(1)
    except Exception as exc:
        print('metrics scrape failed:',exc,flush=True)
        scraping.set(0)
    return Response(content=generate_latest(reg),media_type=CONTENT_TYPE_LATEST)
