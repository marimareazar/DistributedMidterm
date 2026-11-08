"""Task 7 authorized maintenance actions: GC and conservative ring rebalancing.

WARNING: RING currently has six configured nodes. This handles drift within those
six nodes; scaling to arbitrary seventh node requires extending Task 4's ring.
"""
import os, time
import httpx
import psycopg
from psycopg.rows import dict_row
from fastapi import APIRouter, Header, HTTPException, Query
from metadata_service.redundancy import NODE_NAMES, NODE_URLS, RING, TOKEN, live_nodes, referenced_elsewhere

router=APIRouter(prefix='/task7', tags=['Task 7: queued maintenance'])
DSN=os.getenv('DATABASE_URL','postgresql://storage:storagepass@db:5432/distributed_storage')

def authorize(token):
    if token != TOKEN: raise HTTPException(403,'Worker token required')

def node_inventory(node):
    r=httpx.get(f'{NODE_URLS[node]}/internal/chunks',headers={'X-Worker-Token':TOKEN},timeout=15)
    r.raise_for_status()
    return r.json()['chunks']

@router.post('/maintenance/gc')
def garbage_collect(x_worker_token: str | None=Header(None), grace_seconds: int=Query(300,ge=60,le=86400), dry_run: bool=Query(False)):
    """Remove aged, unreferenced physical chunks only. Skips unreachable nodes."""
    authorize(x_worker_token)
    summary={'examined':0,'orphaned':0,'deleted':0,'failed':0,'skipped_nodes':[],'dry_run':dry_run}
    with psycopg.connect(DSN,row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            active=live_nodes(cur)
            refrows=cur.execute('SELECT node_id,BTRIM(sha256) AS hash FROM t4_shards').fetchall()
            refrows+=cur.execute('SELECT node_id,BTRIM(sha256) AS hash FROM chunks').fetchall()
            refs={(row['node_id'],row['hash']) for row in refrows}
    for node in NODE_NAMES:
        if node not in active:
            summary['skipped_nodes'].append(node)
            continue
        try:
            inventory=node_inventory(node)
        except (httpx.HTTPError,KeyError,ValueError):
            summary['skipped_nodes'].append(node)
            continue
        for item in inventory:
            summary['examined']+=1
            h=item['sha256']
            if (node,h) in refs or time.time()-item['mtime']<grace_seconds:
                continue
            summary['orphaned']+=1
            if dry_run: continue
            try:
                with psycopg.connect(DSN,row_factory=dict_row) as conn:
                    with conn.cursor() as cur:
                        # Recheck under DB transaction to avoid racing with an upload.
                        count=cur.execute("""SELECT
                            (SELECT COUNT(*) FROM t4_shards WHERE node_id=%s AND BTRIM(sha256)=%s)
                          + (SELECT COUNT(*) FROM chunks WHERE node_id=%s AND BTRIM(sha256)=%s) AS n""",
                            (node,h,node,h)).fetchone()['n']
                        if count: continue
                        resp=httpx.delete(f'{NODE_URLS[node]}/chunks/{h}',timeout=20)
                        if resp.status_code in (200,404): summary['deleted']+=1
                        else: summary['failed']+=1
            except (httpx.HTTPError,psycopg.Error):
                summary['failed']+=1
    return summary

@router.post('/maintenance/rebalance')
def rebalance(x_worker_token: str | None=Header(None), limit: int=Query(20,ge=1,le=200)):
    """Move misplaced shards toward current 6-node ring. Safeguard: don't move
    without a live target, verified source bytes, and exclusive target ownership."""
    authorize(x_worker_token)
    summary={'examined':0,'moved':0,'bytes_transferred':0,'skipped':0,'note':'Current fixed six-node ring only'}
    with psycopg.connect(DSN,row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            if not cur.execute('SELECT pg_try_advisory_xact_lock(74004007) AS ok').fetchone()['ok']:
                return {'busy':True}
            live=live_nodes(cur)
            if len(live)<len(NODE_NAMES):
                return {**summary,'skipped':'Require all six nodes healthy for safe rebalancing'}
            rows=cur.execute('SELECT * FROM t4_chunks ORDER BY id LIMIT %s',(limit,)).fetchall()
            for chunk in rows:
                summary['examined']+=1
                shards=cur.execute('SELECT * FROM t4_shards WHERE chunk_id=%s ORDER BY shard_index', (chunk['id'],)).fetchall()
                n=chunk['replicas'] if chunk['protection']=='replication' else 6
                preferred=RING.locate_replicas(chunk['sha256'].strip(),n)
                present={s['node_id'] for s in shards}
                for s in shards:
                    expected=preferred[s['shard_index']]
                    if expected==s['node_id'] or expected in present:
                        continue
                    try:
                        r=httpx.get(f"{NODE_URLS[s['node_id']]}/chunks/{s['sha256'].strip()}",timeout=30)
                        if r.status_code!=200: continue
                        from hashlib import sha256
                        payload=r.content
                        if sha256(payload).hexdigest()!=s['sha256'].strip(): continue
                        uploaded=httpx.put(f"{NODE_URLS[expected]}/chunks/{s['sha256'].strip()}",content=payload,timeout=30)
                        if uploaded.status_code not in (200,201):continue
                        cur.execute('UPDATE t4_shards SET node_id=%s WHERE chunk_id=%s AND shard_index=%s',
                                    (expected,chunk['id'],s['shard_index']))
                        # Keep the original chunk on the old owner until a future GC check.
                        # Deleting immediately could remove data shared by another object.
                        present.discard(s['node_id']);present.add(expected)
                        summary['moved']+=1;summary['bytes_transferred']+=len(payload)
                    except httpx.HTTPError:
                        summary['skipped']+=1
    return summary
