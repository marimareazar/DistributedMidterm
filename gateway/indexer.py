"""Background PostgreSQL -> Elasticsearch metadata index synchronization.
Existing object IDs are stable; stale entries are removed when versions are deleted.
"""
from __future__ import annotations
import json
import os
import sys
import time
from datetime import datetime, date
import httpx
import psycopg
from psycopg.rows import dict_row

ES_URL=os.getenv('ELASTICSEARCH_URL','http://elasticsearch:9200').rstrip('/')
DATABASE_URL=os.getenv('DATABASE_URL','postgresql://storage:storagepass@db:5432/distributed_storage')
INDEX='distributed_objects_v1'
MAPPING={'mappings':{'properties':{
    'version_id':{'type':'long'}, 'bucket':{'type':'keyword'}, 'bucket_text':{'type':'text'},
    'key':{'type':'keyword'},'key_text':{'type':'text'},'version':{'type':'integer'},
    'size':{'type':'long'},'protection':{'type':'keyword'},'chunking':{'type':'keyword'},
    'created_at':{'type':'date'},'tags':{'type':'object','enabled':False},
    'tags_flat':{'type':'keyword'}, 'tag_text':{'type':'text'},
}}}


def documents():
    with psycopg.connect(DATABASE_URL,row_factory=dict_row) as conn:
        # t5_object_tags is created by the gateway lifespan.
        rows=conn.execute("""SELECT v.id AS version_id,v.bucket,v.object_key AS key,v.version,
            v.size,v.protection,v.chunking,v.created_at,
            COALESCE(t.tags,'{}'::jsonb) AS tags
            FROM t4_versions v LEFT JOIN t5_object_tags t ON t.version_id=v.id
            ORDER BY v.id""").fetchall()
    result=[]
    for row in rows:
        tags=row['tags'] if isinstance(row['tags'],dict) else json.loads(row['tags'])
        row['tags']=tags
        row['created_at']=row['created_at'].isoformat()
        row['key_text']=row['key'].replace('/',' ').replace('_',' ').replace('-',' ')
        row['bucket_text']=row['bucket'].replace('-',' ')
        row['tags_flat']=[f'{k}={v}' for k,v in tags.items()]
        row['tag_text']=' '.join([f'{k} {v}' for k,v in tags.items()])
        result.append(row)
    return result


def reindex_all():
    records=documents()
    with httpx.Client(timeout=90) as client:
        r=client.put(f'{ES_URL}/{INDEX}',json=MAPPING)
        if r.status_code not in (200,201) and r.status_code!=400: r.raise_for_status()
        # Bulk index writes use existing version IDs for idempotence.
        for start in range(0,len(records),500):
            batch=records[start:start+500]
            lines=[]
            for doc in batch:
                lines.append(json.dumps({'index':{'_index':INDEX,'_id':str(doc['version_id'])}}))
                lines.append(json.dumps(doc))
            body='\n'.join(lines)+'\n'
            out=client.post(f'{ES_URL}/_bulk?refresh=true',content=body,
                   headers={'Content-Type':'application/x-ndjson'})
            out.raise_for_status()
            if out.json().get('errors'):
                raise RuntimeError('Elasticsearch bulk index reported item errors')
        # Delete entries for versions that no longer exist in PostgreSQL.
        ids=[str(doc['version_id']) for doc in records]
        if ids:
            query={'query':{'bool':{'must_not':[{'ids':{'values':ids}}]}}}
        else:
            query={'query':{'match_all':{}}}
        response=client.post(f'{ES_URL}/{INDEX}/_delete_by_query?refresh=true&conflicts=proceed',json=query)
        response.raise_for_status()
        return {'indexed_objects':len(records),'stale_removed':response.json().get('deleted',0)}


def main():
    while True:
        try:
            result=reindex_all()
            print(f'index synchronized: {result}',flush=True)
        except Exception as exc:
            print(f'index sync error: {exc}',flush=True)
        time.sleep(int(os.getenv('SEARCH_SYNC_INTERVAL','15')))

if __name__=='__main__':main()
