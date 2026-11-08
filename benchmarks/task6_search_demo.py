"""Task 6 end-to-end Elasticsearch search demonstration; requires running containers."""
import json
import os
import time
import uuid
from pathlib import Path
import httpx

BASE=os.getenv('GATEWAY_URL','http://localhost:18200').rstrip('/')
KEY=os.getenv('GATEWAY_API_KEY','change-me-local-api-key')
HEAD={'X-API-Key':KEY}


def main():
    name='ui-demo-'+uuid.uuid4().hex[:9]
    with httpx.Client(timeout=120,headers=HEAD) as client:
        b=client.post(BASE+'/api/buckets',json={'name':name,'protection':'replication'})
        b.raise_for_status()
        content=b'Task 6 Elasticsearch demonstration - Computer Engineering'
        up=client.post(BASE+f'/api/buckets/{name}/objects',data={'key':'reports/term-paper.txt','tags':json.dumps({'course':'midterm','year':'2026'}),'chunking':'fixed'},
                       files={'file':('term-paper.txt',content,'text/plain')})
        up.raise_for_status()
        idx=client.post(BASE+'/api/search/reindex');idx.raise_for_status()
        results=client.get(BASE+'/api/search/elasticsearch',params={'bucket':name,'prefix':'reports/','tag_key':'course','tag_value':'midterm'})
        results.raise_for_status();result=results.json()
        assert result['total']>=1 and any(x['key']=='reports/term-paper.txt' for x in result['results'])
        state={'bucket':name,'upload_status':up.status_code,'indexed':idx.json()['indexed_objects'],
               'search_total':result['total'],'engine':result['engine'],'matched':True}
        Path('reports').mkdir(exist_ok=True)
        Path('reports/task6_search_demo.json').write_text(json.dumps(state,indent=2))
        print(json.dumps(state,indent=2))
        print('Saved reports/task6_search_demo.json')
if __name__=='__main__':main()
