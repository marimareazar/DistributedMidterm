"""Small curl-free demo and JSON report of gateway capabilities."""
import hashlib
import json
import os
import uuid
from pathlib import Path
import httpx

URL = os.getenv('GATEWAY_TEST_URL','http://localhost:18200')
KEY=os.getenv('GATEWAY_API_KEY','change-me-local-api-key')

def main():
    result={}
    with httpx.Client(base_url=URL,timeout=180) as c:
        result['unauthenticated_status']=c.get('/api/buckets').status_code
        headers={'X-API-Key':KEY}
        name='dem'+uuid.uuid4().hex[:12]
        result['bucket_create']=c.post('/api/buckets',headers=headers,json={'name':name,'protection':'replication'}).status_code
        content=b'DistributedMidterm task five range download test! '*1000
        uploaded=c.post(f'/api/buckets/{name}/objects',headers=headers,
                        data={'key':'folder/test.txt','tags':'{"topic":"midterm"}'},
                        files={'file':('test.txt',content,'text/plain')})
        uploaded.raise_for_status()
        result['upload']=uploaded.json()
        got=c.get(f'/api/buckets/{name}/objects/folder/test.txt/download',headers=headers)
        got.raise_for_status()
        result['full_download_sha256_match']=hashlib.sha256(got.content).digest()==hashlib.sha256(content).digest()
        part=c.get(f'/api/buckets/{name}/objects/folder/test.txt/download',headers={**headers,'Range':'bytes=0-19'})
        result['range_status']=part.status_code
        result['range_verified']=part.content==content[:20]
        search=c.get('/api/search',headers=headers,params={'bucket':name,'tag_key':'topic','tag_value':'midterm'})
        search.raise_for_status()
        result['search_results']=len(search.json()['results'])
        cluster=c.get('/api/cluster',headers=headers)
        cluster.raise_for_status()
        result['healthy_nodes']=sum(bool(n['healthy']) for n in cluster.json()['nodes'])
        result['delete_status']=c.delete(f'/api/buckets/{name}/objects/folder/test.txt',headers=headers).status_code
    Path('reports').mkdir(exist_ok=True)
    Path('reports/task5_api_demo.json').write_text(json.dumps(result,indent=2))
    print(json.dumps(result,indent=2))
    print('Saved: reports/task5_api_demo.json')
    assert (result['unauthenticated_status']==401 and result['bucket_create']==201 and
            result['full_download_sha256_match'] and result['range_verified'] and
            result['search_results']==1 and result['delete_status']==200)

if __name__=='__main__':main()
