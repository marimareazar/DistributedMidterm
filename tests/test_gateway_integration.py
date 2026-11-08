"""Live gateway integration tests; requires Docker Compose running."""
import os
import uuid
import hashlib
import httpx
import pytest

BASE=os.getenv('GATEWAY_TEST_URL','http://localhost:18200')
KEY=os.getenv('GATEWAY_API_KEY','change-me-local-api-key')
HEADERS={'X-API-Key':KEY}

@pytest.fixture(scope='module')
def client():
    with httpx.Client(base_url=BASE,timeout=120) as c:
        try: r=c.get('/health'); r.raise_for_status()
        except httpx.HTTPError as exc: pytest.fail(f'Gateway not accessible: {exc}')
        yield c


def test_auth_and_rate_headers(client):
    assert client.get('/api/buckets').status_code==401
    assert client.get('/api/buckets',headers={'X-API-Key':'wrong'}).status_code==401
    assert client.get('/api/buckets',headers=HEADERS).status_code==200


@pytest.mark.parametrize('mode', ['replication','erasure'])
def test_gateway_object_lifecycle(client,mode):
    bucket='g'+uuid.uuid4().hex[:16]
    key='folder/integration.bin'
    original=(b'hello distributed gateway task 5! '*6000)
    result=client.post('/api/buckets',headers=HEADERS,json={'name':bucket,'protection':mode,'replicas':3})
    assert result.status_code==201,result.text
    uploaded=client.post(f'/api/buckets/{bucket}/objects',headers=HEADERS,
        data={'key':key,'chunking':'fastcdc','tags':'{"team":"engineering","stage":"test"}'},
        files={'file':('integration.bin',original,'application/octet-stream')})
    assert uploaded.status_code==201, uploaded.text
    assert uploaded.json()['version']==1
    assert uploaded.json()['tags']['team']=='engineering'
    duplicate=client.post(f'/api/buckets/{bucket}/objects',headers=HEADERS,
        data={'key':key,'chunking':'fastcdc','tags':'{"team":"engineering"}'},
        files={'file':('integration.bin',original,'application/octet-stream')})
    assert duplicate.status_code==201, duplicate.text
    assert duplicate.json()['version']==2
    assert duplicate.json()['new_unique_chunks']==0
    listing=client.get(f'/api/buckets/{bucket}/objects',headers=HEADERS)
    assert listing.status_code==200,listing.text
    assert len(listing.json()['objects'])==1
    info=client.get(f'/api/buckets/{bucket}/objects/{key}/metadata',headers=HEADERS)
    assert info.status_code==200 and info.json()['version']==2
    downloaded=client.get(f'/api/buckets/{bucket}/objects/{key}/download',headers=HEADERS)
    assert downloaded.status_code==200 and hashlib.sha256(downloaded.content).digest()==hashlib.sha256(original).digest()
    partial=client.get(f'/api/buckets/{bucket}/objects/{key}/download',headers={**HEADERS,'Range':'bytes=10-29'})
    assert partial.status_code==206 and partial.content==original[10:30]
    assert partial.headers['Content-Range']==f'bytes 10-29/{len(original)}'
    suffix=client.get(f'/api/buckets/{bucket}/objects/{key}/download',headers={**HEADERS,'Range':'bytes=-10'})
    assert suffix.status_code==206 and suffix.content==original[-10:]
    bad=client.get(f'/api/buckets/{bucket}/objects/{key}/download',headers={**HEADERS,'Range':'bytes=999999999-'})
    assert bad.status_code==416
    searched=client.get('/api/search',headers=HEADERS,params={'bucket':bucket,'prefix':'folder/',
        'min_size':len(original),'max_size':len(original),'tag_key':'team','tag_value':'engineering'})
    assert searched.status_code==200,searched.text
    assert len(searched.json()['results'])==2
    status=client.get('/api/cluster',headers=HEADERS)
    assert status.status_code==200 and len(status.json()['nodes'])>=6
    for version in (1,2):
        removed=client.delete(f'/api/buckets/{bucket}/objects/{key}',headers=HEADERS,params={'version':version})
        assert removed.status_code==200,removed.text
    missing=client.get(f'/api/buckets/{bucket}/objects/{key}/metadata',headers=HEADERS)
    assert missing.status_code==404
