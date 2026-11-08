"""Task 6 search query tests: no Elasticsearch service needed for these."""
import pytest
from fastapi.testclient import TestClient
from gateway import search_es
from gateway.main import app
from gateway.security import _WINDOWS


@pytest.fixture(autouse=True)
def clean(monkeypatch):
    monkeypatch.setenv('GATEWAY_API_KEY','unit-search-key')
    monkeypatch.setenv('RATE_LIMIT_PER_MINUTE','1000')
    _WINDOWS.clear()
    monkeypatch.setattr(search_es, 'es_search', lambda query: {'hits':{'total':{'value':0}, 'hits':[]}})


def test_query_filters():
    seen={}
    def fake(q):
        seen.update(q)
        return {'hits':{'total':{'value':1}, 'hits':[{'_source':{'key':'folder/a.txt'}}]}}
    search_es.es_search=fake
    # Avoid DB lifespan dependency by using context that is initialized outside tests.
    from httpx import ASGITransport
    import asyncio
    async def run():
        import httpx
        async with httpx.AsyncClient(transport=ASGITransport(app=app),base_url='http://test') as client:
            return await client.get('/api/search/elasticsearch', headers={'X-API-Key':'unit-search-key'}, params={
              'bucket':'demo','prefix':'folder/','tag_key':'course','tag_value':'midterm','min_size':100})
    res=asyncio.run(run())
    assert res.status_code==200
    assert res.json()['results'][0]['key']=='folder/a.txt'
    assert {'term':{'bucket':'demo'}} in seen['query']['bool']['filter']
    assert {'term':{'tags_flat':'course=midterm'}} in seen['query']['bool']['filter']


def test_unauthorized():
    import asyncio, httpx
    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
            return await client.get('/api/search/elasticsearch')
    assert asyncio.run(run()).status_code==401


def test_invalid_size_range():
    import asyncio, httpx
    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
            return await client.get('/api/search/elasticsearch?min_size=100&max_size=2',headers={'X-API-Key':'unit-search-key'})
    assert asyncio.run(run()).status_code==422
