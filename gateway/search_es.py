"""Authenticated Elasticsearch queries and on-demand indexing for Task 6."""
from __future__ import annotations
import os
import httpx
from fastapi import APIRouter, Depends, HTTPException, Query
from gateway.security import check_auth_and_rate
from gateway.indexer import reindex_all

ES_URL = os.getenv('ELASTICSEARCH_URL', 'http://elasticsearch:9200').rstrip('/')
INDEX = 'distributed_objects_v1'
router = APIRouter(prefix='/api/search', dependencies=[Depends(check_auth_and_rate)])


def es_search(params: dict):
    try:
        with httpx.Client(timeout=15) as client:
            response = client.post(f'{ES_URL}/{INDEX}/_search', json=params)
        if response.status_code == 404:
            raise HTTPException(503, 'Elasticsearch index is not ready; run reindex')
        response.raise_for_status()
        return response.json()
    except httpx.HTTPError as exc:
        raise HTTPException(503, 'Elasticsearch is unavailable') from exc


@router.get('/elasticsearch')
def search_elasticsearch(
    q: str = '', bucket: str = '', prefix: str = '',
    min_size: int | None = Query(None, ge=0),
    max_size: int | None = Query(None, ge=0),
    date_from: str = '', date_to: str = '',
    tag_key: str = '', tag_value: str = '',
    limit: int = Query(50, ge=1, le=200), offset: int = Query(0,ge=0),
):
    if (tag_key and not tag_value) or (tag_value and not tag_key):
        raise HTTPException(422, 'Both tag_key and tag_value are required')
    if min_size is not None and max_size is not None and min_size > max_size:
        raise HTTPException(422,'min_size cannot exceed max_size')
    filters = []
    if bucket: filters.append({'term': {'bucket': bucket}})
    if prefix: filters.append({'prefix': {'key': prefix}})
    if min_size is not None or max_size is not None:
        r = {}
        if min_size is not None: r['gte'] = min_size
        if max_size is not None: r['lte'] = max_size
        filters.append({'range': {'size': r}})
    if date_from or date_to:
        r = {}
        if date_from: r['gte'] = date_from
        if date_to: r['lte'] = date_to + 'T23:59:59Z' if len(date_to) == 10 else date_to
        filters.append({'range': {'created_at': r}})
    if tag_key: filters.append({'term': {'tags_flat': f'{tag_key}={tag_value}'}})
    must = [{'multi_match': {'query': q, 'fields': ['key_text^3','bucket_text','tag_text']}}] if q else [{'match_all': {}}]
    result = es_search({'from':offset,'size':limit,'sort':[{'created_at':{'order':'desc'}}, {'version_id':{'order':'desc'}}],
                        'query':{'bool':{'must':must,'filter':filters}}, 'track_total_hits': True})
    return {'total':result['hits']['total']['value'], 'results':[hit['_source'] for hit in result['hits']['hits']],
            'engine':'elasticsearch', 'offset':offset,'limit':limit}


@router.post('/reindex')
def reindex():
    try:
        return reindex_all()
    except (httpx.HTTPError, OSError) as exc:
        raise HTTPException(503, f'Reindex failed: {exc}') from exc


@router.get('/status')
def search_status():
    try:
        with httpx.Client(timeout=5) as client:
            response=client.get(f'{ES_URL}/{INDEX}/_count')
        if response.status_code == 404: return {'ready':False,'indexed_objects':0}
        response.raise_for_status()
        return {'ready':True,'indexed_objects':response.json()['count']}
    except httpx.HTTPError:
        return {'ready':False,'indexed_objects':0}
