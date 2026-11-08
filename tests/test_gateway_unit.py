"""Pure unit checks without requiring Docker."""
from fastapi import HTTPException
import pytest
from gateway.main import parse_range, parse_tags

@pytest.mark.parametrize('header,size,expected',[
    ('bytes=0-9',100,(0,9)),('bytes=10-',100,(10,99)),('bytes=-5',100,(95,99)),
    ('bytes=0-999',100,(0,99)),('bytes=0-0',1,(0,0)),
])
def test_range_valid(header,size,expected):
    assert parse_range(header,size)==expected

@pytest.mark.parametrize('header,size',[
    ('bytes=100-',100),('bytes=20-10',100),('bytes=-0',100),('bytes=1-2,3-4',100),('bytes=0-1',0)
])
def test_range_invalid(header,size):
    with pytest.raises(HTTPException) as exc:
        parse_range(header,size)
    assert exc.value.status_code == 416

def test_tags():
    assert parse_tags('{"department":"IT","type":"report"}')=={'department':'IT','type':'report'}
    with pytest.raises(HTTPException): parse_tags('[1,2]')
    with pytest.raises(HTTPException): parse_tags('{"x":10}')
