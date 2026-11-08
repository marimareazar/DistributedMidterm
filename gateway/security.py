"""API key auth and fixed-window limits, per gateway process."""
import os
import secrets
import threading
import time
from collections import defaultdict
from fastapi import HTTPException, Request

_LOCK = threading.Lock()
_WINDOWS = defaultdict(list)


def check_auth_and_rate(request: Request):
    expected = os.getenv('GATEWAY_API_KEY', 'change-me-local-api-key')
    provided = request.headers.get('X-API-Key', '')
    if not secrets.compare_digest(provided, expected):
        raise HTTPException(401, 'Invalid or missing X-API-Key')
    limit = int(os.getenv('RATE_LIMIT_PER_MINUTE', '120'))
    if limit <= 0:
        raise HTTPException(503, 'Rate limiting misconfigured')
    address = request.client.host if request.client else 'unknown'
    key = (address, expected)
    now = time.monotonic()
    with _LOCK:
        values = _WINDOWS[key]
        values[:] = [v for v in values if now - v < 60]
        if len(values) >= limit:
            raise HTTPException(429, 'Rate limit exceeded; retry in one minute', headers={'Retry-After': '60'})
        values.append(now)
    return True
