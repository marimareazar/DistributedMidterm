"""Dedicated background failure-repair and scrub worker."""
import os
import time
import httpx

TARGET = os.getenv('METADATA_URL', 'http://metadata:8000').rstrip('/')
TOKEN = os.getenv('WORKER_TOKEN', 'local-development-worker-token')
INTERVAL = float(os.getenv('REPAIR_INTERVAL', '10'))

if __name__ == '__main__':
    while True:
        try:
            r = httpx.post(TARGET + '/task4/maintenance/run',
                           headers={'X-Worker-Token': TOKEN}, timeout=180)
            print(f'[repair-worker] status={r.status_code} {r.text[:800]}', flush=True)
        except Exception as exc:
            print(f'[repair-worker] retry after error: {exc}', flush=True)
        time.sleep(INTERVAL)
