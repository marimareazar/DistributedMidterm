"""Verify LB availability; don't claim requests balanced exactly without upstream logs."""
import httpx, json
from pathlib import Path
base='http://localhost:18400'
results=[]
with httpx.Client(timeout=10) as client:
    for i in range(20):
        try:
            r=client.get(base+'/health')
            results.append({'attempt':i+1,'status':r.status_code,'lb_header':r.headers.get('X-Load-Balancer')})
        except httpx.HTTPError as exc:
            results.append({'attempt':i+1,'error':str(exc)})
summary={'request_count':len(results),'successful':sum(x.get('status')==200 for x in results),'requests':results}
Path('reports').mkdir(exist_ok=True)
Path('reports/task7_loadbalancer.json').write_text(json.dumps(summary,indent=2))
print(json.dumps(summary,indent=2))
if summary['successful']!=20: raise SystemExit('Some LB requests failed')
