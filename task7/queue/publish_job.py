"""Run inside queue container, e.g. docker compose ... exec repair-scheduler
    python publish_job.py garbage_collect
"""
import json,sys,time
import pika
from common import connect, QUEUE
kind=sys.argv[1] if len(sys.argv)>1 else 'repair_scrub'
if kind not in ('repair_scrub','rebalance','garbage_collect'):
    raise SystemExit('Use repair_scrub, rebalance, or garbage_collect')
connection,ch=connect()
ch.basic_publish(exchange='',routing_key=QUEUE,body=json.dumps({'type':kind,'created_at':time.time()}),
    properties=pika.BasicProperties(delivery_mode=2,content_type='application/json'))
print(f'Queued {kind}',flush=True)
connection.close()
