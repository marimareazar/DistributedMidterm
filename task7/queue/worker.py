"""Consume RabbitMQ jobs. Repair and scrub use the existing metadata service lock."""
import json, os, time
import httpx
import pika
from common import connect, QUEUE

TARGET=os.getenv('METADATA_URL','http://metadata:8000').rstrip('/')
TOKEN=os.getenv('WORKER_TOKEN','local-development-worker-token')

def execute(task):
    kind=task.get('type')
    mapping={
        'repair_scrub': ('/task4/maintenance/run', {'limit':min(200,max(1,int(task.get('limit',200))))}),
        'rebalance': ('/task7/maintenance/rebalance', {'limit':min(200,max(1,int(task.get('limit',20))))}),
        'garbage_collect': ('/task7/maintenance/gc', {'grace_seconds':max(60,int(task.get('grace_seconds',300)))})
    }
    if kind not in mapping:
        raise ValueError(f'Unsupported job type: {kind!r}')
    path,params=mapping[kind]
    r=httpx.post(f'{TARGET}{path}',params=params,
        headers={'X-Worker-Token':TOKEN},timeout=180)
    r.raise_for_status()
    result=r.json()
    if result.get('busy'):
        raise RuntimeError('Maintenance transaction is currently locked')
    print(f'Finished {kind}: {result}', flush=True)

while True:
    connection,ch=connect()
    ch.basic_qos(prefetch_count=1)
    def callback(channel,method,properties,body):
        try:
            execute(json.loads(body))
        except ValueError as exc:
            print(f'Unsupported/dead job: {exc}',flush=True)
            channel.basic_reject(delivery_tag=method.delivery_tag,requeue=False)
        except Exception as exc:
            print(f'Maintenance job failed: {exc}',flush=True)
            channel.basic_nack(delivery_tag=method.delivery_tag,requeue=True)
            time.sleep(3)
        else:
            channel.basic_ack(delivery_tag=method.delivery_tag)
    try:
        ch.basic_consume(queue=QUEUE,on_message_callback=callback,auto_ack=False)
        print('Ready to consume repair/scrub jobs',flush=True)
        ch.start_consuming()
    except (pika.exceptions.AMQPError,OSError) as exc:
        print(f'Queue reconnect after: {exc}',flush=True)
    finally:
        try: connection.close()
        except Exception: pass
