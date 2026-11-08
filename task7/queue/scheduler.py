"""Durable scheduled repair/scrub jobs, one outstanding job at a time."""
import json, os, time
import pika
from common import connect, QUEUE

INTERVAL = max(10, int(os.getenv('SCHEDULE_INTERVAL','30')))
while True:
    connection, ch = connect()
    try:
        while True:
            # Keep the queue from accumulating many redundant full scrubs.
            state = ch.queue_declare(queue=QUEUE, passive=True).method
            if (state.message_count + state.consumer_count * 0) < 2:
                payload=json.dumps({'type':'repair_scrub','limit':200,'created_at':time.time()})
                ch.basic_publish(exchange='',routing_key=QUEUE,body=payload,
                    properties=pika.BasicProperties(delivery_mode=2,content_type='application/json'))
                print('Queued scheduled repair/scrub job',flush=True)
            time.sleep(INTERVAL)
    except (pika.exceptions.AMQPError,OSError) as exc:
        print(f'Scheduler reconnect: {exc}',flush=True)
    finally:
        try: connection.close()
        except Exception: pass
