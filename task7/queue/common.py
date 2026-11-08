import os, time, pika
QUEUE = 'storage.maintenance'

def connect():
    url = os.getenv('AMQP_URL', 'amqp://midterm:midterm-local-password@rabbitmq:5672/%2F')
    for attempt in range(30):
        try:
            c = pika.BlockingConnection(pika.URLParameters(url))
            ch = c.channel()
            ch.queue_declare(queue=QUEUE, durable=True, arguments={'x-queue-type':'quorum'})
            return c, ch
        except (pika.exceptions.AMQPConnectionError, OSError) as e:
            print(f'RabbitMQ unavailable ({e}), retrying', flush=True)
            time.sleep(2)
    raise RuntimeError('RabbitMQ unavailable after retries')
