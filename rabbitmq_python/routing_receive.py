import pika
import sys
import datetime

credentials = pika.PlainCredentials('tsg', 'tsg2019')
connection = pika.BlockingConnection(pika.ConnectionParameters('154.92.19.167', 5672, '/', credentials))
channel = connection.channel()

channel.exchange_declare(exchange='direct_logs', exchange_type='direct')
result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue
print('this queue name is ', queue_name)

severities = ['info', 'warning']
for severity in severities:
    channel.queue_bind(
        exchange='direct_logs', queue=queue_name, routing_key=severity)

def callback(ch, method, properties, body):
    now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S %f')
    print(" [x] Received {}, received time: {}".format(body, now_str))

print(' [*] Waiting for logs. To exit press CTRL+C')

channel.basic_consume(
    queue=queue_name, on_message_callback=callback, auto_ack=True)

channel.start_consuming()