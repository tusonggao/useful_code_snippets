import pika
import datetime


# credentials = pika.PlainCredentials('tsg', 'tsg2019')
# connection = pika.BlockingConnection(pika.ConnectionParameters('154.92.19.167', 5672, '/', credentials))

credentials = pika.PlainCredentials('admin', 'jianke@mall123')
connection = pika.BlockingConnection(pika.ConnectionParameters('172.25.10.43', 5672, '/essearch', credentials))

channel = connection.channel()
channel.exchange_declare(exchange='logs', exchange_type='fanout')

result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue
print('this queue_name is ', queue_name)

channel.queue_bind(exchange='logs', queue=queue_name)

def callback(ch, method, properties, body):
    now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S %f')
    print(" [x] Received {}, received time: {}".format(body, now_str))
  

channel.basic_consume(queue=queue_name,
                      auto_ack=True,
                      on_message_callback=callback)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()