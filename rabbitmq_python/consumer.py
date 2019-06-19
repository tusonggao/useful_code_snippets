import pika


# credentials = pika.PlainCredentials('guest', 'guest')
# connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', 5672, '/', credentials))
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
  
channel.queue_declare(queue='hello')
  
def callback(ch, method, properties, body):
    print(" [x] Received %r" % (body,))
  
# channel.basic_consume('hello', callback)

channel.basic_consume(queue='hello',
                      auto_ack=True,
                      on_message_callback=callback)

# , consumer_tag='hello-consumer'
  
print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()