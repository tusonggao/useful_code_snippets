import pika
  
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
  
channel.queue_declare(queue='hello')
  
def callback(ch, method, properties, body):
    print(" [x] Received %r" % (body,))
  
channel.basic_consume('hello', callback)
  
print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()