import pika


# credentials = pika.PlainCredentials('guest', 'guest')
# connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', 5672, '/', credentials))

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
  
channel.queue_declare(queue='hello')
  
# channel.basic_publish(exchange='', routing_key='hello', body='Hello World!')


channel.basic_publish(exchange='', routing_key='hello', body='Hello World tsg!')


print("Sent 'Hello World!'")
connection.close()