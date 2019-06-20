import pika
import time
import datetime


###################################################################################################################

# credentials = pika.PlainCredentials('tsg', 'tsg2019')
# # connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', 5672, '/', credentials))
# connection = pika.BlockingConnection(pika.ConnectionParameters('154.92.19.167', 5672, '/', credentials))
#
# # connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
# channel = connection.channel()
#
# channel.queue_declare(queue='hello', durable=True)
#
# for i in range(10):
#     print('sent msg: {}'.format(i))
#     now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S %f')
#     channel.basic_publish(exchange='',
#                           routing_key='hello',
#                           body='Hello World! in jianke windows machine, msg_num: {}, send_time: {}'.format(i, now_str),
#                           properties=pika.BasicProperties(delivery_mode=2))
#     time.sleep(10)
#
# # channel.basic_publish(exchange='', routing_key='hello', body='Hello World tsg!')
#
# print("Sent 'Hello World!'")
# connection.close()

###################################################################################################################

# credentials = pika.PlainCredentials('tsg', 'tsg2019')
# connection = pika.BlockingConnection(pika.ConnectionParameters('154.92.19.167', 5672, '/', credentials))


credentials = pika.PlainCredentials('admin', 'jianke@mall123')
connection = pika.BlockingConnection(pika.ConnectionParameters('172.25.10.43', 5672, '/essearch', credentials))


channel = connection.channel()
channel.exchange_declare(exchange='logs', exchange_type='fanout')

for i in range(10):
    print('sent msg: {}'.format(i))
    now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S %f')
    channel.basic_publish(exchange='logs',
                          routing_key='',
                          body='Hello World! in jianke windows machine, new msg_num: {}, send_time: {}'.format(i, now_str))
    time.sleep(10)

connection.close()


