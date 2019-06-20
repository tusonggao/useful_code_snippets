import pika
import time
import datetime


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


