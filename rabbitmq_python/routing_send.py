import pika
import sys
import time
import random
import datetime

credentials = pika.PlainCredentials('tsg', 'tsg2019')
connection = pika.BlockingConnection(pika.ConnectionParameters('154.92.19.167', 5672, '/', credentials))
channel = connection.channel()
channel.exchange_declare(exchange='direct_logs', exchange_type='direct')

severity_lst = ['info', 'warning', 'error']

for i in range(15):
    print('sent msg: {}'.format(i))
    severity = random.sample(severity_lst, 1)[0]
    now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S %f')
    msg_body = 'Hello World! new1_msg_num: {} severity: {} send_time: {}'.format(i, severity, now_str)
    print('msg_body is', msg_body)
    channel.basic_publish(exchange='direct_logs',
                          routing_key=severity,
                          body=msg_body)
    time.sleep(10)

connection.close()