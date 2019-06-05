import redis
import datetime
import time


if __name__ == "__main__":
    number_list = ['300030', '300031', '300032', '300033', '300034', '300035', '300036', '300037', '300038', '19999']
    signal = ['1', '-1']
    # conn = redis.Redis(host='redis-12143.c8.us-east-1-3.ec2.cloud.redislabs.com',
    #                    port=12143, password='that is a secrty')
    # conn = redis.Redis(host='127.0.0.1', port=6379, db=0)
    conn = redis.Redis(host='47.99.141.164', port=6379, db=0, password='tusonggao2019')
    # conn = redis.Redis(host='redis-17215.c100.us-east-1-4.ec2.cloud.redislabs.com',
    #                    port=17215, password='c1FwoIl9cs75eBGl6Z7zXCfY26zQ6P7p')

    pub_cnt = 0
    for i in range(50):
        value_new = str(number_list[i%len(number_list)]) + ' ' + str(signal[i%2])
        conn.publish("chat", value_new)
        pub_cnt += 1
        now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S %f')
        print('pub_cnt: ', pub_cnt, 'now_str: ', now_str, 'publish item ', value_new)
        time.sleep(3)