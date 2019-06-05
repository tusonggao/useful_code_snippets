import redis
import datetime


if __name__ == "__main__":
    # conn = redis.Redis(host='redis-12143.c8.us-east-1-3.ec2.cloud.redislabs.com',
    #                    port=12143, password='that is a secrty')
    # conn = redis.Redis(host='127.0.0.1', port=6379, db=0)
    conn = redis.Redis(host='47.99.141.164', port=6379, db=0, password='tusonggao2019')
    # conn = redis.Redis(host='redis-17215.c100.us-east-1-4.ec2.cloud.redislabs.com',
    #                    port=17215, password='c1FwoIl9cs75eBGl6Z7zXCfY26zQ6P7p')
    ps = conn.pubsub()
    ps.subscribe('chat')  # 从 chat 订阅消息
    print('sub chat in redis_sub')

    msg_cnt = 0
    for item in ps.listen():  # 监听状态：有消息发布了就拿过来
        if item['type'] == 'message':
            msg_cnt += 1
            now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S %f')
            print('msg_cnt: ', msg_cnt, 'now_str: ', now_str, 'item channel is ',
                  item['channel'], 'item data is', item['data'])


