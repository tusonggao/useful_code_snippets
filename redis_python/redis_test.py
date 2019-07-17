import redis
import time

print('hello world!')


# ./src/redis-server --protected-mode no

# conn = redis.Redis(host='127.0.0.1', port=6379, db=0)
# conn = redis.Redis(host='0.0.0.0', port=6379, db=0)


# conn = redis.Redis(host='47.99.141.164', port=6379, db=0, password='tusonggao2019')
# conn = redis.Redis(host='47.99.141.164', port=6379, db=0)
# conn = redis.Redis(host='172.16.5.169', port=6379, db=0, password='tusonggao2019')
# conn = redis.Redis(host='redis-17215.c100.us-east-1-4.ec2.cloud.redislabs.com',
#                    port=17215, password='c1FwoIl9cs75eBGl6Z7zXCfY26zQ6P7p')

conn = redis.Redis(host='172.25.40.36', port=6379, db=11)

# conn.set('nametsg', 'zhangshantest')   #添加

start_t = time.time()
val = conn.get('nametsg')
redis_cost_time = time.time()-start_t

print ('val is ', val, 'cost time: ', redis_cost_time)   #获取

if val is None:
    print('val is None')