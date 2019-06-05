import redis

print('hello world!')


# ./src/redis-server --protected-mode no

# conn = redis.Redis(host='127.0.0.1', port=6379, db=0)
# conn = redis.Redis(host='47.99.141.164', port=6379, db=0, password='tusonggao2019')
conn = redis.Redis(host='47.99.141.164', port=6379, db=0)
# conn = redis.Redis(host='172.16.5.169', port=6379, db=0, password='tusonggao2019')
# conn = redis.Redis(host='redis-17215.c100.us-east-1-4.ec2.cloud.redislabs.com',
#                    port=17215, password='c1FwoIl9cs75eBGl6Z7zXCfY26zQ6P7p')


conn.set('name', 'zhangsan')   #添加
print (conn.get('name'))   #获取