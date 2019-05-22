# 使用post方式时，数据放在data或者body中，不能放在url中，放在url中将被忽略。

import time
import requests
import json
import http.client    #修改引用的模块

print('start running')
start_t = time.time()

#------------------------------------------------------------------------------

c = http.client.HTTPConnection('172.21.57.127', 5000)


headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
c.request('GET', '/add/123/321', '{}', headers)
s = c.getresponse().read().strip()
print('s is ', s)