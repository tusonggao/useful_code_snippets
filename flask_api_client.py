# 使用post方式时，数据放在data或者body中，不能放在url中，放在url中将被忽略。

# select count(*) from jkbd.quanzi_user_force_recentbuy;   8940  原来14041  明天应该是最新的


import time
import requests
import json
import http.client    #修改引用的模块

print('start running')
start_t = time.time()

#------------------------------------------------------------------------------

# c = http.client.HTTPConnection('172.21.57.127', 7777)   # 14041
# c = http.client.HTTPConnection('users-cycles-recommend.dev.jianke.com', 80)  # 14041
#
#
# headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
# c.request('GET', '/get_recentbuy_force', '{}', headers)  # 8940
# # c.request('GET', '/get_recommend/A9343E00-EDC9-40EA-91FA-D41BE979F970', '{}', headers)
# # c.request('GET', '/get_recommend/88B88334-B470-4E7B-8BAD-F99FFFBEA284', '{}', headers) # for ceshi  88B88334-B470-4E7B-8BAD-F99FFFBEA284
# s = c.getresponse().read().strip()
#
# print('s is ', s)
# ddd = json.loads(s)
#
# print('ddd is ', len(ddd))

#------------------------------------------------------------------------------

# response = requests.get('http://users-cycles-recommend.dev.jianke.com/get_recentbuy_force')
response = requests.get('https://fe-acgi.jianke.com/v2/products/475907?manual=true&net=WiFi&sid=35Fr5WIuCfin')
print(response.status_code)  # 打印状态码
print(response.url)          # 打印请求url
print(response.headers)      # 打印头信息
print(response.cookies)      # 打印cookie信息
print(response.text)         # 以文本形式打印网页源码

ddd = json.loads(s)

