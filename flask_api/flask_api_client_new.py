import time
import requests
import json

import requests

data = {
    'name': 'tom',
    'age': 20
}

# response = requests.get('http://127.0.0.1:8080/test_args', params=data)
# print(response.text)

params = {
    'user_ids': ['99C0972D-B1EC-4000-824F-462791D433F7',
                 '7A009D92-CA59-4EEF-80F5-AE0BCA507E10',
                 '413E648D-CA1A-4684-862A-24B6FC0024C7',
                 '5C72E70A-0D86-4E61-8129-1AC6ADB50B5B',
                 '883A07D0-CC66-4D50-AEFF-66386FDC1575'],
    'name': ['tsg', 'hu']
}


# params = {
#     'user_ids': ['7A009D92-CA59-4EEF-80F5-AE0BCA507E10'],
#     'name': ['tsg', 'hu']
# }


# host_name = 'http://ai-service.idc.jianke.com/callcenter-reco'
# host_name = 'http://172.21.57.127:7979'
# host_name = 'http://detail-page-item-based-recommend.dev.jianke.com'
# host_name = 'http://detail-page-item-based-recommend.internal.jianke.com'
# host_name = 'http://detail-page-old-online-recommend.dev.jianke.com'
# host_name = 'http://detail-page-old-online-recommend.internal.jianke.com'
# host_name = 'http://abtest-service.tst.jianke.com'
host_name = 'http://abtest-service.internal.jianke.com'


params = {
          # 'product_code': 439,
          # 'product_code': 286002,
          'product_code': 999999999,
          'user_id': '0011tttyy88oo99uu'}

# params = {'product_code': '286002',
#           'user_id': '0011tttyy88oo99333'}  # item-based

start_t = time.time()

# headers = {
#  'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.108 Safari/537.36',
#  'X-JK-UDID': '000001111112222223333333'
# }

headers = {
    # 'X-JK-UDID': '000001111112222223333333tttuuo',   # 
    # 'X-JK-UDID': '000001111112222223333333tttuui',   # 
    # 'X-JK-UDID': '000001111112222223333333tttuuz',   # 
    # 'X-JK-UDID': '000001111112222223333333tttuuu',     # 
    # 'X-JK-UDID': '000001111112222223333333tttw',       # new {"outcome":["1925","11208","2060","54934","54905"],"algorithm":"NEW_ITEM_BASED"}
    # 'X-JK-UDID': '000001111112222223333333ttte',       # old {"outcome":["3394","28","222","357","1990"],"algorithm":"OLD_ON_LINE"}
    # 'X-JK-UDID': '000001111112222223333333ttty',       # old
    # 'X-JK-UDID': '000001111112222223333333ttthhh',       #
'X-JK-UDID': '000001111112222223333333ttthh11',       # new
# 'X-JK-UDID': '000001111112222223333333ttthhh222',    #
}

# response = requests.get(host_name + '/itemBasedRecommend', params=params)
# response = requests.get(host_name + '/get_running_info', params=params)
# response = requests.get(host_name + '/oldOnlineRecommend', params=params)
response = requests.get(host_name + '/item/recommend', headers=headers, params=params)


print('cost time: ', time.time()-start_t)
print(response.text)
# print(json.loads(response.text)[0]['7A009D92-CA59-4EEF-80F5-AE0BCA507E10'])


# start_t = time.time()
# url_get_all = host_name + '/callcenter/v1/get_all_possibility'
# response = requests.get(url_get_all, params={}, timeout=150)
# print('get by all response.text length ', len(response.text))
# print('get all data cost time: ', time.time()-start_t)
#
# with open('./response_text.txt', 'w') as file:
#     file.write(response.text)


# start_t = time.time()
# response = requests.get('http://172.21.57.127:8888' + '/get_possibility/get_possibility_by_user_ids', params=params)
# print('cost time 1: ', time.time()-start_t)
# print(response.text)
# print(response.status_code)
# print(response.content)


# json(response.text)

# print(json.dumps({'a': 1, 'b': 2}))



