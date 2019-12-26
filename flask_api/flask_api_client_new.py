import time
import requests
import json
import requests
import string
import numpy as np
from collections import Counter

# data = {
#     'name': 'tom',
#     'age': 20
# }

# response = requests.get('http://127.0.0.1:8080/test_args', params=data)
# print(response.text)

# params = {
#     'user_ids': ['99C0972D-B1EC-4000-824F-462791D433F7',
#                  '7A009D92-CA59-4EEF-80F5-AE0BCA507E10',
#                  '413E648D-CA1A-4684-862A-24B6FC0024C7',
#                  '5C72E70A-0D86-4E61-8129-1AC6ADB50B5B',
#                  '883A07D0-CC66-4D50-AEFF-66386FDC1575'],
#     'name': ['tsg', 'hu']
# }

# params = {
#     'user_ids': ['7A009D92-CA59-4EEF-80F5-AE0BCA507E10'],
#     'name': ['tsg', 'hu']
# }

# host_name = 'http://ai-service.idc.jianke.com/callcenter-reco'
# host_name = 'http://127.0.0.1:5000'
# host_name = 'http://detail-page-item-based-recommend.dev.jianke.com'
# host_name = 'http://detail-page-item-based-recommend.internal.jianke.com'
# host_name = 'http://detail-page-old-online-recommend.dev.jianke.com'
# host_name = 'http://detail-page-old-online-recommend.internal.jianke.com'
# host_name = 'http://abtest-service.tst.jianke.com'
host_name = 'http://abtest-service.internal.jianke.com'

params = {
          # 'product_code': 551997,  # {"outcome":["11807","208850","182769","769392","551409"],"algorithm":"NEW_ITEM_BASED"}
                                   # {"outcome":["769392","208850","182769","11807","11808"],"algorithm":"NEW_ITEM_BASED"} {"outcome":["20190730131700"]
                                   # {"outcome":["769392","208850","182769","11807","11808"],"algorithm":"NEW_ITEM_BASED"} {"outcome":["20190801193825"]
                                   # {"outcome":["182769","208850","11807","11808","769392"],"algorithm":"NEW_ITEM_BASED"} {"outcome":["20190805160440"]
                                   # {"outcome":["208850","182769","11807","11808","551409"], "algorithm":"NEW_ITEM_BASED"} {"outcome":["20190807155851"]
                                   # {"outcome":["208850","769392","11807","182769","199516"], "algorithm":"NEW_ITEM_BASED"} {"outcome":["20190809180407"]
                                   # {"outcome":["3772","675","1555","11929","269069"],"algorithm":"NEW_ITEM_BASED"} {"outcome":["20190820174752"]
                                   # {"outcome":["269069","3772","4450","9647","11929"],"algorithm":"NEW_ITEM_BASED"}  last_read_data_datetime: 2019-08-29 19:17:38
                                   # {"outcome":["269069","1555","7159","3772","4450"],"algorithm":"NEW_ITEM_BASED"}  {"outcome":["20190901155952"]
                                   # {"outcome":["769392","208850","11807","182769","551409"],"algorithm":"NEW_ITEM_BASED"}
                                   # {"outcome":["551409","11807","551455","853642","208850"],"algorithm":"NEW_ITEM_BASED"}  ["20191209152753"]

         # 551997 769392#11807#208850#551409#182769#199516#11808#251645#551455#53416#226388#597721#179016#227350#6414#  2019.9.3  14:33  from table
         # 551997 769392#208850#11807#182769#551409#11808#199516#551455#53416#251645#226388#227350#6414#597721#849093#853642# 2019.9.4  21:33  from table
         #  'product_code': 759860,  # {"outcome":["9058","536009","279770","6465","8757"],"algorithm":"NEW_ITEM_BASED"}  错误的
                                   # {"outcome":["823976","511379","511399","511378","602474"],"algorithm":"NEW_ITEM_BASED"} {"outcome":["20190903211934"], 2019-09-04 9:15查询 开始恢复正常
                                   # {"outcome":["511399","823976","511379","602474","511378"],"algorithm":"NEW_ITEM_BASED"}   ["20191209152753"]
                                   # {"outcome":["511379","823976","511399","602474","511378"],"algorithm":"NEW_ITEM_BASED"}   ["20191210152532"]
         # 759860数据表查询 823976#511379#511399#511378#602474#602559#602465#511356#562081#602476#333594#511363#489861    2019.9.3 14:33  from table

         'product_code': 11692,  # {"outcome":["710515","160028","323977","269069","764808"],"algorithm":"NEW_ITEM_BASED"}
                                 # {"outcome":["710515","269069","764808","675","207422"],"algorithm":"NEW_ITEM_BASED"}
                                 # {"outcome":["710515","269069","764808","207422","767968"],"algorithm":"NEW_ITEM_BASED"}
                                 # {"outcome":["710515","269069","675","207422","805082"],"algorithm":"NEW_ITEM_BASED"}
                                 # {"outcome":["710515","269069","207422","764808","675"],"algorithm":"NEW_ITEM_BASED"}
                                 # {"outcome":["710515","269069","764808","207422","11929"],"algorithm":"NEW_ITEM_BASED"}
                                 # {"outcome":["710515","269069","764808","207422","11929"],"algorithm":"NEW_ITEM_BASED"} "20191107162527"
                                 # {"outcome":["710515","269069","764808","207422","11929"],"algorithm":"NEW_ITEM_BASED"}
                                 # {"outcome":["710515","269069","764808","767968","207422"],"algorithm":"NEW_ITEM_BASED"} "20191111161912"
                                 # {"outcome":["710515","269069","767968","207422","675"],"algorithm":"NEW_ITEM_BASED"}  20191120152311
                                 # {"outcome":["269069","767968","207422","764808","805082"],"algorithm":"NEW_ITEM_BASED"}  ["20191202152925"]
                                 # {"outcome":["269069","805082","207422","767968","764808"],"algorithm":"NEW_ITEM_BASED"}  ["20191205152330"]
                                 # {"outcome":["269069","207422","764808","767968","675"],"algorithm":"NEW_ITEM_BASED"}  ["20191208152242"]
                                 # {"outcome":["767968","764808","269069","675","207422"],"algorithm":"NEW_ITEM_BASED"}  ["20191209152753"]
                                 # {"outcome":["767968","269069","764808","675","207422"],"algorithm":"NEW_ITEM_BASED"}  ["20191210152532"]
                                 # {"outcome":["767968","269069","675","764808","805082"],"algorithm":"NEW_ITEM_BASED"}  ["20191215153006"]
                                 # {"outcome":["767968","269069","675","764808","805082"],"algorithm":"NEW_ITEM_BASED"}  ["20191215153006"]
                                 # {"outcome":["675","767968","269069","764808","805082"],"algorithm":"NEW_ITEM_BASED"}  ["20191223154112"]
                                 # {"outcome":["269069","767968","675","838349","805082"],"algorithm":"NEW_ITEM_BASED"}  ["20191225152357"]

          # 'product_code': 999999999,    # {"outcome":["269069","805082","207422","767968","764808"],"algorithm":"NEW_ITEM_BASED"}
          # 'user_id': '0011tttyy88oo99uu'
}

# params = {'product_code': '286002',
#           'user_id': '0011tttyy88oo99333'}  # item-based

start_t = time.time()

# headers = {
#  'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.108 Safari/537.36',
#  'X-JK-UDID': '000001111112222223333333ttty' #old
# }

headers = {
    # 'X-JK-UDID': '3332255555444iii',   #
    # 'X-JK-UDID': '000001111112222223333333tttuui',   #
    'X-JK-UDID': '000001111112222223333333tttuuz',   # new
    # 'X-JK-UDID': '000001111112222223333333tttuuu',     # new
    # 'X-JK-UDID': '000001111112222223333333tttw',       # old {"outcome":["1925","11208","2060","54934","54905"],"algorithm":"NEW_ITEM_BASED"}
    # 'X-JK-UDID': '000001111112222223333333ttte',       #  new {"outcome":["3394","28","222","357","1990"],"algorithm":"OLD_ON_LINE"}
    # 'X-JK-UDID': '000001111112222223333333ttty',       # new
    # 'X-JK-UDID': '000001111112222223333333ttthhh',     # new
    # 'X-JK-UDID': '2642F990-CA9D-4227-89D3-09927DD3F207', # new
    # 'X-JK-UDID': '000001111112222223333333ttthh11',    # new
    # 'X-JK-UDID': '000001111112222223333333ttthhh222',    # new
}


# cost time:  0.011009693145751953
# {
#   "outcome": [
#     "20190820174752"
#   ]
# }

# response = requests.get(host_name + '/itemBasedRecommend', params=params)
# response = requests.get(host_name + '/emptyRecommend', params=params)
# response = requests.get(host_name + '/get_running_info', params=params)
# last_read_data_datetime: 2019-08-22 19:04:40 len of item_based_recommend_outcome: 16251
# response = requests.get(host_name + '/oldOnlineRecommend', params=params)
response = requests.get(host_name + '/item/recommend', headers=headers, params=params)

print(response.text)
print('time cost: ', time.time()-start_t)

# algo_lst = []
# for i in range(500):
#     all_candidate_chs = string.digits + string.ascii_letters
#     rand_udid = ''.join(list(np.random.choice(list(all_candidate_chs), 22)))
#     headers = {'X-JK-UDID': rand_udid}
#     print('rand_udid is ', rand_udid)
#
#     start_t = time.time()
#     response = requests.get(host_name + '/item/recommend', headers=headers, params=params)
#     print('cost time: ', time.time()-start_t)
#     print(response.text)
#     response = json.loads(response.text)
#     # print('response is ', response)
#     print('response[algorithm]', response['algorithm'])
#     algo_lst.append(response['algorithm'])
#     time.sleep(0.3)
#
# print('Counter of algo_lst is ', Counter(algo_lst))

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




