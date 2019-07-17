# 使用post方式时，数据放在data或者body中，不能放在url中，放在url中将被忽略。

# select count(*) from jkbd.quanzi_user_force_recentbuy;   8940  原来14041  明天应该是最新的   --8539

import time
import requests
import json
import http.client    #修改引用的模块

print('start running')
start_t = time.time()



#------------------------------------------------------------------------------


# c = http.client.HTTPConnection('172.21.57.127', 7777)  #
# c = http.client.HTTPConnection('127.0.0.1', 5000)  # 7766
# c = http.client.HTTPConnection('users-cycles-recommend.dev.jianke.com', 80)  # 7766  8823
c = http.client.HTTPConnection('users-cycles-recommend.internal.jianke.com', 80)  #7766  7766


start_t = time.time()

headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
c.request('GET', '/get_recentbuy_force', '{}', headers)  # 8539  #8528
# c.request('GET', '/get_recommend/A9343E00-EDC9-40EA-91FA-D41BE979F970', '{}', headers)
# c.request('GET', '/get_recommend/88B88334-B470-4E7B-8BAD-F99FFFBEA284', '{}', headers) # for ceshi  88B88334-B470-4E7B-8BAD-F99FFFBEA284
s = c.getresponse().read().strip()
print('request cost time: ', time.time()-start_t)

print('s is ', s)
ddd = json.loads(s)

print('ddd is ', len(ddd))

#------------------------------------------------------------------------------

# response = requests.get('http://users-cycles-recommend.dev.jianke.com/get_recentbuy_force')
# response = requests.get('https://fe-acgi.jianke.com/v2/products/475907?manual=true&net=WiFi&sid=35Fr5WIuCfin')
# print(response.status_code)  # 打印状态码
# print(response.url)          # 打印请求url
# print(response.headers)      # 打印头信息
# print(response.cookies)      # 打印cookie信息
# print(response.text)         # 以文本形式打印网页源码

# ddd = json.loads(response.text)
#
# if 'success' in ddd and ddd['success']==False:
#     print('success is false')
# else:
#     print('ddd[data] is ', ddd['mainData'])

#------------------------------------------------------------------------------

def split_all_text(response_text):
    idx_right = -1
    outcome = {}

    while True:
        start_idx = idx_right + 1
        idx_left = response_text.find('"', start_idx)
        if idx_left==-1:
            break

        start_idx = idx_left + 1
        idx_right = response_text.find('"', start_idx)
        if idx_right==-1:
            break

        buy_user_id = response_text[idx_left+1:idx_right]

        start_idx = idx_right + 1
        idx_left = response_text.find('"', start_idx)
        if idx_left==-1:
            break

        start_idx = idx_left + 1
        idx_right = response_text.find('"', start_idx)
        if idx_right==-1:
            break

        score = float(response_text[idx_left+1:idx_right])
        outcome[buy_user_id] = score

    return outcome


def check_upload_successful(check_num=5):
    print('in check_upload_successful()')
    host_name = 'http://ai-service.idc.jianke.com/callcenter-reco'
    buy_user_id_lst = ['D908B16B-9020-446A-AFC1-12AD87E905A4']

    print('buy_user_id_lst is ', buy_user_id_lst)

    start_t = time.time()
    url_get_by_ids = host_name + '/callcenter/v1/get_possibility_by_user_ids'
    response = requests.get(url_get_by_ids, params={'user_ids': buy_user_id_lst})
    end_t = time.time()
    print('get by ids response.text', response.text)
    print('total cost time: ', time.time() - start_t)
    outcome = split_all_text(response.text)
    print('outcome is ', outcome)
    # outcome is  {'F4821CE7-416D-4DE2-88D4-E78FD964FE23': 0.13295731509276573, '24EE22A2-E2D4-481E-B37C-A1B3EDE6E279': 0.06812750759063789, '8378DBF4-9D3F-44B7-888A-36CF8879C097': 0.03494671547084243, '9B2BEF21-09DE-4C00-B7FC-AE302D556C65': 0.07208371028420489}

    # url_get_all = host_name + '/callcenter/v1/get_all_possibility'
    # print('get start...')
    # response = requests.get(url_get_all, params={}, timeout=150)
    # print('get end')
    # end_t = time.time()
    # print('get by all response.text length ', len(response.text))
    # outcome = split_all_text(response.text)
    # print('all possibility outcome len is ', len(outcome))

    print('get all cost time', end_t - start_t)


check_upload_successful()



