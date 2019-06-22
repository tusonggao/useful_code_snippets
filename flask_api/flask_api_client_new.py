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
    'user_ids': ['99C0972D-B1EC-4000-824F-462791D433F7', '7A009D92-CA59-4EEF-80F5-AE0BCA507E10']
}


# host_name = 'http://ai-service.idc.jianke.com/callcenter-reco'
# response = requests.get(host_name + '/callcenter/v1/get_possibility_by_user_ids', params=params)
# print(response.text)
# print(type(json.loads(response.text)))
# print(json.loads(response.text)[0]['7A009D92-CA59-4EEF-80F5-AE0BCA507E10'])


response = requests.get('http://172.21.57.127:8888' + '/get_possibility/get_possibility_by_user_ids', params=params)
print(response.text)
print(response.status_code)
print(response.content)

# json(response.text)




