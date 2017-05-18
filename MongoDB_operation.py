# -*- coding: utf-8 -*-
from __future__ import print_function
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd
import numpy as np
import time

from datetime import datetime
import numpy as np
from random import random
import collections
import pymongo
import sys

sys.path.append("D:\\Tinysoft\\Analyse.NET\\")

import TSLPy3 as ts

#ts.DefaultConnectAndLogin("test") #调用函数登陆

#data = ts.RemoteExecute("return 1;",{}) #执行语句
#print(str(data[2], encoding='gbk'))

ts.ConnectServer("tsl.tinysoft.com.cn", 443)
dl = ts.LoginServer("mesesfund", "12345678")  #Tuple(ErrNo,ErrMsg) 登陆用户
if dl[0]==0 :
    print("登陆成功")
    print("服务器设置:", ts.GetService())
    ts.SetComputeBitsOption(64) #设置计算单位
    print("计算位数设置:", ts.GetComputeBitsOption())
    data = ts.RemoteExecute("return 'return a string';",{}) #执行一条语句
    print("数据:", data)
    ts.Disconnect() #断开连接
else:
    print(dl[1])

def get1MinDataByDate(stockID, date_str):
    a=ts.RemoteCallFunc('getMinMarketDataByDate', [stockID, date_str], {})
    data = a[1]
#    return data
    results = []
    for d in data:
#        time_str = d[b'date'].split()[1].decode()
        time_str = d[b'date']
        vol = d[b'vol']
        open_price = d[b'open']
        high_price = d[b'high']
        low_price = d[b'low']
        close_price = d[b'close']
        results.append((time_str, vol, open_price, high_price, 
                        low_price, close_price))
    return results

def get1MinDataTwoByDate(stockID, date_str1, date_str2):
    a=ts.RemoteCallFunc('getMinMarketDataByTwoDate', 
                        [stockID, date_str1, date_str2], {})
    data = a[1]
    results = []
    for d in data:
        time_str = d[b'date']
        vol = d[b'vol']
        open_price = d[b'open']
        high_price = d[b'high']
        low_price = d[b'low']
        close_price = d[b'close']
        results.append((time_str, vol, open_price, high_price, 
                        low_price, close_price))
    return results

def operateMongoDB():
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    mydb = client['VnTrader_1Min_Db']
#    mydb.drop_collection('Cu00000')
#    mydb.drop_collection('Cu_ZL_LX')
#    my_collection = mydb['Cu_ZL_LX']
    mydb.drop_collection('ZL000001')
    my_collection = mydb['ZL000001']
    	
#    my_collection = mydb['IF0000']
#    my_collection = mydb['IF0000_NEW']
    
    # results = get1MinDataByDate('ZL000003', '2017-3-12')
#    results = get1MinDataTwoByDate('ZL000003', '2010-3-1', '2017-3-12')
    results = get1MinDataTwoByDate('ZL000001', '2010-3-1', '2017-3-12')
#    results = get1MinDataTwoByDate('IF00', '2010-4-10', '2017-3-12')
#    print('results is ', results)
    
    count = 0
    for d in results:
        count += 1
        if count%10000==0:
            print('current count is ', count)
        try:
            dt = datetime.strptime(d[0].decode(), '%Y-%m-%d %H:%M:%S')
        except ValueError:
            dt = datetime.strptime(d[0].decode()+' 00:00:00', '%Y-%m-%d %H:%M:%S')
        res = my_collection.insert_one(
                        { "datetime" : dt,
                          "symbol": 'Cu_ZL_LX',
                          "volume" : d[1],
                          "open" : d[2],
                          "high" : d[3],
                          "low" : d[4],
                          "close" : d[5]
                        }
                    )
    
    
    startDate = datetime(2010, 3, 6, 8, 21, 0)
    endDate = datetime(2017, 4, 1, 15, 55, 0)
    
    flt = {'datetime':{'$gte': startDate,
                       '$lt': endDate}}      
#                       
#    begin_t = time.time()
#    initCursor = my_collection.find(flt)
#    end_t = time.time()
#    print 'cost time1 %d seconds'%(end_t-begin_t)
#    print 'initCursor[0] is ', initCursor[0]
    
#    begin_t = time.time()
#    initData = []
#    for d in initCursor:
#        initData.append(d)
#        
#    print('len of initData is ', len(initData))
#    end_t = time.time()
#    print 'cost time2 %d seconds'%(end_t-begin_t)
    
#    print 'len of initData is ',  len(initData)

def testMongoDB():
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    mydb = client['MongoDB_testing']
    mydb.drop_collection('testing_db')
    my_collection = mydb['testing_db']
    	
    aaa = np.array([random() for i in range(20)])
    
    my_collection.insert_one(
                        { "datetime" : datetime.now,
                          "array" : aaa
                        }
                    )
                    
def ouputTestMongoDB():
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    mydb = client['MongoDB_testing']
    getoutput_collections = mydb['testing_db']
    
    for collection in getoutput_collections:
        print(collection['datetime'], collection['array'])
    


def findDiff():
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    mydb = client['VnTrader_1Min_Db']
    my_collection1 = mydb['IF0000']
    my_collection2 = mydb['IF0000_NEW']
    
    startDate = datetime(2010, 3, 6, 8, 21, 0)
    endDate = datetime(2017, 4, 1, 15, 55, 0)
    
    flt = {'datetime':{'$gte': startDate,
                       '$lt': endDate}}      
#                       
#    begin_t = time.time()
    initCursor1 = my_collection1.find(flt)
    initCursor2 = my_collection2.find(flt)
#    end_t = time.time()
#    print 'cost time1 %d seconds'%(end_t-begin_t)
#    print 'initCursor[0] is ', initCursor[0]
    
#    begin_t = time.time()
    count = 0
    dt_set_1 = set()
    for d in initCursor1:
        count += 1
        if count%100000==0:
            print('In initCursor1 count is ', count)
        dt_set_1.add(d['datetime'])
        
    count = 0
    dt_set_2 = set()
    for d in initCursor2:
        count += 1
        if count%100000==0:
            print('In initCursor2 count is ', count)
        dt_set_2.add(d['datetime'])
        
    print('len dt_set_1 is ', len(dt_set_1))
    print('len dt_set_2 is ', len(dt_set_2))
    if len(dt_set_1) >= len(dt_set_2):
        print('len(dt_set_1-dt_set_2) is ', len(dt_set_1-dt_set_2))
        print('dt_set_1 - dt_set_2 is ', dt_set_1-dt_set_2)
    else:
        print('len(dt_set_2-dt_set_1) is ', len(dt_set_2-dt_set_1))
        print('dt_set_2 - dt_set_1 is ', dt_set_2-dt_set_1)
        

if __name__=='__main__':
    testMongoDB()   
    ouputTestMongoDB()
#    aaa = set()
#    aaa.add(3)
#    aaa.add(4)
#    print(aaa, len(aaa))
#    findDiff()
#    operateMongoDB()
#    results = get1MinDataByDate('IF00', '2017-3-6')
#    print('len of results is ', len(results))
#    print('content of results is ', results)
    
    
    
    
    
