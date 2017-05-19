#encoding: utf-8

import pandas as pd
import numpy as np
import pymongo
import time
import sys

sys.path.append("D:\\Tinysoft\\Analyse.NET\\")
#################################################################################################################

import TSLPy3 as ts

def get_tradingDay_list(start_date, end_date):
    a = ts.RemoteCallFunc('getTradingDay', [start_date, end_date], {})
    return [d.decode() for d in a[1]]

def get_MoneyInFlow_Last30Min_fromTS(date_str):
    a = ts.RemoteCallFunc('get_MoneyInFlow_Last30Min', [date_str], {})
    return a[1]

def get_moneyInflow_by_date(date_str, store_to_csv=True):
    results = get_MoneyInFlow_Last30Min_fromTS(date_str)

    
    df = pd.DataFrame(results)
    df = pd.pivot_table(df, values=[b'x_amount'], 
                            index=[b'stock_id'], columns=[b'date'])

    df_new = pd.DataFrame()
    df_new[u'超大单'] = (df[df>=1000000].count(axis=1) - 
                      df[df<=-1000000].count(axis=1))
    df_new[u'大单'] = (df[(df<1000000) & (df>=200000)].count(axis=1) -
                    df[(df>-1000000) & (df<=-200000)].count(axis=1))
    df_new[u'中单'] = (df[(df<200000) & (df>=40000)].count(axis=1) -
                    df[(df>-200000) & (df<=-40000)].count(axis=1))
    df_new[u'小单'] = (df[(df<40000) & (df>=0)].count(axis=1) -
                    df[(df>-40000) & (df<=0)].count(axis=1))
    df_new.index = df_new.index.map(lambda x: x.decode('utf-8'))    
    df_new.index.name = 'stock_id'
    
    if store_to_csv==True:
        df_new.to_csv(date_str+'.csv', encoding='utf-8', 
                      index_label=df_new.index.name)
    
    return df_new


def storeToMongoDB(date_str, df):
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    mydb = client['Mesess_Strategy_Monitor']
    collection = mydb['testing']
    collection.insert_one({'date_str': date_str, 'df': df.to_json()})   


def getFromMongoDB(date_str):
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    mydb = client['Mesess_Strategy_Monitor']
    collection = mydb['testing']
    doc= collection.find_one({'date_str': date_str})
    df = pd.read_json(doc['df'])
    return df

if __name__=='__main__':
#    print(get_tradingDay_list('2017-3-12', '2017-4-5'))
    begin_t = time.time()
    df1 = get_moneyInflow_by_date('2017-4-5')
    end_t = time.time()
    print('cost time1: %.7f sec'%(end_t-begin_t))
#    storeToMongoDB('2017-4-5', df)
    
    begin_t = time.time()
    df2 = getFromMongoDB('2017-4-5')
    end_t = time.time()
    print('cost time2: %.7f sec'%(end_t-begin_t))
    
    if df1==df2:
        print('the same dataframe')
    else:
        print('different dataframe')
#    print(df)
        
    
#    for date_str in get_tradingDay_list('2017-3-12', '2017-4-5'):
#        start_t = time.time()
#        df = get_moneyInflow_by_date(date_str)
#        end_t = time.time()
#        print('date_str: %s cost time is %.7f sec'%(date_str, end_t-start_t))
    
#    df = pd.DataFrame.from_csv('2017-03-31.csv', encoding='utf-8')
#    print(df)
    
