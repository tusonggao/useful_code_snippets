#encoding: utf-8
import pandas as pd
import numpy as np
import sys

sys.path.append("D:\\Tinysoft\\Analyse.NET\\")
#################################################
#################################################
#################################################

import TSLPy3 as ts

def get_MoneyInFlow_Last30Min_fromTS(date_str):
    a=ts.RemoteCallFunc('get_MoneyInFlow_Last30Min', [date_str], {})
    data = a[1]
    return data

def get_moneyInflow_by_date(date_str):
    results = get_MoneyInFlow_Last30Min_fromTS(date_str)
    
    df = pd.DataFrame(results)
    df = pd.pivot_table(df, values=[b'x_amount'], 
                           index=[b'stock_id'], columns=[b'date'])
#    df = pd.pivot_table(df, values=[b'x_amount'], 
#                           index=[b'stock_id'])
    df[u'超大单'] = (df[df>=1000000].count(axis=1) - 
                      df[df<=-1000000].count(axis=1))
    df[u'大单'] = (df[(df<1000000) & (df>=200000)].count(axis=1) -
                    df[(df>-1000000) & (df<=-200000)].count(axis=1))
    df[u'中单'] = (df[(df<200000) & (df>=40000)].count(axis=1) -
                    df[(df>-200000) & (df<=-40000)].count(axis=1))
    df[u'小单'] = (df[(df<40000) & (df>=0)].count(axis=1) -
                    df[(df>-40000) & (df<=0)].count(axis=1))
                    
#    df.index = df.index.map(lambda x: x.decode('utf-8'))
    df.index.map(lambda x: x.decode('utf-8'))
#    df = df.loc[:, [u'超大单', u'大单', u'中单', u'小单']]
#    df = df[[u'超大单', u'大单', u'中单', u'小单']].copy()
    return df.loc[:, [u'超大单', u'大单', u'中单', u'小单']]
#    return df

if __name__=='__main__':
#    df = get_moneyInflow_by_date('2017-3-31')
#    print(df)
#    print(df.ix[[b'SZ300636']])
    
    data = pd.DataFrame(np.arange(6).reshape((2, 3)),
               index=pd.Index(['Ohio', 'Colorado'], name='state'),
               columns=pd.Index(['one', 'two', 'three'], name='number'))
#    print(data)
    result = data.stack()

    df = pd.DataFrame(data={'Province' : ['ON','QC','BC','AL','AL','MN','ON'],
                           'City' : ['Toronto','Montreal','Vancouver','Calgary','Edmonton','Winnipeg','Windsor'],
                           'Sales' : [13,6,16,8,4,3,1]})
    print(df)
    table = pd.pivot_table(df, values=['Sales'], index=['Province'], 
                           columns=['City'], aggfunc=np.sum, margins=True)
    print(table)
    
#    print(result)
#    df.to_excel('original_111.xlsx')
    

    
    
    
    

    
    
    
    
    
