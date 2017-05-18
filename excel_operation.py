# http://blog.csdn.net/mr__fang/article/details/7089581
import xlrd
import xlwt
import random
import pymongo
from datetime import datetime

def storeTradingResults(strategy_name, start_dt, end_dt):
    file = xlwt.Workbook()
    #table = file.add_sheet(u'成交比对')
    table = file.add_sheet(u'成交比对', cell_overwrite_ok=True)

    client = pymongo.MongoClient('mongodb://localhost:27017/')
    mydb = client['VnTrader_Actions']
    print('Actions_Signal')
    cursor = mydb.Actions_Signal.find({"strategy_name": strategy_name})
    row = 1
    for document in cursor:
        if start_dt <= document['datetime'] <= end_dt:
            table.write(row, 1, document['symbol'])
            table.write(row, 2, document['direction'])
            table.write(row, 3, document['volume'])
            table.write(row, 4, document['price'])
            table.write(row, 5, document['datetime'].strftime('%Y-%m-%d %H:%M:%S'))
            print(document)
            row += 1
    
    row = 1
    print('Actions_Trade')
    cursor = mydb.Actions_Trade.find({"strategy_name": strategy_name})
    for document in cursor:
        if start_dt <= document['datetime'] <= end_dt:
            table.write(row, 7, document['symbol'])
            table.write(row, 8, document['direction'])
            table.write(row, 9, document['volume'])
            table.write(row, 10, document['price'])
            table.write(row, 11, document['datetime'].strftime('%Y-%m-%d %H:%M:%S'))
            print(document)
            row += 1
    
    results_file_name = strategy_name + '_' + 'trading_results.xls'
    file.save(results_file_name)

data = xlrd.open_workbook('excelFile.xlsx', 'w')
table = data.sheets()[0]               # 通过索引顺序获取
table = data.sheet_by_index(0)         # 通过索引顺序获取
table = data.sheet_by_name(u'Sheet1')  # 通过名称获取

nrows = table.nrows
ncols = table.ncols

for i in range(nrows):
    print(table.row_values(i))

#file = xlwt.Workbook()   #注意这里的Workbook首字母是大写，无语吧

#table = file.add_sheet('sheet name')
#table = file.add_sheet('sheet name',cell_overwrite_ok=True)
#
#for row in range(10):
#    for col in range(5):
#        table.write(row, col, 'row: %d col: %d  value: %d'%(
#                    row, col, random.randint(0, 100)))
#        
#file.save('demo.xls')

print('###########################################################')

start_dt = datetime(2017, 3, 30, 19, 0, 0)
end_dt = datetime(2017, 3, 31, 19, 0, 0)
storeTradingResults("DoubleEmaDemo_TSG", start_dt, end_dt)
storeTradingResults("DoubleEmaDemo_TSG111", start_dt, end_dt)








