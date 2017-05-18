# http://blog.csdn.net/mr__fang/article/details/7089581
# http://blog.sina.com.cn/s/blog_5357c0af01019gjo.html
# http://stackoverflow.com/questions/25144175/append-rows-in-excel-using-xlwt-in-python
# http://stackoverflow.com/questions/14587271/accessing-worksheets-using-xlwt-get-sheet-method

import xlrd
import xlwt
from xlutils.copy import copy
import itertools

import random
import pymongo
from datetime import datetime

class ExcelWriter(object):
    def __init__(self, file_name=None, mode='w'):  #'w' for write, 'a' for append
        self.file_name = file_name
        if mode=='w':
            self.workbook = xlwt.Workbook()
        else:
            rb = xlrd.open_workbook(file_name, formatting_info=True)
            self.workbook = copy(rb)
    
    #----------------------------------------------------------------------
    def write_to_cell(self, row, col, cell_content, sheet_name='Sheet1'):
        self.table = self.get_sheet_by_name(sheet_name)
        if self.table is None:
            self.table = self.workbook.add_sheet(
                              sheet_name, cell_overwrite_ok=True)
        self.table.write(row, col, cell_content)
        print('get here')
    
    #----------------------------------------------------------------------
    def get_sheet_by_name(self, sheet_name):
        """Get a sheet by name from xlwt.Workbook, a strangely missing method.
        Returns None if no sheet with the given name is present.
        """
        # Note, we have to use exceptions for flow control because the
        # xlwt API is broken and gives us no other choice.
        try:
            for idx in itertools.count():
                sheet = self.workbook.get_sheet(idx)
                if sheet.name == sheet_name:
                    return sheet
        except IndexError:
            return None        
    
    def save(self, file_name=None):
        if file_name is None:
            file_name = self.file_name
        if not file_name.endswith('.xls'):
            file_name += '.xls'
        self.workbook.save(file_name)
        self.file_name = file_name    


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

if __name__=='__main__':
#    xls_writer = ExcelWriter('excel_data.xls', 'a')
#    xls_writer.write_to_cell(1, 1, 'Hello_World')
#    xls_writer.write_to_cell(2, 1, '3, 1 Hello_Cell')
#    xls_writer.write_to_cell(2, 1, 'Hello_Content', 'Content')
#    xls_writer.write_to_cell(10, 10, 'New_data')
    
#    xls_writer.write_to_cell(11, 11, 'Append_New_data')
#    xls_writer.write_to_cell(5, 5, 'Append_In_Content', 'Content')
#    xls_writer.save('excel_data111.xls')
    
    rb = xlrd.open_workbook('excel_data.xls', formatting_info=True)
    wb = copy(rb)
    sheet = wb.get_sheet(0)
    sheet.write(0, 1, 'Modified')
#    Sheet1 = wb.add_sheet('Sheet5')
    wb.save('excel_data.xls')
    
#    for idx in itertools.count():
#        print('idx is ', idx)
#        if idx >= 33:
#            break
    
#    data = xlrd.open_workbook('excelFile.xlsx', 'w')
#    table = data.sheets()[0]               # 通过索引顺序获取
#    table = data.sheet_by_index(0)         # 通过索引顺序获取
#    table = data.sheet_by_name(u'Sheet1')  # 通过名称获取
#    
#    nrows = table.nrows
#    ncols = table.ncols
#    
#    for i in range(nrows):
#        print(table.row_values(i))

    #file = xlwt.Workbook()   #注意这里的Workbook首字母是大写，无语吧    
    #table = file.add_sheet('sheet name')
    #table = file.add_sheet('sheet name',cell_overwrite_ok=True)    #
    #for row in range(10):
    #    for col in range(5):
    #        table.write(row, col, 'row: %d col: %d  value: %d'%(
    #                    row, col, random.randint(0, 100)))
    #        
    #file.save('demo.xls')    
#    print('###########################################################')
#    
#    start_dt = datetime(2017, 3, 30, 19, 0, 0)
#    end_dt = datetime(2017, 3, 31, 19, 0, 0)
#    storeTradingResults("DoubleEmaDemo_TSG", start_dt, end_dt)
#    storeTradingResults("DoubleEmaDemo_TSG111", start_dt, end_dt)








