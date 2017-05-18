    def initConnectToDB(self): # added by tusonggao
        self.dbConn = pymysql.connect(use_unicode=True, charset="utf8", 
                                      host='localhost', user='root',
                                      passwd='', db='mysql')
        self.dbCur = self.dbConn.cursor() 
        self.tableCreated = False   



def insertToDB(self, symbol, orderType, price, volume, strategy_name):
        current_time =  time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        self.logging("line1 %s %s %s %s %s"%(
                              str(symbol), unicode(orderType), 
                              str(price), str(volume), strategy_name))
        
#        symbol = symbol.encode('utf-8').decode('latin1')
#        orderType = orderType.encode('utf-8').decode('latin1')
#        strategy_name = strategy_name.encode('utf-8').decode('latin1')
        self.logging("line2 %s %s %s %s %s"%(
                              str(symbol), unicode(orderType), 
                              str(price), str(volume), strategy_name))
        
        if not self.tableCreated:
            self.dbCur.execute('''create table if not exists ctp_signals(
                       symbol varchar(10) not null,
                       orderType varchar(6) not null,                       
                       price float not null,
                       volume int not null,
                       strategy_name varchar(50) not null,
                       updateTime datetime not null)ENGINE=Myisam DEFAULT CHARSET=utf8''')
            self.tableCreated = True
            
        self.dbCur.execute('insert into ctp_signals(symbol, orderType, price, volume, '
                    'strategy_name, updateTime) values("%s","%s", %f, %d, "%s", "%s")'%(
                    symbol, orderType, price, volume, strategy_name, current_time))
                    
        self.dbConn.commit()
            
#        print(" direction text is ", direction, "long is ", DIRECTION_LONG, "short is",  DIRECTION_SHORT) #tusonggao
#                     
#        cur.execute('insert into ctp_positions(symbol, exchange, name, direction, '
#                    'position, price, updateTime) select "%s","%s", "%s", "%s", %d, %f, "%s" '
#                    'from DUAL where not exists(select * from ctp_positions where '
#                    'ctp_positions.symbol="%s" and ctp_positions.direction="%s") '%(symbol, 
#                    exchange, vtSymbol, direction, pos, price, current_time, symbol, direction))
#              
#        cur.execute('update ctp_positions set symbol="%s", exchange="%s", name="%s", '
#                    'direction="%s", position=%d, price=%f, updateTime="%s" where '
#                    'symbol="%s" and direction="%s" '%(symbol, exchange, vtSymbol, 
#                    direction, pos, price, current_time, symbol, direction))
              
                       
#        cur.execute('''insert into ctp_positions(cl_name, cl_cycle, cl_days, cl_class, total_profit, month_profit, '''
#              '''day_profit, win_profit, lose_profit, profit_line, create_time, update_time) select '%s', '%s', '%s', '%s','''
#              '''%f, %f, %f, %f, %f, '%s','%s', '%s' from DUAL where not exists(select * from dsjcl_list where dsjcl_list.cl_name='%s') '''%(cl_name, \
#              self.period, cl_days, self.getInstrumentIdStr(), total_profit, month_profit, day_profit, \
#              win_profit, lose_profit, profit_line_text, current_time, current_time, cl_name))
#              
#        cur.execute('''update dsjcl_list set cl_cycle='%s', cl_days='%s', cl_class='%s', total_profit=%f, month_profit=%f, '''
#              '''day_profit=%f, win_profit=%f, lose_profit=%f, profit_line='%s', update_time='%s' where cl_name='%s' '''%(self.period, cl_days,\
#              self.getInstrumentIdStr(), total_profit, month_profit, day_profit, win_profit, lose_profit, profit_line_text, current_time, cl_name))     
    
