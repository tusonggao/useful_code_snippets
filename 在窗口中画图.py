class StockPositionValueMplWidget(MatplotlibWidget):
    def plot(self, recorder):
        if not hasattr(self, 'stkId_list'):
            self.stkId_list = recorder.stkId_list[:]
        bar_width = 1
        y_pos = np.arange(len(self.stkId_list))*3.5
        value_list = [recorder.stockPositionsDict[stk_id]*GlobalVars.rt_prices[stk_id]
                       for stk_id in self.stkId_list]
        error = np.random.rand(len(self.stkId_list))
        heights = [100]*len(self.stkId_list)
        self.axes.hold(True)
        rects = self.axes.barh(y_pos, value_list, bar_width, 
                               align='center', 
                               color='green')
        self.axes.set_yticks(y_pos)
        self.axes.set_yticklabels(self.stkId_list)
        self.axes.set_xlabel('')
        self.axes.set_xlim(0, int(max(value_list)*1.2))
        self.axes.set_title(u'µ±Ç°³Ö²Ö')
        
        self.axes.hold(False)
        self.draw()
        
    
    def showAutoLabel(self, rects):
        for rect in rects:
            height = rect.get_height()
            self.axes.text(1.03*height, rect.get_y()+rect.get_width()/2.0,  
                           'Hahaha')