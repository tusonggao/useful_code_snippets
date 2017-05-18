class PositionsShowerDlg(QtGui.QMainWindow):
    def __init__(self, parent=None, recorder=None):
        QtGui.QMainWindow.__init__(self, parent)
        
        self.valPlot = StockPositionValueMplWidget(width=12, height=18)
        self.recorder = recorder
        self.valPlot.plot(self.recorder)        
        hLayout = QtGui.QHBoxLayout()
        
#        hLayout.addStretch()
#        hLayout.addSpacing(10)
#        hLayout.addSpacing(10)
#        hLayout.addStretch()
        
        self.valPlot11 = StockPositionValueMplWidget(width=12, height=18)
        self.valPlot.plot(self.recorder)
        hLayout.addWidget(self.valPlot)
        hLayout.addWidget(self.valPlot11)
        
        self.scrollArea = QtGui.QScrollArea()
#        self.scrollArea.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.scrollArea.setAlignment(Qt.AlignHCenter)
#        self.scrollArea.setWidgetResizable(True)
#        self.scrollArea.setBackgroundRole(QtGui.QPalette.Dark)
#        self.scrollArea.setWidget(self.valPlot)
        widget = QtGui.QWidget()
        widget.setLayout(hLayout)
        self.scrollArea.setWidget(widget)
        
#        self.scrollArea.setWidget(self.valPlot)
        
#        self.resizeEvent = self.onResize
#        self.scrollArea.setLayout(hLayout)
        
        self.setCentralWidget(self.scrollArea)
        self.timer = QtCore.QTimer(self)
        self.timer.timeout.connect(self.onTimer)
        self.timer.start(7000)
#        self.connect(self, QtCore.SIGNAL('resizeEvent()'), 
#                     self.onResize)
        
    def onTimer(self):
        self.valPlot.plot(self.recorder)

    def onResize(self):
        print('triggered onResize()')
        pass