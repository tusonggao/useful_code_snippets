# https://www.jianshu.com/p/305c4961ee06   ARIMA模型原理及实现

# https://www.kaggle.com/c/favorita-grocery-sales-forecasting/data


import pandas as pd
import matplotlib.pyplot as plt
ChinaBank = pd.read_csv('ChinaBank.csv',index_col = 'Date',parse_dates=['Date'])

#ChinaBank.index = pd.to_datetime(ChinaBank.index)
sub = ChinaBank['2014-01':'2014-06']['Close']
train = sub.ix['2014-01':'2014-03']
test = sub.ix['2014-04':'2014-06']
plt.figure(figsize=(10,10))
print(train)
plt.plot(train)
plt.show()


ChinaBank['Close_diff_1'] = ChinaBank['Close'].diff(1)
ChinaBank['Close_diff_2'] = ChinaBank['Close_diff_1'].diff(1)
ChinaBank['Close_diff_3'] = ChinaBank['Close_diff_2'].diff(1)
fig = plt.figure(figsize=(20,6))
ax1 = fig.add_subplot(141)
ax1.plot(ChinaBank['Close'])
ax2 = fig.add_subplot(142)
ax2.plot(ChinaBank['Close_diff_1'])
ax3 = fig.add_subplot(143)
ax3.plot(ChinaBank['Close_diff_2'])
ax4 = fig.add_subplot(144)
ax4.plot(ChinaBank['Close_diff_3'])
plt.show()