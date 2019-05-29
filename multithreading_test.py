from __future__ import print_function, division

import sys
import os
import json
import time
import datetime
import threading

has_new_run_date, has_new_current_date = False, False
run_date_global, current_time_global = '2019-01-01', '00:00:00'
run_date, current_time = '2019-01-02', '01:00:00'

class readNewDataThread(threading.Thread):  # 每天早上从数据库获取最新数据的辅助线程
  def __init__(self, thread_num):
      threading.Thread.__init__(self)
      self.run_date = datetime.datetime.now().strftime('%Y-%m-%d')
      self.thread_num = thread_num

  def run(self):
      global run_date_global, current_time_global, has_new_run_date, has_new_current_date
      cnt = 0
      while True:
          cnt += 1
          time.sleep(7)   # 休眠30分钟
          run_date = datetime.datetime.now().strftime('%Y-%m-%d')
          current_time = datetime.datetime.now().strftime('%H:%M:%S')
          print('in readNewDataThread, run_date is ', run_date, 'current_time is ', current_time)
          if cnt%5==0:
              run_date_global, current_time_global = run_date, current_time
              has_new_run_date, has_new_current_date = True, True


def main_thread_func():
    global has_new_run_date, has_new_current_date, run_date_global, current_time_global, run_date, current_time
    while True:
        time.sleep(10)
        # run_date = datetime.datetime.now().strftime('%Y-%m-%d')
        # current_time = datetime.datetime.now().strftime('%H:%M:%S')

        if has_new_run_date:
            run_date = run_date_global
            has_new_run_date = False
        if has_new_current_date:
            current_time = current_time_global
            has_new_run_date = False
        print('in main_thread_func, run_date is', run_date, 'current_time :', current_time)


if __name__=='__main__':
    read_thread = readNewDataThread(3333)
    read_thread.start()
    # read_thread.join()

    main_thread_func()
    print('read_thread.start() triggered!')






