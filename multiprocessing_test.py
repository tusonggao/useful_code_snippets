import multiprocessing
from multiprocessing import Pool
from turtle import *
import time
import os

ttt_vvv = [3, 4, 5]

def fff(d, l):
    d[1] = '1'
    d['2'] = 2
    l.reverse()
    
def multiprocessing_main():
    manager = multiprocessing.Manager()
    d = manager.dict()
    l = manager.list(range(10))
    
    print 'd is ', d
    print 'l is ', l
    
    p = multiprocessing.Process(target=fff, args=(d, l))
    p.start()
    p.join()
    
    print 'd is ', d
    print 'l is ', l

def f_show_event():
    print('In f_show_event: ', ' Hello World!')

class Event_handler(object):
    def __init__(self):
        self.event_handlers = {}
        self.add_event_handler('show', f_show_event)
    
    def handle_event(self, event_name):
        self.event_handlers[event_name]()
    
    def add_event_handler(self, name, func):
        self.event_handlers[name] = func
    
    def send_event(self, event_name):
        self.event_list.append(event_name)
        
        

def fff(pid, results, handler):
    sum_val = 0.0
    for i in range(10**5):
        sum_val += 1.0/(i+1)**(pid+1)
    results[pid] = sum_val
    ttt_vvv.append(pid)
    handler.handle_event('show')
#    if hasattr(os, 'getppid'):  # only available on Unix
#        print('parent process:', os.getppid())
    print('process id:', os.getpid(), 'ttt_vvv is ', ttt_vvv)

def multiprocessing_main():
    manager = multiprocessing.Manager()
    results = manager.dict()
    process_list = []
    handler = Event_handler()
    print('In multiprocessing_main')
    begin_t = time.time()
    for i in range(8):
        p = multiprocessing.Process(target=fff, args=(i, results, handler))
        process_list.append(p)
        p.start()
    for p in process_list:
        p.join()
    end_t = time.time()    
    print('results is ', results)
    print('In Main process ttt_vvv is ', ttt_vvv)
    print('multiprocessing_main cost time is %.7f sec'%(end_t - begin_t))
    for p in process_list:
        print('p is_alive ', p.is_alive())
    
def nonmultiprocessing_main():
    results = {}    
    print('In nonmultiprocessing_main')
    begin_t = time.time()
    for i in range(8):
        sum_val = 0.0
        for j in range(10**5):
            sum_val += 1.0/(j+1)**(i+1)
        results[i] = sum_val
    end_t = time.time()    
    print('results is ', results)
    print('multiprocessing_main cost time is %.7f sec'%(end_t - begin_t))

#def multiprocessing_main():
#    manager = multiprocessing.Manager()
#    d = manager.dict()
#    l = manager.list(range(10))
#    
#    print('d is ', d)
#    print('l is ', l)
#    
#    p = multiprocessing.Process(target=fff, args=(d, l))
#    p.start()
#    p.join()
#    
#    print('d is ', d)
#    print('l is ', l)

    
def f(x):
    return x*x

if __name__ == '__main__':
#    with Pool(5) as p:
#        print(p.map(f, [1, 2, 3]))    
#    multiprocessing_main()
#    print(multiprocessing.cpu_count())
    
    color('red', 'yellow')
    begin_fill()
    while True:
        forward(200)
        left(170)
        if abs(pos()) < 1:
            break
    end_fill()
    done()

#    nonmultiprocessing_main()
