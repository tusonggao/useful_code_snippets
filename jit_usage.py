import time
from numba import jit

#使用jit 对于计算密集型的程序 可以快100倍!

def foo1(x,y):
    start_t = time.time()
    s = 0
    for i in range(x,y):
        s += i
    print('foo1 Time used: {} sec Outcome: {}'.format(time.time()-start_t, s))
    return s

print(foo1(1,100000000))                 
# 输出：foo1 Time used: 13.098878145217896 sec Outcome: 4999999950000000


@jit
def foo2(x,y):
    start_t = time.time()
    s = 0
    for i in range(x,y):
        s += i
    print('foo2 Time used: {} sec Outcome: {}'.format(time.time()-start_t, s))
    return s

print(foo2(1,100000000))
	
# 输出：foo2 Time used: 0.129807710647583 sec Outcome: 4999999950000000