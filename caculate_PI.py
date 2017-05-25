import numpy as np
import math
import itertools

from decimal import Decimal as Dec, getcontext as gc

def PI(maxK=70, prec=1008, disp=1007): # parameter defaults chosen to gain 1000+ digits within a few seconds
    gc().prec = prec
    K, M, L, X, S = 6, 1, 13591409, 1, 13591409 
    for k in range(1, maxK+1):
        M = (K**3 - (K<<4)) * M / k**3 
        L += 545140134
        X *= -262537412640768000
        S += Dec(M * L) / X
        K += 12
    pi = 426880 * Dec(10005).sqrt() / S
    pi = Dec(str(pi)[:disp]) # drop few digits of precision for accuracy
    print("PI(maxK=%d iterations, gc().prec=%d, disp=%d digits) =\n%s" % 
          (maxK, prec, disp, pi))
    return pi

def calculate_PI(digit):
    prev_PI = 0
    new_PI = 0
    sign = 1
    sum_val = 0
    
    for k in range(100000):
        sum_val += (12*sign*math.factorial(6*k)*(545140134*k + 13591409) /
             (math.factorial(3*k)*math.factorial(k)*math.pow(640320, 3*k+1.5)))
        new_PI = 1.0/sum_val
        if (new_PI-prev_PI) <= 10**(-digit):
            break
        sign *= -1
        prev_PI = new_PI
    
    return new_PI
        
if __name__=='__main__':
    print('%.50f'%(calculate_PI(40)))
    print('%.50f'%(math.pi))
    print('%.50f'%(PI()))
        