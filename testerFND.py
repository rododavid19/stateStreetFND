from FND import *
import numpy as np
from PandasBE import piEval
import pandas as pd
import hashlib
#import matplotlib.pyplot as plt
#import xlrd


forex = pd.read_csv('forex.csv')


def encrypt_string(hash_string):
  '''
  This function takes a string to be hashed as input, and returns a hash signature.
  param hash_string: input string for which hash will be created.
  If hash_string is not a bytestring, we need to encode it with the .encocde() function.
  return param sha_signature: hash for the input string generated using the SHA2 hashing method.
  '''
  sha_signature = hashlib.sha256(hash_string.encode()).hexdigest()
  return sha_signature



def randomWalkSeries(initialValue=100, sigma=.002, start='2019-01-01', end='2019-01-31', freq='H'):
    dates = pd.date_range(start=start, end=end, freq=freq)
    values = np.zeros(len(dates))
    norm = np.random.normal(size=len(dates))
    value = initialValue
    values[0] = value
    for i in range(1, len(values)):
        values[i] = values[i - 1] + (values[i - 1] * norm[i] * sigma)
    return pd.Series(values, index=dates)

def randomWalkSeries2(initialValue= -100, sigma=.002, start='2019-01-01', end='2019-01-31', freq='H'):
    dates = pd.date_range(start=start, end=end, freq=freq)
    values = np.zeros(len(dates))
    norm = np.random.normal(size=len(dates))
    value = initialValue
    values[0] = value
    for i in range(1, len(values)):
        values[i] = values[i - 1] + (values[i - 1] * norm[i] * sigma)
    return pd.Series(values, index=dates)




with Network() as n:
# 'forex': forex,

    sourceDict = {'fake': randomWalkSeries()}  # here series are loaded


    add(seriesSource('fake'), seriesSource('fake') )
    macd(seriesSource('fake'))
    x = ema(seriesSource('fake'),10)
    y = ema(seriesSource('fake'),10)
    z = x - y
    w = z - x


   # macd(seriesSource('fake'))
   # add(seriesSource('fake'), seriesSource('fake'))
   # equal(seriesSource('fake'), seriesSource('fake'))
 #   equal(a, b)
    #macd(seriesSource('fake'))

    sinkDict = piEval(n, sourceDict)                # here network objects are mapped to their values and evaluated
    n.report()


    print(sinkDict)



##TODO: attemp another network here as "m" and make sure it's the same object?? or new Newtwork.
