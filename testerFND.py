from FND import *
import numpy as np
from pymongo import MongoClient
from arctic import Arctic
from PandasBE import piEval
import pandas as pd
import hashlib
import matplotlib.pyplot as plt
#import xlrd


forex = pd.read_csv('tests/forex.csv')


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
    return pd.DataFrame(values, index=dates)




with Network() as n:
# 'forex': forex,

    sourceDict = { 'fake':randomWalkSeries()}  # here series are loaded

    a = seriesSource('fake')
    a = -a
    a += a


<<<<<<< HEAD
    add(seriesSource('fake'), seriesSource('fake') )
    getColumns(forex, ["DateTime", "BidPrice1", "AskPrice2"], True)
=======
  #  add(seriesSource('fake'), seriesSource('del') )
>>>>>>> parent of cf48df2... FND v1.2 Report func fix
   # macd(seriesSource('fake'))
   # x = ema(seriesSource('fake'),10)
   # y = ema(seriesSource('fake'),10)
   # z = x - y
   # w = z - x

    sinkDict = piEval(n, sourceDict)                # here network objects are mapped to their values and evaluated
    n.report()

   # df = sinkDict.get('__2__/signal')
   # df.plot()
   # plt.show()




<<<<<<< HEAD

=======
##TODO: attemp another network here as "m" and make sure it's the same object?? or new Newtwork.
>>>>>>> parent of cf48df2... FND v1.2 Report func fix
