from FND import *
import numpy as np
from PandasBE import piEval
import pandas as pd
import hashlib
import matplotlib.pyplot as plt
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

def randomWalkSeries3(initialValue=100, sigma=.002, start='2019-01-01', end='2019-01-31', freq='H'):
    dates = pd.date_range(start=start, end=end, freq=freq)
    values = np.zeros(len(dates))
    norm = np.random.normal(size=len(dates))
    value = initialValue
    values[0] = value
    for i in range(1, len(values)):
        values[i] = values[i - 1] + (values[i - 1] * norm[i] * sigma)
    return pd.DataFrame(values, index=dates)

def randomWalkSeries4(initialValue= -100, sigma=.002, start='2019-01-01', end='2019-01-31', freq='H'):
    dates = pd.date_range(start=start, end=end, freq=freq)
    values = np.zeros(len(dates))
    norm = np.random.normal(size=len(dates))
    value = initialValue
    values[0] = value
    for i in range(1, len(values)):
        values[i] = values[i - 1] + (values[i - 1] * norm[i] * sigma)
    return pd.DataFrame(values, index=dates)




with Network() as n:
    n.report()





