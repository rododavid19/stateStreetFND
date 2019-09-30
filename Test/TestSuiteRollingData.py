from unittest import TestCase
from FND import *
from testerFND import *
import numpy as np
from PandasBE import piEval
from math import ceil


class Tester(TestCase):
   def testMinRolling(self):
       with Network() as n:
           sourceDict = {'fake': randomWalkSeries()}  # here series are loaded
           df = seriesSource('fake')
           min(df, name="test", window=5)
           toComp = sourceDict['fake']
           toCompSeries = pd.Series(toComp).rolling(window=5).min()
           sinkDict = piEval(n, sourceDict)
           toComp2 = sinkDict['test']
           self.assertTrue(x == y for x, y in zip(toCompSeries.values, toComp2.values))
           n.report()

   def testMaxRolling(self):
       with Network() as n:
           sourceDict = {'fake': randomWalkSeries()}  # here series are loaded
           df = seriesSource('fake')
           max(df, name="test", window=5)
           toComp = sourceDict['fake']
           toCompSeries = pd.Series(toComp).rolling(window=5).max()
           sinkDict = piEval(n, sourceDict)
           toComp2 = sinkDict['test']
           self.assertTrue(x == y for x, y in zip(toCompSeries.values, toComp2.values))
           n.report()

   def testStandardDeviationRolling(self):
       with Network() as n:
           sourceDict = {'fake': randomWalkSeries()}  # here series are loaded
           df = seriesSource('fake')
           stdev(df, name="test", window=5)
           toComp = sourceDict['fake']
           toCompSeries = pd.Series(toComp).rolling(window=5).std()
           sinkDict = piEval(n, sourceDict)
           toComp2 = sinkDict['test']
           self.assertTrue(x == y for x, y in zip(toCompSeries.values, toComp2.values))
           n.report()

   def testSumRolling(self):
       with Network() as n:
           sourceDict = {'fake': randomWalkSeries()}  # here series are loaded
           df = seriesSource('fake')
           sum(df, name="test", window=5)
           toComp = sourceDict['fake']
           toCompSeries = pd.Series(toComp).rolling(window=5).sum()
           sinkDict = piEval(n, sourceDict)
           toComp2 = sinkDict['test']
           self.assertTrue(x == y for x, y in zip(toCompSeries.values, toComp2.values))
           n.report()

   def testMeanRolling(self):
       with Network() as n:
           sourceDict = {'fake': randomWalkSeries()}  # here series are loaded
           df = seriesSource('fake')
           sma(df, name="test", window=5)
           toComp = sourceDict['fake']
           toCompSeries = pd.Series(toComp).rolling(window=5).mean()
           sinkDict = piEval(n, sourceDict)
           toComp2 = sinkDict['test']
           self.assertTrue(x == y for x, y in zip(toCompSeries.values, toComp2.values))
           n.report()