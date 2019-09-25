from unittest import TestCase
from FND import *
from testerFND import *
import numpy as np
from PandasBE import piEval
from math import ceil
class Tester(TestCase):
   def testABS(self):
       with Network() as n:
            sourceDict = {'fake': randomWalkSeries2()}  # here series are loaded
            abs(seriesSource('fake'))
            sinkDict = piEval(n, sourceDict)
            n.report()
            for x in sinkDict['__0__']:
                print(x)
                self.assertGreaterEqual(x, 0)

   def testABS2(self):
       with Network() as n:
            sourceDict = {'fake': randomWalkSeries2()}  # here series are loaded
            abs(seriesSource('fake'))
            sinkDict = piEval(n, sourceDict)
            self.assertTrue(abs(x) == y for x, y in zip(sourceDict['fake'], sinkDict['__0__']))

   def testRemainder(self):
       with Network() as n:
            sourceDict = {'fake': randomWalkSeries2(), 'fake2': randomWalkSeries2()}  # here series are loaded
            remainder(seriesSource('fake'), seriesSource('fake2'))
            sinkDict = piEval(n, sourceDict)
            self.assertTrue(z == (x % y) for x, y, z in zip(sourceDict['fake'], sourceDict['fake2'], sinkDict['__0__']))

   def testFloor(self):
       with Network() as n:
            sourceDict = {'fake': randomWalkSeries()}  # here series are loaded
            floor(seriesSource('fake'))
            sinkDict = piEval(n, sourceDict)
            n.report()
            self.assertTrue(y == x//1 for x, y in zip(sourceDict['fake'], sinkDict['__0__']))

   def testCeiling(self):
       with Network() as n:
           sourceDict = {'fake': randomWalkSeries()}  # here series are loaded
           ceiling(seriesSource('fake'))
           sinkDict = piEval(n, sourceDict)
           n.report()
           self.assertTrue(y == ceil(x) for x, y in zip(sourceDict['fake'], sinkDict['__0__']))

   def testLog(self):
       with Network() as n:
           sourceDict = {'fake': randomWalkSeries()}  # here series are loaded
           log(seriesSource('fake'))
           sinkDict = piEval(n, sourceDict)
           n.report()
           self.assertTrue(y == np.log(x) for x, y in zip(sourceDict['fake'], sinkDict['__0__']))

    #######################################
    ###TESTS FOR DATAFRAMES (NOT SERIES)###
    #######################################

   def testABS3(self):
       with Network() as n:
            sourceDict = {'fake': randomWalkSeries3()}  # here series are loaded
            abs(seriesSource('fake'))
            sinkDict = piEval(n, sourceDict)
            n.report()
            for x in sinkDict['__0__'].values:
                print(x)
                self.assertGreaterEqual(x, 0)

   def testABS4(self):
       with Network() as n:
            sourceDict = {'fake': randomWalkSeries4()}  # here series are loaded
            abs(seriesSource('fake'))
            sinkDict = piEval(n, sourceDict)
            self.assertTrue(abs(x) == y for x, y in zip(sourceDict['fake'].values, sinkDict['__0__'].values))

   def testRemainder2(self):
       with Network() as n:
            sourceDict = {'fake': randomWalkSeries3(), 'fake2': randomWalkSeries4()}  # here series are loaded
            remainder(seriesSource('fake'), seriesSource('fake2'))
            sinkDict = piEval(n, sourceDict)
            self.assertTrue(z == (x % y) for x, y, z in zip(sourceDict['fake'].values, sourceDict['fake2'].values, sinkDict['__0__'].values))

   def testFloor2(self):
       with Network() as n:
            sourceDict = {'fake': randomWalkSeries3()}  # here series are loaded
            floor(seriesSource('fake'))
            sinkDict = piEval(n, sourceDict)
            n.report()
            self.assertTrue(y == x//1 for x, y in zip(sourceDict['fake'].values, sinkDict['__0__'].values))

   def testCeiling2(self):
       with Network() as n:
           sourceDict = {'fake': randomWalkSeries3()}  # here series are loaded
           ceiling(seriesSource('fake'))
           sinkDict = piEval(n, sourceDict)
           n.report()
           self.assertTrue(y == ceil(x) for x, y in zip(sourceDict['fake'].values, sinkDict['__0__'].values))

   def testLog2(self):
       with Network() as n:
           sourceDict = {'fake': randomWalkSeries3()}  # here series are loaded
           log(seriesSource('fake'))
           sinkDict = piEval(n, sourceDict)
           n.report()
           self.assertTrue(y == np.log(x) for x, y in zip(sourceDict['fake'].values, sinkDict['__0__'].values))