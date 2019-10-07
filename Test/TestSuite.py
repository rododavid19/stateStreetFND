from unittest import TestCase
from FND import *
from testerFND import *
import numpy as np
from PandasBE import piEval

class Tester(TestCase):
    def test_lessThan(self):
        with Network() as n:
            sourceDict = {'fake': randomWalkSeries(), 'fake2': randomWalkSeries2()}  # here series are loaded
            lessThan(seriesSource('fake2'), seriesSource('fake'))
            sinkDict = piEval(n, sourceDict)
            for x in sinkDict['__0__']:
                self.assertTrue(x)
        print("End of LT Test")

    def test_lessorEqual_1(self):
        with Network() as n:
            sourceDict = {'fake': randomWalkSeries(), 'fake2': randomWalkSeries2()}  # here series are loaded
            lessOrEqual(seriesSource('fake2'), seriesSource('fake'))
            sinkDict = piEval(n, sourceDict)
            for x in sinkDict['__0__']:
                self.assertTrue(x)
        print("End of LE Test 1")

    def test_lessorEqual_2(self):
        with Network() as n:
            sourceDict = {'fake': randomWalkSeries()}  # here series are loaded
            lessOrEqual(seriesSource('fake'), seriesSource('fake'))
            sinkDict = piEval(n, sourceDict)
            for x in sinkDict['__0__']:
                self.assertTrue(x)
        print("End of LE Test 2")

    def test_lessorEqual_3(self):
        with Network() as n:
            sourceDict = {'fake': randomWalkSeries(), 'fake2': randomWalkSeries2()}  # here series are loaded
            lessOrEqual(seriesSource('fake'), seriesSource('fake2'))
            sinkDict = piEval(n, sourceDict)
            for x in sinkDict['__0__']:
                self.assertFalse(x)
        print("End of LE Test 3")

    def test_Equal(self):
        with Network() as n:
            sourceDict = {'fake': randomWalkSeries()}  # here series are loaded
            equal(seriesSource('fake'), seriesSource('fake'))
            sinkDict = piEval(n, sourceDict)
            for x in sinkDict['__0__']:
                self.assertTrue(x)
        print("End of Equal Test")

    def test_greaterOrEqual_1(self):
        with Network() as n:
            sourceDict = {'fake': randomWalkSeries(), 'fake2': randomWalkSeries2()}  # here series are loaded
            greaterOrEqual(seriesSource('fake'), seriesSource('fake2'))
            sinkDict = piEval(n, sourceDict)
            for x in sinkDict['__0__']:
                self.assertTrue(x)
        print("End of GE Test 1")

    def test_greaterOrEqual_2(self):
        with Network() as n:
            sourceDict = {'fake': randomWalkSeries()}  # here series are loaded
            greaterOrEqual(seriesSource('fake'), seriesSource('fake'))
            sinkDict = piEval(n, sourceDict)
            for x in sinkDict['__0__']:
                self.assertTrue(x)
        print("End of GE Test 2")

    def test_notEqual_1(self):
        with Network() as n:
            sourceDict = {'fake': randomWalkSeries()}  # here series are loaded
            notEqual(seriesSource('fake'), seriesSource('fake'))
            sinkDict = piEval(n, sourceDict)
            for x in sinkDict['__0__']:
                self.assertFalse(x)
        print("End of NE Test 1")

    def test_notEqual_2(self):
        with Network() as n:
            sourceDict = {'fake': randomWalkSeries(), 'fake2': randomWalkSeries2()}  # here series are loaded
            notEqual(seriesSource('fake'), seriesSource('fake2'))
            sinkDict = piEval(n, sourceDict)
            for x in sinkDict['__0__']:
                self.assertTrue(x)
        print("End of NE Test 1")

    #######################################
    ###TESTS FOR DATAFRAMES (NOT SERIES)###
    #######################################

    def test_lessThan_2(self):
        with Network() as n:
            sourceDict = {'fake': randomWalkSeries3(), 'fake2': randomWalkSeries4()}  # here series are loaded
            lessThan(seriesSource('fake2'), seriesSource('fake'))
            sinkDict = piEval(n, sourceDict)
            for x in sinkDict['__0__'].values:
                self.assertTrue(x)
        print("End of LT Test")

    def test_lessorEqual_4(self):
        with Network() as n:
            sourceDict = {'fake': randomWalkSeries3(), 'fake2': randomWalkSeries4()}  # here series are loaded
            lessOrEqual(seriesSource('fake2'), seriesSource('fake'))
            sinkDict = piEval(n, sourceDict)
            for x in sinkDict['__0__'].values:
                self.assertTrue(x)
        print("End of LE Test 1")

    def test_lessorEqual_5(self):
        with Network() as n:
            sourceDict = {'fake': randomWalkSeries3()}  # here series are loaded
            lessOrEqual(seriesSource('fake'), seriesSource('fake'))
            sinkDict = piEval(n, sourceDict)
            for x in sinkDict['__0__'].values:
                self.assertTrue(x)
        print("End of LE Test 2")

    def test_lessorEqual_6(self):
        with Network() as n:
            sourceDict = {'fake': randomWalkSeries3(), 'fake2': randomWalkSeries4()}  # here series are loaded
            lessOrEqual(seriesSource('fake'), seriesSource('fake2'))
            sinkDict = piEval(n, sourceDict)
            for x in sinkDict['__0__'].values:
                self.assertFalse(x)
        print("End of LE Test 3")

    def test_Equal_2(self):
        with Network() as n:
            sourceDict = {'fake': randomWalkSeries3()}  # here series are loaded
            equal(seriesSource('fake'), seriesSource('fake'))
            sinkDict = piEval(n, sourceDict)
            for x in sinkDict['__0__'].values:
                print(x)
                self.assertTrue(x)
        print("End of Equal Test")

    def test_greaterOrEqual_3(self):
        with Network() as n:
            sourceDict = {'fake': randomWalkSeries3(), 'fake2': randomWalkSeries4()}  # here series are loaded
            greaterOrEqual(seriesSource('fake'), seriesSource('fake2'))
            sinkDict = piEval(n, sourceDict)
            for x in sinkDict['__0__'].values:
                self.assertTrue(x)
        print("End of GE Test 1")

    def test_greaterOrEqual_4(self):
        with Network() as n:
            sourceDict = {'fake': randomWalkSeries3()}  # here series are loaded
            greaterOrEqual(seriesSource('fake'), seriesSource('fake'))
            sinkDict = piEval(n, sourceDict)
            for x in sinkDict['__0__'].values:
                self.assertTrue(x)
        print("End of GE Test 2")

    def test_notEqual_3(self):
        with Network() as n:
            sourceDict = {'fake': randomWalkSeries3()}  # here series are loaded
            notEqual(seriesSource('fake'), seriesSource('fake'))
            sinkDict = piEval(n, sourceDict)
            for x in sinkDict['__0__'].values:
                self.assertFalse(x)
        print("End of NE Test 1")

    def test_notEqual_4(self):
        with Network() as n:
            sourceDict = {'fake': randomWalkSeries3(), 'fake2': randomWalkSeries4()}  # here series are loaded
            notEqual(seriesSource('fake'), seriesSource('fake2'))
            sinkDict = piEval(n, sourceDict)
            for x in sinkDict['__0__'].value:
                self.assertTrue(x)
        print("End of NE Test 1")