import unittest
from FND import *
import numpy as np
from testerFND import *

forex = pd.read_csv('forex.csv')
#TODO for this data, it makes most sense for now to chop off the dates column and the zeros column, which cause problems
forex = forex.drop(forex.columns[[0,5]], axis=1) #.values


class TestPrimitivesA(unittest.TestCase):

    def test_AddPrimitive(self):
        print("testing primitive: add...")
        with Network() as n:
            sourceDict = {'fake': randomWalkSeries()}
            add(seriesSource('fake'), seriesSource('fake'))
            sinkDict = piEval(n, sourceDict)
            minSource = np.amin(sourceDict['fake']._values) + np.amin(sourceDict['fake']._values)
            minSink = np.amin(sinkDict['__0__']._values)
            self.assertEqual(minSource, minSink)
        print("test successful")

    def test_AddPrimitiveDeep(self):
        print("deep testing primitive: add...")
        with Network() as n:
            sourceDict = {'fake': randomWalkSeries()}
            add(seriesSource('fake'), seriesSource('fake'))
            sinkDict = piEval(n, sourceDict)
            for x, y in zip(sourceDict['fake'], sinkDict['__0__']):
                self.assertEqual(x*2, y)
        print("test successful")

    def test_AddPrimitiveDFDeep(self):
        print("deep df testing primitive: add...")
        with Network() as n:
            sourceDict = {'forex': forex}
            add(seriesSource('forex'), seriesSource('forex'))
            sinkDict = piEval(n, sourceDict)
            pd.DataFrame.equals(forex.add(forex), sinkDict)
            #for source, sink in zip(sourceDict['forex'].iterrows(), sinkDict['__0__'].iterrows()):
             #   for i in range(1, len(source[1])):
             #       self.assertEqual(source[1][i]*2, sink[1][i])
        print("test successful")

    def test_SubtractPrimitive(self):
        print("testing primitive: subtract...")
        with Network() as n:
            sourceDict = {'fake': randomWalkSeries()}
            subtract(seriesSource('fake'), seriesSource('fake'))
            sinkDict = piEval(n, sourceDict)
            minSource = np.amin(sourceDict['fake']._values) - np.amin(sourceDict['fake']._values)
            minSink = np.amin(sinkDict['__0__']._values)
            self.assertEqual(minSource, minSink)
        print("test successful")

    def test_SubtractPrimitiveDeep(self):
        print("deep testing primitive: subtract...")
        with Network() as n:
            sourceDict = {'fake': randomWalkSeries()}
            subtract(seriesSource('fake'), seriesSource('fake'))
            sinkDict = piEval(n, sourceDict)
            for x, y in zip(sourceDict['fake'], sinkDict['__0__']):
                self.assertEqual(x-x, y)
        print("test successful")

    def test_SubtractPrimitiveDFDeep(self):
        print("deep df testing primitive: subtract...")
        with Network() as n:
            sourceDict = {'forex': forex}
            subtract(seriesSource('forex'), seriesSource('forex'))
            sinkDict = piEval(n, sourceDict)
            for source, sink in zip(sourceDict['forex'].iterrows(), sinkDict['__0__'].iterrows()):
                for i in range(1, len(source[1])):
                    self.assertEqual(source[1][i]-source[1][i], sink[1][i])
        print("test successful")

    def test_MultiplyPrimitive(self):
        print("testing primitive: multiply...")
        with Network() as n:
            sourceDict = {'fake': randomWalkSeries()}
            multiply(seriesSource('fake'), seriesSource('fake'))
            sinkDict = piEval(n, sourceDict)
            minSource = np.amin(sourceDict['fake']._values) * np.amin(sourceDict['fake']._values)
            minSink = np.amin(sinkDict['__0__']._values)
            self.assertEqual(minSource, minSink)
        print("test successful")

    def test_MultiplyPrimitiveDeep(self):
        print("deep testing primitive: multiply...")
        with Network() as n:
            sourceDict = {'fake': randomWalkSeries()}
            multiply(seriesSource('fake'), seriesSource('fake'))
            sinkDict = piEval(n, sourceDict)
            for x, y in zip(sourceDict['fake'], sinkDict['__0__']):
                self.assertEqual(x*x, y)
        print("test successful")

    def test_MultiplyPrimitiveDFDeep(self):
        print("deep df testing primitive: multiply...")
        with Network() as n:
            sourceDict = {'forex': forex}
            multiply(seriesSource('forex'), seriesSource('forex'))
            sinkDict = piEval(n, sourceDict)
            pd.DataFrame.equals(forex.subtract(forex), sinkDict)
            # for source, sink in zip(sourceDict['forex'].iterrows(), sinkDict['__0__'].iterrows()):
            #     for i in range(1, len(source[1])):
            #         self.assertEqual(source[1][i]*source[1][i], sink[1][i])
        print("test successful")

    def test_DividePrimitive(self):
        print("testing primitive: divide...")
        with Network() as n:
            sourceDict = {'fake': randomWalkSeries()}
            divide(seriesSource('fake'), seriesSource('fake'))
            sinkDict = piEval(n, sourceDict)
            minSource = np.amin(sourceDict['fake']._values) / np.amin(sourceDict['fake']._values)
            minSink = np.amin(sinkDict['__0__']._values)
            self.assertEqual(minSource, minSink)
        print("test successful")

    def test_DividePrimitiveDeep(self):
        print("deep testing primitive: divide...")
        with Network() as n:
            sourceDict = {'fake': randomWalkSeries()}
            divide(seriesSource('fake'), seriesSource('fake'))
            sinkDict = piEval(n, sourceDict)
            for x, y in zip(sourceDict['fake'], sinkDict['__0__']):
                self.assertEqual(x/x, y)
        print("test successful")

    def test_DividePrimitiveDFDeep(self):
        print("deep df testing primitive: mdivide...")
        with Network() as n:
            sourceDict = {'forex': forex}
            divide(seriesSource('forex'), seriesSource('forex'))
            sinkDict = piEval(n, sourceDict)
            pd.DataFrame.equals(forex.divide(forex), sinkDict)
            # for source, sink in zip(sourceDict['forex'].iterrows(), sinkDict['__0__'].iterrows()):
            #     for i in range(1, len(source[1])):
            #         self.assertEqual(source[1][i]/source[1][i], sink[1][i])
        print("test successful")

    def test_NegativePrimitive(self):
        print("testing primitive: negative...")
        with Network() as n:
            sourceDict = {'fake': randomWalkSeries()}
            neg(seriesSource('fake'))
            sinkDict = piEval(n, sourceDict)
            minSource = np.amax(sourceDict['fake']._values) * (-1)
            minSink = np.amin(sinkDict['__0__']._values)
            self.assertEqual(minSource, minSink)
        print("test successful")

    def test_NegativePrimitiveDeep(self):
        print("deep testing primitive: negative...")
        with Network() as n:
            sourceDict = {'fake': randomWalkSeries()}
            neg(seriesSource('fake'))
            sinkDict = piEval(n, sourceDict)
            for x, y in zip(sourceDict['fake'], sinkDict['__0__']):
                self.assertEqual(x*(-1), y)
        print("test successful")

    def test_NegativePrimitiveDFDeep(self):
        print("deep df testing primitive: negative...")
        with Network() as n:
            sourceDict = {'forex': forex}
            neg(seriesSource('forex'))
            sinkDict = piEval(n, sourceDict)
            pd.DataFrame.equals(-forex, sinkDict)
            # for source, sink in zip(sourceDict['forex'].iterrows(), sinkDict['__0__'].iterrows()):
            #     for i in range(1, len(source[1])):
            #         self.assertEqual(source[1][i]*(-1), sink[1][i])
        print("test successful")
