import unittest
from FND import *
from PandasBE import piEval
import pandas as pd
import numpy as np

def randomWalkSeries(initialValue=100, sigma=.002, start='2019-01-01', end='2019-01-31', freq='H'):
    dates = pd.date_range(start=start, end=end, freq=freq)
    values = np.zeros(len(dates))
    norm = np.random.normal(size=len(dates))
    value = initialValue
    values[0] = value
    for i in range(1, len(values)):
        values[i] = values[i - 1] + (values[i - 1] * norm[i] * sigma)
    return pd.Series(values, index=dates)

class testNetwork_ColumnMethods(unittest.TestCase):

    def test_GetColString(self):
        #Checks to make sure inputting a single string returns the correct column as a pd.series object
        with Network() as n:
            forex = pd.read_csv("forex.csv")
            sourceDict = {'forex': forex}  # here series are loaded
            df = seriesSource('forex')
            columnNames = "DateTime"
            getColumns(df, columnNames, name='test')
            toComp = sourceDict['forex'].get(columnNames)
            sinkDict = piEval(n, sourceDict)
            toComp2 = sinkDict['test']
            self.assertIsNone(pd.testing.assert_series_equal(toComp, toComp2))
            n.report()

    def test_GetColStringErorr(self):
        #Checks to make sure columnNames is a string
        with Network() as n:
            forex = pd.read_csv("forex.csv")
            sourceDict = {'forex': forex}
            df = seriesSource('forex')
            columnNames = 1
            getColumns(df, columnNames)
            self.assertRaises(Exception, piEval, n, sourceDict)


    def test_GetColArray(self):
        #Checks to make sure inputting a list of string returns the correct columns as a pd.Dataframe
        with Network() as n:
            forex = pd.read_csv("forex.csv")
            sourceDict = {'forex': forex}  # here series are loaded
            df = seriesSource('forex')
            cols = ['DateTime', 'BidPrice1', 'AskPrice2']
            getColumns(df, cols,name='test')
            toComp = pd.DataFrame(columns=cols)
            for s in cols:
                toComp[s] = sourceDict['forex'][s]
            sinkDict = piEval(n, sourceDict)
            toComp2 = sinkDict['test']
            self.assertIsNone(pd.testing.assert_frame_equal(toComp,toComp2))
            n.report()

    def test_GetColArrayError(self):
        #Checks to make sure columnNames is a list of only strings
        with Network() as n:
            forex = pd.read_csv("forex.csv")
            sourceDict = {'forex': forex}  # here series are loaded
            df = seriesSource('forex')
            columns = ['DateTime', 1, False]
            getColumns(df, columns)
            self.assertRaises(Exception, piEval, n, sourceDict)



    #def test_PutColEmpty(self):
    #    with Network() as n:
    #        forex = pd.read_csv("forex.csv")
    #        sourceDict = {'forex': forex}
    #        df = seriesSource('forex')
    #        newDf = seriesSource()
            #TODO: figure out how to transmit an empty seriesSource object to primitive OP
    #        columnDict = {'DateTime' : forex['DateTime'], 'AskPrice1': forex['AskPrice1']}
    #        putColumns(df, columnDict, newDf)
    #        sinkDict =piEval(n, sourceDict)
    #        n.report()

    def test_PutMultipleCol(self):
        #Checks to make sure multiple valid columns can be added to a dataframe
        with Network() as n:
            forex = pd.read_csv("forex.csv")
            forex2 = forex.copy()
            del forex2['Zero']
            del forex2['AskPrice2']
            del forex2['BidPrice2']
            sourceDict = {'forex': forex, 'forex2': forex2}
            df = seriesSource('forex')
            newDf = seriesSource('forex2')
            columnDict = {'AskPrice2' : forex['AskPrice2'], 'BidPrice2': forex['BidPrice2'], 'Zero':forex['Zero']}
            columnDictTest = {'DateTime': forex['DateTime'], 'AskPrice1':forex['AskPrice1'], 'BidPrice1': forex['BidPrice1'],'AskPrice2' : forex['AskPrice2'], 'BidPrice2': forex['BidPrice2'], 'Zero':forex['Zero']}
            putColumns(df, columnDict, newDf, name='test')
            toComp = pd.DataFrame(columns=list(forex2))
            for key in columnDictTest.keys():
                toComp[key] = columnDictTest[key]
            sinkDict =piEval(n, sourceDict)
            toComp2 = sinkDict['test']
            self.assertIsNone(pd.testing.assert_frame_equal(toComp,toComp2))
            n.report()

    def test_PutColSeriesError(self):
        #Checks to make sure newDf is a dataframe and not a series
        with Network() as n:
            forex = pd.read_csv("forex.csv")
            sourceDict = {'forex': forex, 'fake': randomWalkSeries()}
            df = seriesSource('forex')
            newDf = seriesSource('fake')
            columnDict = {'AskPrice2' : forex['AskPrice2'], 'BidPrice2': forex['BidPrice2'], 'Zero':forex['Zero']}
            putColumns(df, columnDict, newDf)
            self.assertRaises(Exception, piEval, n, sourceDict)

    #def test_PutCol_MisAlignedIndices_Error(self):
    #    #Checks to make sure that user can only execute putColumns if newDf indices allign with the series in columnDict
    #    with Network() as n:
    #        forex = pd.read_csv("forex.csv")
    #        forex2 = forex.copy()
    #        del forex2['Zero']
    #        del forex2['AskPrice2']
    #        del forex2['BidPrice2']
    #        forex2 = forex2[:-3]
    #        sourceDict = {'forex': forex, 'forex2': forex2}
    #        df = seriesSource('forex')
    #        newDf = seriesSource('forex2')
    #        columnDict = {'AskPrice2': forex['AskPrice2'], 'BidPrice2': forex['BidPrice2'],'Zero': forex['Zero']}
    #        putColumns(df, columnDict, newDf)
    #        self.assertRaises(Exception, piEval, n, sourceDict)

    def test_PutCol_SameColAlreadyInNewDf_Error(self):
        #Checks to make sure columnDict does not contain any columns already in newDf
        with Network() as n:
            forex = pd.read_csv("forex.csv")
            forex2 = forex.copy()
            sourceDict = {'forex': forex, 'forex2': forex2}
            df = seriesSource('forex')
            newDf = seriesSource('forex2')
            columnDict = {'Zero': forex['Zero']}
            putColumns(df, columnDict, newDf)
            self.assertRaises(Exception, piEval, n, sourceDict)



class testNetwork_EMA_STDEV_MIN_MAX_SUM_DELAY(unittest.TestCase):

    def test_EMADataframe(self):
        with Network() as n:
            forex = pd.read_csv("forex.csv")
            sourceDict = {'forex': forex}  # here series are loaded
            df = seriesSource('forex')
            ema(df, span=10, name="test")
            toComp = sourceDict['forex'].ewm(span=10).mean()
            sinkDict = piEval(n, sourceDict)
            toComp2 = sinkDict['test']
            self.assertIsNone(pd.testing.assert_frame_equal(toComp, toComp2))
            n.report()

    def test_EMASeries(self):
        with Network() as n:
            sourceDict = {'fake': randomWalkSeries()}
            df = seriesSource('fake')
            ema(df, span=10, name="test")
            toComp = sourceDict['fake'].ewm(span=10).mean()
            sinkDict = piEval(n,sourceDict)
            toComp2 = sinkDict['test']
            self.assertIsNone(pd.testing.assert_series_equal(toComp, toComp2))
            n.report()

    def test_STDEV_Window0(self):
        with Network() as n:
            forex = pd.read_csv("forex.csv")
            sourceDict = {'forex': forex}  # here series are loaded
            df = seriesSource('forex')
            stdev(df, window=0,name="test")
            toComp = sourceDict['forex'].std(axis=0)
            sinkDict = piEval(n, sourceDict)
            toComp2 = sinkDict['test']
            self.assertIsNone(pd.testing.assert_series_equal(toComp, toComp2))
            n.report()

    def test_STDEV_Window1(self):
        with Network() as n:
            forex = pd.read_csv("forex.csv")
            sourceDict = {'forex': forex}  # here series are loaded
            df = seriesSource('forex')
            stdev(df, window=1,name="test")
            toComp = sourceDict['forex'].std(axis=1)
            sinkDict = piEval(n, sourceDict)
            toComp2 = sinkDict['test']
            self.assertIsNone(pd.testing.assert_series_equal(toComp, toComp2))
            n.report()

    def test_STDEV_WindowString(self):
        with Network() as n:
            forex = pd.read_csv("forex.csv")
            sourceDict = {'forex': forex}  # here series are loaded
            df = seriesSource('forex')
            stdev(df, window="AskPrice1",name="test")
            toComp = sourceDict['forex'].loc[:, "AskPrice1"].std()
            sinkDict = piEval(n, sourceDict)
            toComp2 = sinkDict['test']
            self.assertEqual(toComp, toComp2)
            n.report()

    def test_STDEV_Series(self):
        with Network() as n:
            sourceDict = {'fake': randomWalkSeries()}  # here series are loaded
            df = seriesSource('fake')
            stdev(df, name="test")
            toComp = sourceDict['fake'].std()
            sinkDict = piEval(n, sourceDict)
            toComp2 = sinkDict['test']
            self.assertEqual(toComp,toComp2)
            n.report()


    def test_MINSeries(self):
        #Basic Mintest for Series object
        with Network() as n:
            sourceDict = {'fake': randomWalkSeries()}  # here series are loaded
            df = seriesSource('fake')
            min(df, name="test")
            toComp = sourceDict['fake'].min()
            sinkDict = piEval(n, sourceDict)
            toComp2 = sinkDict['test']
            self.assertEqual(toComp, toComp2)
            n.report()

    def test_MINDataframe(self):
        #Basic Mintest for Dataframe object
        with Network() as n:
            forex = pd.read_csv('forex.csv')
            sourceDict = {'forex': forex}  # here series are loaded
            df = seriesSource('forex')
            min(df, name="test")
            toComp = sourceDict['forex'].min()
            sinkDict = piEval(n, sourceDict)
            toComp2 = sinkDict['test']
            self.assertEqual(toComp.all(), toComp2.all())
            n.report()

    def test_MAXSeries(self):
        #Basic maxtest for Series Object
        with Network() as n:
            sourceDict = {'fake': randomWalkSeries()}  # here series are loaded
            df = seriesSource('fake')
            max(df, name="test")
            toComp = sourceDict['fake'].max()
            sinkDict = piEval(n, sourceDict)
            toComp2 = sinkDict['test']
            self.assertEqual(toComp, toComp2)
            n.report()

    def test_MAXDataframe(self):
        #Basic maxtest for Dataframe object
        with Network() as n:
            forex = pd.read_csv('forex.csv')
            sourceDict = {'forex': forex}  # here series are loaded
            df = seriesSource('forex')
            max(df, name="test")
            toComp = sourceDict['forex'].max()
            sinkDict = piEval(n, sourceDict)
            toComp2 = sinkDict['test']
            self.assertEqual(toComp.all(), toComp2.all())
            n.report()

    def test_SUMSeries(self):
        #Basic sumtest for Series object
        with Network() as n:
            sourceDict = {'fake': randomWalkSeries()}  # here series are loaded
            df = seriesSource('fake')
            sum(df, name="test")
            toComp = sourceDict['fake'].sum()
            sinkDict = piEval(n, sourceDict)
            toComp2 = sinkDict['test']
            self.assertEqual(toComp, toComp2)
            n.report()

    def test_SUMDataframe(self):
        #Basis sumtest for Dataframe object
        with Network() as n:
            forex = pd.read_csv('forex.csv')
            sourceDict = {'forex': forex}  # here series are loaded
            df = seriesSource('forex')
            sum(df, name="test")
            toComp = sourceDict['forex'].sum()
            sinkDict = piEval(n, sourceDict)
            toComp2 = sinkDict['test']
            self.assertEqual(toComp.all(), toComp2.all())
            n.report()

    def test_DELAYDataframe(self):
        #Basic delay test for Dataframe
        with Network() as n:
            forex = pd.read_csv("forex.csv")
            sourceDict = {'forex': forex}  # here series are loaded
            df = seriesSource('forex')
            sample = 3
            delay(df, sample, name='test')
            toComp = sourceDict['forex'].shift(periods=sample)
            sinkDict = piEval(n, sourceDict)
            toComp2 = sinkDict['test']
            self.assertIsNone(pd.testing.assert_frame_equal(toComp,toComp2))
            n.report()

    def test_DELAYSeries(self):
        #Basic delay test for Series
        with Network() as n:
            sourceDict = {'fake': randomWalkSeries()}  # here series are loaded
            df = seriesSource('fake')
            sample = 3
            delay(df, sample, name='test')
            toComp = sourceDict['fake'].shift(periods=sample)
            sinkDict = piEval(n, sourceDict)
            toComp2 = sinkDict['test']
            self.assertIsNone(pd.testing.assert_series_equal(toComp,toComp2))
            n.report()

