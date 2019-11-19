import unittest
from FND import *
from PandasBE import piEval
import pandas as pd
import numpy as np
from datetime import datetime, time, date, timedelta

class testNetwork_SMA_Strategy_Tests(unittest.TestCase):
    def test_SMA_Strategy_MINI(self):
        with Network() as n:
            forex = pd.read_csv("forex-mini.csv")
            sourceDict = {'forex': forex}  # here series are loaded
            df = seriesSource('forex')
            simple_2SMA_Strategy(df, shortWindow=10, longWindow=200, name="test")
            sinkDict = piEval(n, sourceDict)
            n.report()
