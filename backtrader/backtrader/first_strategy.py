from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import datetime  # For datetime objects
import os.path  # To manage paths
import sys  # To find out the script name (in argv[0])
from FND import *
from PandasBE import piEval
# Import the backtrader platform
import backtrader as bt
# Create a Stratey



class myFirstIndicator(bt.Indicator):
    lines = ('AskPrice','BidPrice','Output',)
    params = (('period',1),)

    def __init__(self):
        self.lines.AskPrice = self.data.high
        self.lines.BidPrice = self.data.low
        self.lines.Output = self.lines.AskPrice <= self.lines.BidPrice


class TestStrategy(bt.Strategy):

    def log(self, txt, dt=None):
        ''' Logging function for this strategy'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self, sinkDict):
        # Keep a reference to the "close" line in the data[0] dataseries
        self.dataclose = self.datas[0].close
        es1 = sinkDict['Ema1']
        emaRes2 = sinkDict['Ema2']

    def next(self):
        # Simply log the closing price of the series from the reference
        self.log('Close, %.2f' % self.dataclose[0])
        if self.emaRes1 > self.emaRes2:
            self.buy()


if __name__ == '__main__':
    # Create a cerebro entity
    cerebro = bt.Cerebro()

    # Datas are in a subfolder of the samples. Need to find where the script is
    # because it could have been called from anywhere
    modpath = os.path.dirname(os.path.abspath(sys.argv[0]))
    datapath = os.path.join(modpath, 'orcl-1995-2014.txt')

    data = bt.feeds.GenericCSVData(
        dataname='forex.csv',
        nullvalue=0.0,
        dtformat=("%Y-%m-%d %H:%M"),
        datetime=0,
        time=-1,
        high=1,
        low=2,
        open=3,
        close=4,
        volume=5,
        openinterest=-1
    )

    with Network() as n:
        forex = pd.read_csv("forex.csv")
        sourceDict = {'forex': forex}  # here series are loaded
        df = seriesSource('forex')
        ema(df, name='Ema1', span=2)
        ema(df, name='Ema2', span=10)
        sinkDict = piEval(n, sourceDict)




    # Add a strategy
    #cerebro.addstrategy(TestStrategy, sinkDict)


    # Create a Data Feed

    # Add the Data Feed to Cerebro
    cerebro.adddata(data)
    cerebro.add_signal(bt.SIGNAL_LONG, myFirstIndicator)

    # Set our desired cash start
    cerebro.broker.setcash(100000.0)

    # Print out the starting conditions
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

    # Run over everything
    cerebro.run()

    # Print out the final result
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
