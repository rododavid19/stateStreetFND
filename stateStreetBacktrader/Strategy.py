import backtrader as bt
import datetime
import random


class Strategy():
    def __init__(self):
        self.order = None  # keep track of our orders
        self.buyprice = None  # keeps track of our buy price
        self.sellprice = None
        self.trade = None  # store the current trade
        commission = 0.0
        self.comm = bt.CommissionInfo(commission=commission)  # object of commisionInfo class to get pnl
        # self.orderHist = []
        self.orderDict = {}  # stores a dictionary of each trade
        self.datelist = 0
        self.finalpnl = 0  # final pnl
        self.set_tradehistory(True)  # cerebro attribute -- ignore
        self.time_after_ml = datetime.time(10, 0, 0, 0)
        self.after_day = False  # to know when to stop trading
        self.datastatus = None
        self.signals = {}  # store the buy/sell signals from csv

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:  # checks if order was submitted
            self.order = order
            return
        if order.status in [order.Expired]:  # checks if order was expired
            self.log('BUY EXPIRED')
        if order.status in [order.Completed]:  # checks if order has been completed
            if order.isbuy():  # it is a buy order
                print('%s , BUY EXECUTED, PRICE: %.6f --- COST: %.6f --- COMM: %.6f' % ( order.executed.dt, order.executed.price, order.executed.value, order.
                executed.comm))  # records on terminal the order info
                self.buyprice = order.executed.price
                print('--------------------------------------------------------------------------------------------\n')
            else:  # sell order
                print('%s , SELL EXECUTED, PRICE: %.6f --- COST: %.6f --- COMM: % .6f' % ( order.executed.dt, order.executed.price, order.executed.price*1500000, order.executed.comm))
                self.bar_executed = len(self)
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:  # order has been cancelled
            self.log('ORDER CANCELLED/REJECTED')
        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            print('trade not closed')
            return
        self.finalpnl = self.finalpnl + trade.pnl
        self.log('Operation Profit, Gross: %.5f, Net: %.5f TotalPNL: %.2f' % (trade.pnl, trade.pnlcomm, self.finalpnl))
        # self.orderDict.pop(self.orderDict.items(0))
        print('--------------------------------------------------------------------------------------------\n')

    def next(self):
        dtstr = self.data.datetime.datetime().isoformat()
        pnl = self.comm.profitandloss(self.position.size, self.position.price, self.data.bid[0])
        '''''This is the print statement for bid/offer'''
        txt = '{}: {} - Bid = {} - Ask = {} - Signal = {} - probability = {}'.format(len(self), self.datetime.datetime(), self.data.bid[0], self.data.ask[0], self.data.predsignal[0], self.data.probability[0] )
        print(txt)
        pip = .0001
        if self.order:
            return
        if self.after_day == False:  # will open order when predictions are coming in between 9 and 10AM
            tradeid = random.randint(1, 10000)
            if self.data.predsignal[0] == 1 and self.data.probability[0] > .75:  # will issue a market order
                self.order = self.buy(tradeid=tradeid)
                case = {tradeid: self.order}
                self.orderDict.update(case)  # add to dictionary
                print('trade id ', tradeid, self.order.executed.dt)
            elif self.data.predsignal[0] == 0 and self.data.probability[0] < .35:  # issue a market order
                self.order = self.sell(tradeid=tradeid)
                case = {tradeid: self.order}
                self.orderDict.update(case)
                print('trade id ', tradeid, self.order.executed.dt)
        for key in list(self.orderDict):
            solopnl = self.comm.profitandloss(self.orderDict[key].executed.size, self.orderDict[key].executed.price, self.data.bid[0])
            # print('Trade ID: ', key, ' at ' , self.orderDict[key].executed.price, ' has a pnl of ', solopnl, self.orderDict[key].ordtype)
            # pip values to make profit if order was a buy
            pip_range1_pos = 1 * pip
            pip_range2_pos = 10 * pip
            # pip values if it was a short
            pip_range1_neg = -2 * pip
            pip_range2_neg = -10 * pip
            if self.orderDict[key].ordtype == 0:  # it was a buy order
                # range of profit values we will check
                limit_price_p = self.orderDict[key].executed.price + pip_range1_pos
                stop_price_p = self.orderDict[key].executed.price + pip_range2_pos
                if self.data.bid[0] >= limit_price_p and self.data.bid[0] <= stop_price_p:
                    # checks if bid price is within range
                    # print('-----------------------------Order was Sold at Limit', self.data.bid[0], key, ' - -----------------')
                    self.order = self.sell(tradeid=key)  # sells based on tradeid
                    del self.orderDict[key]  # remove order from dictionary
                    return
            elif self.orderDict[key].ordtype == 1:  # it was a sell order
                # range of profit values we will check
                limit_price_p = self.orderDict[key].executed.price + pip_range1_neg
                stop_price_p = self.orderDict[key].executed.price + pip_range2_neg
                if self.data.ask[0] <= limit_price_p and self.data.ask[0] >= stop_price_p:  #checks if bid price is within range
                # print('-----------------------------Order was Bought at Limit', self.data.ask[0],key, ' - -----------------')
                    self.order = self.buy(tradeid=key)  # sells based on tradeid
                    del self.orderDict[key]
                    return

    def start(self):
        return

    def stop(self):
        return

    def buy(self):
        return

    def sell(self):
        return
