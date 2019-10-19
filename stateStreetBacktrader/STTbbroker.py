import backtrader as bt


def buy():
    return


def sell():
    return


def submit():
    return


def _checksubmitted():
    return


def next():
    return


def try_exec_Intraday():
    return


def _execute():
    return


def market_execute_intraday(self, order, pbid, pask, pcreated, dtcoc):
    #intraday = 0
    psize = order.executed.remsize #size of our fill order
    buyafter = bt.num2date(order.timedelay) #hold over time period 500 ms
    if dtcoc >= buyafter: #time delay
        if order.isbuy():
            price = pask #price at dtcoc
            print('Market Buy Order')
            print('Wait till ', bt.num2date(order.timedelay), ' to buy')
            self._execute(order, ago=0, price=price, dtcoc=dtcoc) #execute method
        else:
            price = pbid
            print('Market Sell Order')
            print('Wait till ',bt.num2date(order.timedelay), ' to sell')
            self._execute(order, ago=0, price=price, dtcoc=dtcoc)


def limit_execute_intraday(self, order, pbid, pask, pcreated, dtcoc):
    psize = order.executed.remsize
    buyafter = bt.num2date(order.timedelay)
    if dtcoc >= buyafter:
        if order.isbuy():
            # print('Limit Buy Order for a price at ', pcreated)
            # print('Wait till ',bt.num2date(order.timedelay), ' to buy')
            # print('valid for:', bt.num2date(order.valid))
            if (pask == pcreated): #<=
                print('yay!!! Hooray you bought at ', pcreated)
                self._execute(order, ago=0, price=pcreated, dtcoc=dtcoc)
            else:
                print('Waiting....................... \n')
        else:
            # print('Limit Buy Order for a price at ', pcreated)
            # print('valid for:', bt.num2date(order.valid))
            # print('Wait till ',bt.num2date(order.timedelay), ' to sell')
            if (pbid == pcreated): #>=
                print('Limit Sell Order')
                self._execute(order, ago=0, price=pcreated, dtcoc=dtcoc)


def stop_execute_intraday(self, order, pbid, pask, pcreated, dtcoc):
    buyafter = bt.num2date(order.timedelay)
    if dtcoc >= buyafter:
        if order.isbuy():
            if pask == pcreated: #
                print('Stop price has been triggered')
                order.exectype = Order.Market
                self.market_execute_intraday(order, pbid, pask, pcreated, dtcoc) #market order
                self._try_exec_IntraDay(order)
            else:
                if pbid == pcreated:
                    print('Stop price has been triggered')
                    self.market_execute_intraday(order, pbid, pask, pcreated, dtcoc)


def stoplimit_execute_intraday(self, order, pbid, pask, pcreated, plimit, dtcoc):
    print('Stop Limit Order')
    buyafter = bt.num2date(order.timedelay)
    if dtcoc >= buyafter:
        if order.isbuy():
            if pask >= pcreated:
                order.triggered = True #order has been triggered to submit limit order
                print('Stop Limit Order2 ', plimit)
                self.limit_execute_intraday(order, pbid, pask, plimit, dtcoc)
            else:
                if pbid <= pcreated:
                    order.triggered = True
                    print('Stop Loss Triggered ===> Limit Order')
                    self.limit_execute_intraday(order, pbid, pask, plimit, dtcoc)