class Order(object):
    def __init__(self, data, size, price, plimit, exectype, valid, tradeid, oco, trailamount,
                 trailpercent, parent, transmit, **kwargs):
        self.size = size


class BuyOrder(Order):
    def __init__(self, data, size, price, plimit, exectype, valid, tradeid, oco, trailamount,
                 trailpercent, parent, transmit, **kwargs):
        super().__init__(self, data, size, price, plimit, exectype, valid, tradeid, oco, trailamount,
                 trailpercent, parent, transmit, **kwargs)


class SellOrder(Order):
    def __init__(self, data, size, price, plimit, exectype, valid, tradeid, oco, trailamount,
                 trailpercent, parent, transmit, **kwargs):
        super().__init__(self, data, size, price, plimit, exectype, valid, tradeid, oco, trailamount,
                         trailpercent, parent, transmit, **kwargs)


class BuyBracketOrder(Order):
    def __init__(self, data, size, price, plimit, exectype,
                 valid, tradeid, trailamount, trailpercent, oargs, stopprice, stopexec, stopargs,
                 limitprice, limitexec, limitargs, **kwargs):
        self.size = size


def _execute():
    return


def next():
    return
