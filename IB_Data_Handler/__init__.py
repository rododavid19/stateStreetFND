import threading
import time
import datetime

from ibapi.wrapper import EWrapper
import ibapi.decoder
import ibapi.wrapper
from ibapi.common import *
from ibapi.ticktype import TickType, TickTypeEnum
from ibapi.comm import *
from ibapi.message import IN, OUT
from ibapi.client import EClient
from ibapi.connection import Connection
from ibapi.reader import EReader
from ibapi.utils import *
from ibapi.execution import ExecutionFilter
from ibapi.scanner import ScannerSubscription
from ibapi.order_condition import *
from ibapi.contract import *
from ibapi.order import *
from ibapi.order_state import *
import csv
import random
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib import style

import socket
import socketserver
from multiprocessing import Process
import queue
import numpy as np


#PNLPLOT
tempNow = datetime.datetime.now()
pnlVars = [0, 0.0, 0.0, 0.0]
#Daily_PnL_Vals = [0.0]
#Daily_PnL_Time = [tempNow]
Daily_PnL_Vals = []
Daily_PnL_Time = []
line1 = []
LineLock = []
#PNLPLOT


marketData = ""
changeCount = 0
loop_flag = False
dataArrived = False
order_ID = 0
demoAccountID = ""

locks = [""]
FOREX = [""]
flags = [""]
id_lock = threading.Lock()



class TestApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)


    def error(self, reqId:TickerId, errorCode:int, errorString:str):
        print("Error:", reqId, " ", errorCode, " ", errorString)


    def realtimeBar(self, reqId: TickerId, time:int, open_: float, high: float, low: float, close: float, volume: int, wap: float, count: int):
            super().realtimeBar(reqId, time, open_, high, low, close, volume, wap, count)
            global FOREX
            global order_ID
            global locks
            global data_lock
            global flags

            # data_lock.acquire()


            arrived = str(datetime.datetime.utcfromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S')) + " " + str(
                open_) + " " + str(high) + " " + str(low) + " " + str(close) + "$"
            print(" Open: " + str(open_) + " High: " + str(high) + " Low: " + str(low) + " Close: " + str(close))
            FOREX.insert(reqId, arrived)
            # if len(FOREX) == 0:
            #     FOREX.insert(reqId, arrived)
            # else:
            #     try:
            #         #FOREX.insert(reqId, " ")
            #         FOREX.insert(reqId, arrived)
            #         #print("FOREX: ", FOREX[reqId], '\n')
            #     except:
            #         print("Attempted to access Forex[", reqId, "] but it does not exist. It's current lenght is ",
            #               len(FOREX))
            # data_lock.release()

            # print("FOREX: ", FOREX[reqId] )
            print("Notifying  ", reqId)
            flags.insert(reqId, True)

            # lock = locks[reqId]
            # print("locking ", reqId)
            # with lock:
            #
            #     #data_lock.acquire()
            #     arrived = str(datetime.datetime.utcfromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S')) + " " + str(open_) + " " + str(high) + " " + str(low) + " " + str(close) + "$"
            #     print(" Open: " + str(open_) + " High: " + str(high) + " Low: " + str(low) + " Close: " + str(close))
            #     FOREX.insert(reqId, arrived)
            #     # if len(FOREX) == 0:
            #     #     FOREX.insert(reqId, arrived)
            #     # else:
            #     #     try:
            #     #         #FOREX.insert(reqId, " ")
            #     #         FOREX.insert(reqId, arrived)
            #     #         #print("FOREX: ", FOREX[reqId], '\n')
            #     #     except:
            #     #         print("Attempted to access Forex[", reqId, "] but it does not exist. It's current lenght is ",
            #     #               len(FOREX))
            #     #data_lock.release()
            #
            #     #print("FOREX: ", FOREX[reqId] )
            #     print("Notifying  ", reqId  )
            #     lock.notifyAll()

    def historicalData(self, reqId: int, bar: BarData):
       # print("HistoricalData. ReqId:", reqId, "BarData.", bar)
        global FOREX
        global order_ID
        global locks
        global flags

        # FOREX.insert(reqId, str(bar) + "$")
        # flags.insert(reqId, True)


        FOREX[reqId] = str(bar) + "$"
        flags[reqId] = True
        while(flags[reqId]):
            pass

        # print("Data Arrived")
        # print("locking ", reqId)
        # print(bar)
        # print("Data Recieved from " , reqId, " is", )
        # print("Data Recieved from ", reqId)
        # print("IB locking " , reqId)
        # lock = locks[reqId]
        # with lock:
        #     print("Data being inserted in " , reqId)
        #     FOREX[reqId] = str(bar) + "$"
        #     flags.insert(reqId, True)
        #     lock.notifyAll()





        # if reqId == 1:
        #     print("other served" , '\n')

        # lock = locks[reqId]
        # print("IB Locking ", reqId, "\n")
        # with lock:
        #     arrived = str(bar) + " "
        #     if len(FOREX) <= reqId:
        #         FOREX.append(arrived)
        #     else:
        # #        print("Querying ", reqId, " in FOREX")
        #         FOREX[reqId] += arrived
        #     try:
        #         print("FOREX: ", FOREX[reqId] , '\n')
        #     except:
        #         print("Attempted to access Forex[", reqId,"] but it does not exist. It's current lenght is " , len(FOREX)  )
        #   #  print("unlocking ", reqId)
        #     print("IB Unlocking ", reqId, "\n")
        #     lock.notify()



    def orderStatus(self, orderId:OrderId , status:str, filled:float,
                    remaining:float, avgFillPrice:float, permId:int,
                    parentId:int, lastFillPrice:float, clientId:int,
                    whyHeld:str, mktCapPrice: float):
        print(status)

    def openOrder(self, orderId:OrderId, contract:Contract, order:Order,
                  orderState:OrderState):
        print(orderState)

    def pnl(self, reqId: int, dailyPnL: float, unrealizedPnL: float, realizedPnL: float):
        global Daily_PnL_Time
        global Daily_PnL_Vals
        global line1
        super().pnl(reqId, dailyPnL, unrealizedPnL, realizedPnL)
        Daily_PnL_Vals.append(dailyPnL)
        Daily_PnL_Time.append(datetime.datetime.now())
        line1 = self.graphIt()
        print("Daily PnL. ReqId:", reqId, "DailyPnL:", dailyPnL,
              "UnrealizedPnL:", unrealizedPnL, "RealizedPnL:", realizedPnL)

    def graphIt(self):
        global line1
        global LineLock
        global Daily_PnL_Time
        global Daily_PnL_Vals
        if LineLock[0]:
            LineLock[0] = False
            # this is the call to matplotlib that allows dynamic plotting
            plt.ion()
            fig = plt.figure(figsize=(13, 6))
            ax = fig.add_subplot(111)
            # create a variable for the line so we can later update it
            line1, = ax.plot(Daily_PnL_Time, Daily_PnL_Vals, '-o', alpha=0.8)
            # update plot label/title
            plt.ylabel('USD')
            plt.title('Daily Profit and Loss')
            plt.show()
        else:
            line1.set_data(Daily_PnL_Time, Daily_PnL_Vals)
            plt.xlim(np.min(Daily_PnL_Time), np.max(Daily_PnL_Time))
            if np.min(Daily_PnL_Vals) <= line1.axes.get_ylim()[0] or np.max(Daily_PnL_Vals) >= line1.axes.get_ylim()[1]:
                plt.ylim(
                    [np.min(Daily_PnL_Vals) - np.std(Daily_PnL_Vals), np.max(Daily_PnL_Vals) + np.std(Daily_PnL_Vals)])
            plt.pause(.01)
        return line1


def interactiveBrokers(symbol:str, secType:str, currency:str, exchange:str, orderID:str):
    global line1
    global LineLock
    global demoAccountID
    LineLock.append(True)
    app = TestApp()
    app.connect("127.0.0.1", 7497, orderID)
    time.sleep(.0000000000001)  # TODO: report bug to IB repo. or daemon?
    # in production code, wait for nexOder call back before you continue
    contract = Contract()
    contract.symbol = symbol
    contract.secType = secType
    contract.currency = currency
    contract.exchange = exchange
    #app.reqMarketDataType(4)
    print("Requesting IB contract by id" , orderID )

    queryTime = (datetime.datetime.today() - datetime.timedelta(days=100)).strftime("%Y%m%d %H:%M:%S")
    app.reqHistoricalData(int(orderID), contract, queryTime,"1 M", "1 day", "MIDPOINT", 1, 1, False, [] )
    # app.reqRealTimeBars(int(orderID), contract, 5, "MIDPOINT", False, [])

    # TODO Make sure to change acctcode from being hardcoded
    # app.reqAccountUpdates(subscribe=True, acctCode=demoAccountID)
    # app.reqPnL(17001, demoAccountID, "")
    # app.pnl(pnlVars[0], pnlVars[1], pnlVars[2], pnlVars[3])
    # order = Order()
    # order.action = "BUY"
    # order.orderType = "LMT"
    # order.totalQuantity = 20
    # order.lmtPrice = 108.525
    # app.placeOrder(1, contract, order)
    app.run()



class ThreadedTCPRequestHandler(socketserver.BaseRequestHandler):

    def handle(self):
        global FOREX
        global dataArrived
        global order_ID
        global locks
        global id_lock
        global flags

        id_lock.acquire()
        local_id = order_ID
        print("NEW CLIENT: ", order_ID , '\n')
        # recieved = self.request[0].strip()
        # request = str.split(recieved.decode("utf-8"))
        # socket = self.request[1]

        client_request = self.request.recv(1024)
        print(client_request)

        IB_args = client_request[:client_request.__len__()]
        IB_args = IB_args.decode("utf-8")
        IB_args = str.split(IB_args)
        IB_args.append(local_id)
        print("starting IB with id ", local_id, '\n')
        IB_thread = threading.Thread(target=interactiveBrokers, args=IB_args)
        lock = threading.Condition()
        locks.insert(local_id, lock)
        order_ID += 1
        flags.insert(local_id, False)
        FOREX.insert(local_id, "")
        IB_thread.start()
        id_lock.release()

        while (True):
            if (flags[local_id] is True):
                print(local_id, " is sending ", FOREX[local_id])
                self.request.sendall(bytes(FOREX[local_id].encode("utf-8")))
                FOREX[local_id] = ""
                flags[local_id] = False

        # while(True):
        #     with lock:
        #         print( local_id, " is going to wait for IB.")
        #         lock.wait()
        #        # print(local_id, " is sending ", FOREX[local_id])
        #         self.request.sendall(bytes(FOREX[local_id].encode("utf-8")))
        #         FOREX[local_id] = ""
        #         flags[local_id] = False


        # while(True):
        #     with lock:
        #         lock.wait()
        #         print( local_id, " is sending ", FOREX[local_id] )
        #         self.request.sendall(bytes(FOREX[local_id].encode("utf-8"))):
        #         FOREX[local_id] = ""




class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):   # is declaration necessary?
    pass

def serverLaunch():
    HOST, PORT = "127.0.0.1", 19192
    server = ThreadedTCPServer( (HOST,PORT), ThreadedTCPRequestHandler)


    with server:
        ip, port = server.server_address
        server_thread = threading.Thread(target=server.serve_forever)
        server_thread.daemon = False # will terminate thread when main is done
        try:
            server_thread.start()
            print("Server started at {} port {}".format(HOST, PORT))
            while True: time.sleep(100)
        except (KeyboardInterrupt, SystemExit):
            server.shutdown()
            server.server_close()
            exit()

if __name__ == "__main__":
    serverLaunch()



# TODO: UDPServer to TCPServer  -- DONE.
# TODO: Broker function called by TCP handler  -- half/Done
# TODO: Golang request parser   -- DONE
# TODO: P&L plotting function.  https://matplotlib.org/gallery/style_sheets/dark_background.html#sphx-glr-gallery-style-sheets-dark-background-py
# TODO: FOREX data time synchronization?   -- QUESTION
#TODO: add Same Source checkers in prim_eval funcs in golang



