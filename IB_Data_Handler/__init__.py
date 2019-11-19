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

import socket
import socketserver
from multiprocessing import Process
import queue


marketData = ""
changeCount = 0
loop_flag = False
dataArrived = False
order_ID = 0

locks = [""]
FOREX = [""]
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

            lock = locks[reqId]
            print("locking ", reqId)
            with lock:

                #data_lock.acquire()
                arrived = str(datetime.datetime.utcfromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S')) + " " + str(open_) + " " + str(high) + " " + str(low) + " " + str(close) + "$"
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
                #data_lock.release()

                #print("FOREX: ", FOREX[reqId] )
                print("Notifying  ", reqId  )
                lock.notifyAll()

    def historicalData(self, reqId: int, bar: BarData):
       # print("HistoricalData. ReqId:", reqId, "BarData.", bar)
        global FOREX
        global order_ID
        global locks
        if reqId == 1:
            print("other served" , '\n')

        lock = locks[reqId]
        print("IB Locking ", reqId, "\n")
        with lock:
            arrived = str(bar) + " "
            if len(FOREX) <= reqId:
                FOREX.append(arrived)
            else:
        #        print("Querying ", reqId, " in FOREX")
                FOREX[reqId] += arrived
            try:
                print("FOREX: ", FOREX[reqId] , '\n')
            except:
                print("Attempted to access Forex[", reqId,"] but it does not exist. It's current lenght is " , len(FOREX)  )
          #  print("unlocking ", reqId)
            print("IB Unlocking ", reqId, "\n")
            lock.notify()



    def orderStatus(self, orderId:OrderId , status:str, filled:float,
                    remaining:float, avgFillPrice:float, permId:int,
                    parentId:int, lastFillPrice:float, clientId:int,
                    whyHeld:str, mktCapPrice: float):
        print(status)


def interactiveBrokers(symbol:str, secType:str, currency:str, exchange:str, orderID:str):
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

    # queryTime = (datetime.datetime.today() - datetime.timedelta(days=179)).strftime("%Y%m%d %H:%M:%S")
    # app.reqHistoricalData(int(orderID), contract, queryTime,"1 M", "1 day", "MIDPOINT", 1, 1, False, [] )
    app.reqRealTimeBars(int(orderID), contract, 5, "MIDPOINT", False, [])
    # order = Order()
    # order.action = "BUY"
    # order.orderType = "MKT"
    # order.totalQuantity = 20
    # #order.lmtPrice = 0.68095
    # app.placeOrder(69, contract, order)
    app.run()



class ThreadedTCPRequestHandler(socketserver.BaseRequestHandler):

    def handle(self):
        global FOREX
        global dataArrived
        global order_ID
        global locks
        global id_lock


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
        IB_thread.start()
        id_lock.release()

        while(True):
            with lock:
                lock.wait()
                print( local_id, " is sending ", FOREX[local_id] )
                self.request.sendall(bytes(FOREX[local_id].encode("utf-8")))
                FOREX[local_id] = ""



def client_sink(id:int, socket, client_address ):
    global locks
    global FOREX
    global data_lock
    lock = locks[id]

    while(True):
        with lock:
            #print("Sender Locking ", id, " at time: " , datetime.datetime.today(),'\n',)
            lock.wait()
            data_lock.acquire()
            #print("Sender UNLOCKING ", id,  " at time: " , datetime.datetime.today(), '\n',)
            try:

                print(id, " is SENDING ", FOREX[id], '\n')
                socket.sendto(bytes(FOREX[id].encode("utf-8")), client_address)
            except:
                print(" FOREX index incorrect attempted on index: ", id, " and FOREX has, " , FOREX )
            data_lock.release()


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



# TODO: UDPServer to TCPServer
# TODO: Broker function called by TCP handler
# TODO: Golang request parser
# TODO: P&L plotting function.  https://matplotlib.org/gallery/style_sheets/dark_background.html#sphx-glr-gallery-style-sheets-dark-background-py
# TODO: FOREX data time synchronization?
#TODO: add Same Source checkers in prim_eval funcs in golang



