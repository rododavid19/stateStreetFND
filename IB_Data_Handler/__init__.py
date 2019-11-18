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


marketData = ""
changeCount = 0
loop_flag = False
dataArrived = False
order_ID = 0

locks = [ ]
FOREX = [ ]
data_lock = threading.Lock()



class TestApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)


    def error(self, reqId:TickerId, errorCode:int, errorString:str):
        print("Error:", reqId, " ", errorCode, " ", errorString)

    def tickPrice(self, reqId:TickerId , tickType:TickType, price:float, attrib:TickAttrib):
        global dataArrived
        global FOREX
        global order_ID
        dataArrived = True
        arrived = TickTypeEnum.to_str(tickType) + "Price: " + str(price) + " "
        # Tick Price. Ticket ID: " + str(reqId) + "
        FOREX += arrived
        order_ID += 1

    def realtimeBar(self, reqId: TickerId, time:int, open_: float, high: float, low: float, close: float, volume: int, wap: float, count: int):
            super().realtimeBar(reqId, time, open_, high, low, close, volume, wap, count)
            global FOREX
            global order_ID
            global locks
            global data_lock

            lock = locks[reqId]
            print("locking ", reqId)
            with lock:
                arrived = str(open_) + " " + str(high) + " " + str(low) + " " + str(close)
                print(" Open: " + str(open_) + " High: " + str(high) + " Low: " + str(low) + " Close: " + str(close))


                data_lock.acquire()
                if len(FOREX) == 0:
                    FOREX.insert( reqId,arrived)
                else:
                    try:
                        FOREX.insert(reqId, arrived)
                        #print("FOREX: ", FOREX[reqId], '\n')
                    except:
                        print("Attempted to access Forex[", reqId, "] but it does not exist. It's current lenght is ",
                              len(FOREX))
                data_lock.release()

                #print("FOREX: ", FOREX[reqId] )
                print("unlocking ", reqId)
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
    app.run()



class ThreadedUDPRequestHandler(socketserver.BaseRequestHandler):

    def handle(self):
        global FOREX
        global dataArrived
        global order_ID
        global locks
        local_id = order_ID
        print("NEW CLIENT: ", order_ID , '\n')
        recieved = self.request[0].strip()
        request = str.split(recieved.decode("utf-8"))
        socket = self.request[1]

        IB_args = request[:request.__len__()-1]
        req_id = order_ID
        IB_args.append(req_id)
        print("starting IB with id ", req_id, '\n')
        IB_thread = threading.Thread(target=interactiveBrokers, args=IB_args)
        clientSink_args = [local_id, socket, self.client_address ]
        clientSink_thread = threading.Thread(target=client_sink, args=clientSink_args)
        lock = threading.Condition()
        locks.append(lock)
        IB_thread.start()
        clientSink_thread.start()
        order_ID += 1


def client_sink(id:int, socket, client_address ):
    global locks
    global FOREX
    global data_lock
    lock = locks[id]
    while(True):
        with lock:
            print("Sender Locking ", id, " at time: " , datetime.datetime.today(),'\n',)
            lock.wait()
            print("Sender UNLOCKING ", id,  " at time: " , datetime.datetime.today(), '\n',)
            data_lock.acquire()
            try:
                print(id, " is SENDING ", FOREX[id], '\n')
                socket.sendto(bytes(FOREX[id].encode("utf-8")), client_address)
            except:
                print(" FOREX index incorrect attempted on index: ", id, '\n' )
            FOREX[id] = ""
            data_lock.release()


class ThreadedUDPServer(socketserver.ThreadingMixIn, socketserver.UDPServer):   # is declaration necessary?
    pass

def serverLaunch():
    HOST, PORT = "127.0.0.1", 19192
    server = socketserver.UDPServer( (HOST,PORT), ThreadedUDPRequestHandler)

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




