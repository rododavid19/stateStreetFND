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
import matplotlib.pyplot as plt
import csv
import random

import socket
import socketserver
from multiprocessing import Process


marketData = ""
changeCount = 0
loop_flag = False
lock = threading.Condition()
dataArrived = False
order_ID = 0
FOREX = ""


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
            global dataArrived
            global FOREX
            global order_ID
            print(open_ )

            # arrived = open_
            # FOREX += arrived
            # dataArrived = True



def interactiveBrokers(symbol:str, secType:str, currency:str, exchange:str):
    app = TestApp()
    app.connect("127.0.0.1", 7497, 0)
    time.sleep(.0000000000001)  # TODO: report bug to IB repo. or daemon?
    # in production code, wait for nexOder call back before you continue
    contract = Contract()
    contract.symbol = symbol
    contract.secType = secType
    contract.currency = currency
    contract.exchange = exchange
    #app.reqMarketDataType(4)
    app.reqRealTimeBars(1, contract, 5, "MIDPOINT", False, [])
    app.run()



class ThreadedUDPRequestHandler(socketserver.BaseRequestHandler):

    # TODO: adapt handle to tick size and tick price
    def handle(self):
        global FOREX
        global dataArrived
        recieved = self.request[0].strip()
        request = str.split(recieved.decode("utf-8"))
        socket = self.request[1]
        current_thread = threading.current_thread()
        IB_args = request[:request.__len__()]
        IB_thread = threading.Thread(target=interactiveBrokers, args=IB_args)
        # to fix .000001 sleep bug, change IB_thread daemon here?
        IB_thread.start()


        while(True):
            if dataArrived == True:
                # print("Bundle Data ID: ", FOREX , " is leaving at time ", datetime.datetime.now().time()
                # , bytes(FOREX.encode("utf-8")).__sizeof__() )
                print("FOREX: ", FOREX)
                # TODO: cut excess data if appended and pass if data is irrelevant (close, high )
                socket.sendto(bytes(FOREX.encode("utf-8")), self.client_address)
                FOREX = ""
            dataArrived = False




class ThreadedUDPServer(socketserver.ThreadingMixIn, socketserver.UDPServer):   # is declaration necessary?
    pass

def serverLaunch():

    HOST, PORT = "127.0.0.1", 19192
    server = socketserver.UDPServer( (HOST,PORT), ThreadedUDPRequestHandler)

    with server:
        ip, port = server.server_address

        server_thread = threading.Thread(target=server.serve_forever)
        server_thread.daemon = True # will terminate thread when main is done

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




