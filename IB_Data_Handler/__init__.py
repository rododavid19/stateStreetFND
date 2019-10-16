import threading
import time

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

import socket
import socketserver
from multiprocessing import Process


marketData = ""
changeCount = 0
loop_flag = False
lock = threading.Condition()
dataArrived = False

# import pdb; pdb.set_trace()
# import code; code.interact(local=locals())
# import code; code.interact(local=dict(globals(), **locals()))

class TestApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)


    def error(self, reqId:TickerId, errorCode:int, errorString:str):
        print("Error:", reqId, " ", errorCode, " ", errorString)

    def tickPrice(self, reqId:TickerId , tickType:TickType, price:float, attrib:TickAttrib):
       # print("Tick Price. Ticket ID:", reqId, "tickType:", TickTypeEnum.to_str(tickType), "Price:", price, end=' ')
        s = "Tick Price. Ticket ID:" + str(reqId)
        global marketData
        global lock
        global changeCount
        global dataArrived
        with lock:
            marketData += s
            changeCount += 1
            dataArrived = True
            lock.notify()



    def tickSize(self, reqId:TickerId, tickType:TickType, size:int):
       # print("Tick Size. Ticket ID:", reqId, "tickType:", TickTypeEnum.to_str(tickType), "Size:", size, end=' ')
        r = 4

    def historicalData(self, reqId: int, bar: BarData):
        s = "Historical Data. " + reqId, "Date:", bar.date, "Open", bar.open, "High:", bar.high, "Low:", bar.low, "Close:", bar.close, "Volume:", bar.volume, "Count", bar.barCount, "\n"
        marketData.append(s)
       # print("Historical Data. ", reqId, "Date:", bar.date, "Open", bar.open, "High:", bar.high, "Low:", bar.low, "Close:", bar.close, "Volume:", bar.volume, "Count", bar.barCount, "\n")


def interactiveBrokers():
    app = TestApp()
    app.connect("127.0.0.1", 7497, 0)
    time.sleep(.01)

    # in production code, wait for nexOder call back before you continue
    contract = Contract()
    contract.symbol = "AAPL"
    contract.secType = "STK"
    contract.currency = "USD"
    contract.exchange = "SMART"
    contract.primaryExchange = "NASDAQ"
    app.reqMarketDataType(4)
    app.reqMktData(1, contract, "", False, False, [])
    app.run()





def looper():
    #print("at looper")

    global marketData
    global lock
    global changeCount
    global dataArrived
    og_size = changeCount
    sentinel = False
    f = open("stock.txt", "w+")

    while(True):
        with lock:
            while not dataArrived:
                lock.wait()
            curr_size = changeCount
            if (curr_size != og_size):
                print("Market DataChanged! and the size is: " + str(curr_size))
                og_size = curr_size
                f.write(marketData)
            if (curr_size == 10):
                sentinel = True
            dataArrived = False
            lock.release()











def main():
 print()










class ThreadedUDPRequestHandler(socketserver.BaseRequestHandler):

    def handle(self):
        data = self.request[0].strip()
        socket = self.request[1]
        current_thread = threading.current_thread()
        print("{}: client: {}, wrote: {}".format(current_thread.name, self.client_address, data))
        socket.sendto(data.upper(), self.client_address)


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







    print()

if __name__ == "__main__":
    IB_thread = threading.Thread( target= interactiveBrokers)
    IB_thread.start()


    fakeLooper = threading.Thread(target= looper)
    fakeLooper.start()

    fakeLooper.join()
    print("looper done")
    print("IB api Done")



