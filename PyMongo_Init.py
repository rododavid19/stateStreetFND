#######################
from pymongo import MongoClient
import os
import hashlib
from arctic import Arctic
import math
import datetime
import pprint
import glob
from pathlib import Path
import pandas as pd
import csv
import numpy as np
import time
import inspect
import sys

#######################

def main():
        client = MongoClient()
        histData = client.HistoricalData
        #HistImporter(histData)
        #test200201 = monthToCollection(13)
        store = Arctic('localhost')
        store.initialize_library('HistTickStore')
        histlibrary = store['HistTickStore']
        data2 = histlibrary.read('2019-05')
        hashPrimitiveAndStore(inspect.getsource(realTimeUpdate), 0,0)
        rtData = client.RealTimeData
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(getHistTickData("2001", "2001", histData))
        networkHashExample()
        # if not "HistoricalData" in client.list_database_names():

        if not "RealTimeData" in client.list_database_names():
                realTimeUpdate()
        return 1


# TODO Figure out where to get update data from, and the interval for updating real time data
def realTimeUpdate():

        return 1

def networkHashExample():
        d0 = np.array([[0.0, 1.2, 2.4], [4.1, 5.0, 6.9]])
        d1 = np.array([[7.5, 8.2, 9.99], [10.45, 11.30, 12.11]])
        m0 = d0 + d1
        m1 = d0 - d1
        m2 = d0 / d1

        d0B = d0.tobytes()
        d1B = d1.tobytes()
        m0B = m0.tobytes()
        m1B = m1.tobytes()
        m2B = m2.tobytes()

        d0H = hashlib.sha256(d0B).hexdigest()
        d1H = hashlib.sha256(d1B).hexdigest()
        m0H = hashlib.sha256(m0B).hexdigest()
        m1H = hashlib.sha256(m1B).hexdigest()
        m2H = hashlib.sha256(m2B).hexdigest()
        nH = hashlib.sha256(d0B + d1B + m0B + m1B + m2B).hexdigest()

        networkHash = {
                'data_0': d0H,
                'data_1': d1H,
                'module_0': m0H,
                'module_1': m1H,
                'module_2': m2H,
                'network_0': nH
        }

        print(networkHash)

def hashPrimitiveAndStore(primitive, input_data, results):
        # Db Entry should be  (HASH, Key)
        #primitive variable is inspect.getsource(function)
        #input_data is an np_array used input to the primitive
        #results is an np_array containing the resulting data from running the primitive on the data
        input_d = np.array([[0.0, 1.2, 2.4], [4.1, 5.0, 6.9]])
        result_d = np.array([[7.5, 8.2, 9.99], [10.45, 11.30, 12.11]])

        client = MongoClient()
        currDB = client.Hashed
        currColl = currDB["Primitives"]
        func = primitive
        func_bytes = func.encode()
        func_hash_obj = hashlib.sha256(func_bytes)


        input_bytes = input_d.tobytes()
        input_hash_object = hashlib.sha256(input_bytes)
        input_hex_dig = input_hash_object.hexdigest()


        func_hex_dig = func_hash_obj.hexdigest() + input_hex_dig

        objToStore = {
                "Prim+InputData": func_hex_dig,
                "Results": result_d
        }
        currColl.insert_one(objToStore)



# This function should only be called once, fills up the historical database with 6gb(close to 2.5GB once in DB) worth of EURUSD tickdata from 01/01/01 -> 05/31/19
def HistImporter(histData):
        # directory = os.path.join("C:\\Users\\rofor\\PycharmProjects\\PyMongoStateStreet\\HISTORICALDATA\\2001","path")
        basePath = Path(__file__).parent
        path = "C:/Users/rofor/PycharmProjects/PyMongoStateStreet/HISTORICALDATA/*/*.csv"
        filepath = str((basePath / "../HISTORICALDATA/2001/*.csv").resolve())
        store = Arctic('localhost')
        store.initialize_library('HistTickStore')
        library = store['HistTickStore']
        month = 1

        for file in glob.glob(path):
                #221 total months worth of data
                csvFile = open(file, 'r')
                currColl = monthToCollection(month)
                data = pd.read_csv(csvFile, names=["DateTime","BidQuote", "AskQuote", "Volume"])
                library.write(currColl, data)
                csvFile.close()
                month = month + 1
        return None


#Returns string representing YYYY-MM of the month in question
def monthToCollection(month):
        if month % 12 == 0:
            year = int(month/12)
            mon = 12
        else:
                year = (math.floor(month / 12) + 1)
                mon = (month % 12)
        years = str(year)
        mons = str(mon)
        toRet = ""
        if year < 10:
                toRet = "200"+years
        else:
                toRet = "20"+years
        if mon < 10:
                toRet = toRet+"-0"+mons
        else:
                toRet = toRet+"-"+mons
        return toRet



#This function takes a row from the csv.reader and returns a dict object representing that tick data point
def tickRowToDict(row):
        Volume = row.pop()
        AskQuote = row.pop()
        BidQuote = row.pop()
        DateTime = row.pop()
        Day = DateTime[6:8]
        Hour = DateTime[9:11]
        Minute = DateTime[11:13]
        toRet = {"DateTime": DateTime, "Day": Day, "Hour": Hour, "Minute": Minute, "BidQuote": float(BidQuote), "AskQuote": float(AskQuote), "Volume": int(Volume)}
        return toRet


# TODO: 1) Query into DB 2) Get correct amount of data
def getHistTickData(start, end, histData):
        # start and end strings must be the same number of characters
        # "YYYY" : "YYYYMM" : "YYYYMMDD" : "YYYYMMDD HH" : "YYYYMMDD HHMM" : "YYYYMMDD HHMMSS" : "YYYYMMDD HHMMSSNNN" will be the supported formats for data retrieval
        toRet = []
        if len(start) != len(end):
                print("Start and end must be in the same format, e.g. YYYYY , YYYYMM, etc.")
                return ""
        if len(start) == 4: # YYYY MODE
                numYears = int(end) - int(start)
                if numYears == 0:
                        for i in range(1, 13):
                                currColStr = start + "-"
                                if i < 10:
                                        currColStr = currColStr + "0" + str(i)
                                else:
                                        currColStr = currColStr + str(i)
                                currCol = histData[currColStr]
                                for document in currCol.find():
                                        toRet.append(document)
                                toRet.append("New Month")
                        return toRet
                        # return the full start year of data
        if len(start) == 6: # YYYYMM MODE
                numYears = int(end[0:4]) - int(start[0:4])
                numMonths = int(end[4:]) - int(start[4:])
                totalNumMonths = numYears*12 + numMonths
                if totalNumMonths == 0:
                        #return the full start month of data
                        temp = ""
        if len(start) == 8: # YYYYMMDD MODE
                numYears = int(end[0:4]) - int(start[0:4])
                numMonths = int(end[4:6]) - int(start[4:6])
                totalNumMonths = numYears * 12 + numMonths
                sMonthDays = int(start[6:])
                eMonthDays = int(end[6:])
                if(totalNumMonths == 0):
                        if sMonthDays - eMonthDays == 0:
                                # Return the full starting day of data
                                temp == ""
        if len(start) == 11: # YYYYMMDD HH MODE
                numYears = int(end[0:4]) - int(start[0:4])
                numMonths = int(end[4:6]) - int(start[4:6])
                totalNumMonths = numYears * 12 + numMonths
                sMonthDays = int(start[6:8])
                eMonthDays = int(end[6:8])
                sHour = int(start[9:11])
                eHour = int(end[9:11])
                if (totalNumMonths == 0):
                        if sMonthDays - eMonthDays == 0:
                                if sHour == eHour:
                                        # Return the full starting hour of data
                                        temp = ""

        if len(start) == 13: # YYYYMMDD HHMM MODE
                numYears = int(end[0:4]) - int(start[0:4])
                numMonths = int(end[4:6]) - int(start[4:6])
                totalNumMonths = numYears * 12 + numMonths
                sMonthDays = int(start[6:8])
                eMonthDays = int(end[6:8])
                sHour = int(start[9:11])
                eHour = int(end[9:11])
                sMin = int(start[11:13])
                eMin = int(end[11:13])
                if (totalNumMonths == 0):
                        if sMonthDays - eMonthDays == 0:
                                if sHour == eHour:
                                        if sMin == eMin:
                                                # Return the full starting minute of data
                                                temp = ""

        return ""

main()


