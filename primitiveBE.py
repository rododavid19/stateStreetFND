import pandas as pd
import numpy as np
import FND
from pandas.core.window import _Rolling_and_Expanding
from datetime import datetime, timedelta, time

#TODO: FOR ALL add wether Series or DF. Then also check how data was passed??

#TODO: to add support for non-series constant then just add checker before pd operation and calc. result as necessary


# Add(a, b sum)
def ADD(p):
        a = p.arguments["a"].parent.arguments.data
        b = p.arguments["b"].parent.arguments.data

        if type(a) and type(b) is pd.DataFrame:
            p.arguments.data = pd.DataFrame(a + b)
        if type(a) and type(b) is pd.Series:
            p.arguments.data = pd.Series(a + b)


# Subtract(a, b, difference)
def SUBTRACT(p):
        a = p.arguments["a"].parent.arguments.data
        b = p.arguments["b"].parent.arguments.data
        bool_lock = False
        for col in a.columns:
            if a[col].dtype != bool or b[col].dtype != bool:
                bool_lock = False
                break
            else:
                bool_lock = True
        if bool_lock:
            a2 = a.astype(int)
            b2 = b.astype(int)
            if type(a) and type(b) is pd.DataFrame:
                p.arguments.data = pd.DataFrame(a2 - b2)
            if type(a) and type(b) is pd.Series:
                p.arguments.data = pd.Series(a2 - b2)
            return
        if type(a) and type(b) is pd.DataFrame:
            p.arguments.data = pd.DataFrame(a - b)
        if type(a) and type(b) is pd.Series:
            p.arguments.data = pd.Series(a - b)

    # Multiply(a, b, product)
def MULTIPLY(p):
        a = p.arguments["a"].parent.arguments.data
        b = p.arguments["b"].parent.arguments.data

        if type(a) and type(b) is pd.DataFrame:
            p.arguments.data = pd.DataFrame(a * b)
        if type(a) and type(b) is pd.Series:
            p.arguments.data = pd.Series(a * b)

    # Divide(a, b, quotient, remainder=None)
def DIVIDE(p):
        a = p.arguments["a"].parent.arguments.data #TODO do a try catch here in case of zeros
        b = p.arguments["b"].parent.arguments.data

        if type(a) and type(b) is pd.DataFrame:
            p.arguments.data = pd.DataFrame(a / b)
        if type(a) and type(b) is pd.Series:
            p.arguments.data  = pd.Series(a / b)

    # SMA(s, window, ema)
def SMA(p):
        s = p.arguments["series"].parent.arguments.data
        if type(s) is pd.DataFrame or pd.Series:
            window = p.arguments['window']
            p.arguments.data = s.rolling(window=window).mean()

    # EMA(s, span, ema)
def EMA(p):
        s = p.arguments['series'].parent.arguments.data
        if type(s) is pd.DataFrame or type(s) is pd.Series:
            span = p.arguments['span']
            p.arguments.data = s.ewm(span=span).mean()

def NEG(p):
    a = p.arguments["a"].parent.arguments.data
    if type(a) is pd.DataFrame:
        p.arguments.data = pd.DataFrame(-a)
    if type(a) is pd.Series:
        p.arguments.data = pd.Series(-a)


def ABS(p):
    a = p.arguments["a"].parent.arguments.data
    if type(a) is pd.DataFrame:
        p.arguments.data = pd.DataFrame(a).abs()
    if type(a) is pd.Series:
        p.arguments.data = pd.Series(a).abs()

def REMAINDER(p):
    a = p.arguments["a"].parent.arguments.data
    b = p.arguments["b"].parent.arguments.data

    if type(a) and type(b) is pd.DataFrame:
        p.arguments.data = pd.DataFrame(a % b)
    if type(a) and type(b) is pd.Series:
        p.arguments.data = pd.Series(a % b)

def FLOOR(p):
    #Compute the largest integer less than or equal to a / b.
    #TODO make test suite
   a = p.arguments["a"].parent.data
   b = p.arguments["b"].parent.data
   c = pd.series(a / b)
   result = np.floor(c)
   p.arguments["result"] = result

def CEILING(p):
    #Compute the smallest integer greater than or equal to a / b.
    #TODO Make test suite
   a = p.arguments["a"].parent.data
   b = p.arguments["b"].parent.data
   c = pd.series( a / b)
   result = np.ceil(c)
   p.arguments["result"] = result

def LOG(p):
    a = p.arguments["a"].parent.arguments.data
    if type(a) is pd.DataFrame or pd.Series:
        p.arguments.data = np.log(a)

## COMPARISON OPERATIONS ##

def LT(p):
    a = p.arguments["a"].parent.arguments.data
    b = p.arguments["b"].parent.arguments.data

    if type(a) and type(b) is pd.DataFrame:
        p.arguments.data = pd.DataFrame.lt(a, b)
    if type(a) and type(b) is pd.Series:
        p.arguments.data = pd.Series.lt(a, b)


def LE(p):
    a = p.arguments["a"].parent.arguments.data
    b = p.arguments["b"].parent.arguments.data

    if type(a) and type(b) is pd.DataFrame:
        p.arguments.data = pd.DataFrame.le(a,b)
    if type(a) and type(b) is pd.Series:
        p.arguments.data = pd.Series.le(a, b)

def EQ(p):
    a = p.arguments["a"].parent.arguments.data
    b = p.arguments["b"].parent.arguments.data
    if type(a) and type(b) is pd.DataFrame:
        p.arguments.data = pd.DataFrame.eq(a, b)
    if type(a) and type(b) is pd.Series:
        p.arguments.data = pd.Series.eq(a, b)

def GE(p):
    a = p.arguments["a"].parent.arguments.data
    b = p.arguments["b"].parent.arguments.data

    if type(a) and type(b) is pd.DataFrame:
        p.arguments.data = pd.DataFrame.ge(a, b)
    if type(a) and type(b) is pd.Series:
        p.arguments.data = pd.Series.ge(a, b)

def NE(p):
    a = p.arguments["a"].parent.arguments.data
    b = p.arguments["b"].parent.arguments.data

    if type(a) and type(b) is pd.DataFrame:
        p.arguments.data = pd.DataFrame.ne(a, b)
    if type(a) and type(b) is pd.Series:
        p.arguments.data = pd.Series.ne(a, b)

def GT(p):
    a = p.arguments["a"].parent.arguments.data
    b = p.arguments["b"].parent.arguments.data

    if type(a) and type(b) is pd.DataFrame:
        p.arguments.data = pd.DataFrame.gt(a, b)
    if type(a) and type(b) is pd.Series:
        p.arguments.data = pd.Series.gt(a, b)


def PRICEFLOOR(p):
    a = p.arguments["a"].parent.arguments.data
    if type(a) is pd.DataFrame or pd.Series:
        p.arguments.data = np.floor(a)


def PRICECEILING(p):
    a = p.arguments["a"].parent.arguments.data
    if type(a) is pd.DataFrame or pd.Series:
        p.arguments.data = np.ceil(a)

def ANDDF(p):
    a = p.arguments['a'].parent.arguments.data
    b = p.arguments['b'].parent.arguments.data
    p.arguments.data = a & b

def ORDF(p):
    a = p.arguments['a'].parent.arguments.data
    b = p.arguments['b'].parent.arguments.data
    p.arguments.data = a | b

def DFBOOLTOINT(p):
    a = p.arguments['a'].parent.arguments.data
    toRet = a.astype(int)
    p.arguments.data = toRet

def QUANTITYTODF(p):
    a = p.arguments['a'].parent.arguments.data
    quantity = p.arguments['quantity']
    toRet = a.copy()
    for col in toRet.columns:
        if col == toRet.columns[0]:
            continue
        toRet[col].values[:] = quantity
    p.arguments.data = toRet

def ADDTICKS(p):
   #Todo Figure out how to add ticks to series objects
   a = p.arguments["a"].parent.data
   b = p.arguments["b"].parent.data
   result = pd.Series.gt(a, b)
   p.arguments.data = result


def STDEV(p):
    #TODO: add exception for no series or dataframe object?
    a = p.arguments["series"].parent.arguments.data
    if type(a) is pd.Series:
        p.arguments.data = a.std()
    elif type(a) is pd.DataFrame:
        window = p.arguments['window']
        if not(type(window) is int or str):
            raise Exception("'window must be either a string an int'")
        colNames = list(a)
        if type(window) is str:
            if not(window in colNames):
                raise Exception("'window' must be a valid column name in the dataframe")
            p.arguments.data = a.loc[:, window].std()
        elif type(window) is int:
            p.arguments.data = a.std(axis=window)

def MIN(p):
    a = p.arguments["series"].parent.arguments.data
    if type(a) is pd.DataFrame or pd.Series:
        window = p.arguments['window']
        p.arguments.data = a.rolling(window=window).min()


def MAX(p):
    a = p.arguments["series"].parent.arguments.data
    if type(a) is pd.DataFrame or pd.Series:
        window = p.arguments['window']
        p.arguments.data = a.rolling(window=window).max()


def SUM(p):
    a = p.arguments["series"].parent.arguments.data
    if type(a) is pd.DataFrame or pd.Series:
        window = p.arguments['window']
        p.arguments.data = a.rolling(window=window).sum()

def DELAY(p):           # is 'shift()' in pandas.series lib okay for this??
    series = p.arguments["series"].parent.arguments.data
    #dly = p.arguments["dly"].parent.argument.data
    samples = p.arguments['samples']
    if type(series) is pd.Series:
        result = pd.Series(series).shift(periods=samples)
    elif type(series) is pd.DataFrame:
        result = pd.DataFrame(series).shift(periods=samples)
    #result = series.shift(periods=samples)  # TODO: check if there is 'axis' value and evlaute with that parameter if needed, otherwise do general op
    p.arguments.data = result



#DATAFRAME OPERATIONS
def GETCOLUMNS(p):
    #Return one or more columns from a DataFrame, The columns parameter is interpreted as in Pandas (simplified). If columns is a string, that column is returned as a Series object. If columns is a list of strings, then a DataFrame of those columns is returned.
    cols = p.arguments["colNames"]
    if isinstance(cols, list):
        if not(all(isinstance(item, str) for item in cols)):
            raise TypeError(
                ('Error, "colNames" must be either a string or a list of strings')
            )
    elif not(isinstance(cols, str)):
        raise TypeError(
            ('Error, "colNames" must be either a string or a list of strings')
        )
    datafr = p.arguments["series"].parent.arguments.data
    if type(cols) is str:
        result = datafr[cols]
        p.arguments.data = result
        return
    elif type(cols) is list:
        result = pd.DataFrame(columns=cols)
        for s in cols:
            result[s] = datafr[s]
        p.arguments.data = result
        return

def PUTCOLUMNS(p):
    #TODO: Column dict contains column names and series objects to put into newdf, so what does df do?
    datafr = p.arguments["series"].parent.arguments.data
    colNames = p.arguments["colNames"]
    newdf = p.arguments["newDf"].parent.arguments.data
    if not(isinstance(newdf, pd.DataFrame) or isinstance(datafr, pd.DataFrame)):
        raise Exception('newDf & datafr must be Dataframe objects')
    colDict = {}
    for col in colNames:
        colDict[col] = datafr[col]
    if not(all(k in list(datafr) for k in colNames)):
        raise Exception('Error colDict contains columns not found in the input dataframe')
    currentColumns = list(newdf)
    if not(set(currentColumns).isdisjoint(colDict.keys())):
        raise Exception('Error colDict cannot contain any columns already in newDf')
    try:
        for key in colDict.keys():
            newdf[key] = colDict[key]
        p.arguments.data = newdf
    except:
        raise Exception("Error appending columns to newDF, make sure indices align correctly")

def REPLACETIMECOLUMNDF(p):
    #Replaces the timeDate column of a with the timeDate Column of b, and also places it that column in the leftmost index
    a = p.arguments['a'].parent.arguments.data
    b = p.arguments['b'].parent.arguments.data
    toRet = a.copy()
    colNames = b.columns
    toRet[colNames[0]] = b[colNames[0]]
    toRet = toRet[colNames]
    p.arguments.data = toRet

def COLUMNSUMDF(p):
    a = p.arguments['a'].parent.arguments.data
    toRet = pd.DataFrame(columns=a.columns)
    toRetDict = []
    for col in a.columns:
        if col == a.columns[0]:
            continue
        sum = a[col].values.sum()
        toRetDict.append(sum)
    for i in range(len(toRet.columns)):
        if i == 0:
            continue
        toRet.loc[0, toRet.columns[i]] = toRetDict[i - 1]
    p.arguments.data = toRet

def TIMEWEIGHTMEAN(p):
    s = p.arguments['series'].parent.arguments.data
    timeWindow = p.arguments['timewindow']
    if not(type(timeWindow) is int or type(timeWindow) is str or type(timeWindow) is timedelta or type(timeWindow) is float):
        raise Exception('Error TimeWindow must either be an int or float(for integer or float representations of seconds) or datetime object')
    tempRes = s.copy()
    #Case 1: timeWindow is an int, index on regular index
    # if type(timeWindow) is int:
    #     tempRes['DurationOfPrice_Int'] = 1
    #     columnList = list(tempRes.columns)
    #     #TODO NOTE: PANDAS DOES NOT ALLOW FOR ANYTHING OTHER THAN BOTH CLOSED FOR INT WINDOWS
    #     result = tempRes.rolling(timeWindow).weighted_average(columnList=columnList)
    #     columnList2 = columnList
    #     columnList2.remove(columnList[-1])
    #     tempRes2 = pd.DataFrame(columns=columnList2)
    #     tempRes2[columnList[0]] = s[columnList[0]]
    #     for col in list(result.columns):
    #         tempRes2[col] = result[col]
    #     p.arguments.data = tempRes2
    #     return
    #Case 2: either a string, float or timeDelta object needs to be indexed over dateTime column
    #else:
    if type(timeWindow) is float or type(timeWindow) is int:
        timeWindow = timedelta(seconds=timeWindow)
    tempRes = dateStringtoDateTimeFOREXRODO(tempRes)
    columnList = list(tempRes.columns)
    for col in list(tempRes.columns):
        if col == columnList[0] or col == columnList[-1]:
            continue
        tempRes[col] = tempRes[col] * tempRes[columnList[-1]]
    #tempRes = time_Weight_NAN_Filler(timeWindow, tempRes)
    result = tempRes.rolling(timeWindow, on=list(tempRes.columns)[0], closed='left').weighted_average(columnList=columnList)
    result = result.assign(DurationOfPrice_NS=tempRes["DurationOfPrice_NS"])
    result = time_Weight_NAN_Filler(timeWindow, result)
    columnList2 = columnList
    columnList2.remove(columnList[-1])
    tempRes2 = pd.DataFrame(columns=columnList2)
    tempRes2[columnList[0]] = s[columnList[0]]
    for col in list(result.columns):
        if col == list(result.columns)[-1]:
            continue
        tempRes2[col] = result[col]
    p.arguments.data = tempRes2

def TIMEWEIGHTSTD(p):
    s = p.arguments['series'].parent.arguments.data
    timeWindow = p.arguments['timewindow']
    if not (type(timeWindow) is int or type(timeWindow) is str or type(timeWindow) is timedelta or type(
            timeWindow) is float):
        raise Exception(
            'Error TimeWindow must either be an int or float(for integer or float representations of seconds) or datetime object')
    tempRes = s.copy()
    # Case 1: timeWindow is an int, index on regular index
    # if type(timeWindow) is int:
    #     if timeWindow == 1:
    #         raise Exception('STD is undefined for a window of 1')
    #     tempRes['DurationOfPrice_Int'] = 1
    #     columnList = list(tempRes.columns)
    #     # TODO NOTE: PANDAS DOES NOT ALLOW FOR ANYTHING OTHER THAN BOTH CLOSED FOR INT WINDOWS
    #     result = tempRes.rolling(timeWindow).weighted_STD(columnList=columnList)
    #     columnList2 = columnList
    #     columnList2.remove(columnList[-1])
    #     tempRes2 = pd.DataFrame(columns=columnList2)
    #     tempRes2[columnList[0]] = s[columnList[0]]
    #     for col in list(result.columns):
    #         tempRes2[col] = result[col]
    #     p.arguments.data = tempRes2
    #     return
    #     # Case 2: either a string, float, int or timeDelta object needs to be indexed over dateTime column
    # else:
    if type(timeWindow) is float or type(timeWindow) is int:
        timeWindow = timedelta(seconds=timeWindow)
    tempRes = dateStringtoDateTimeFOREXRODO(tempRes)
    columnList = list(tempRes.columns)
    for col in list(tempRes.columns):
        if col == columnList[0] or col == columnList[-1]:
            continue
        tempRes[col] = tempRes[col] * tempRes[columnList[-1]]
    result = tempRes.rolling(timeWindow, on=list(tempRes.columns)[0], closed='left').weighted_STD(columnList=columnList)
    result = result.assign(DurationOfPrice_NS=tempRes["DurationOfPrice_NS"])
    result = time_Weight_NAN_Filler(timeWindow, result)
    columnList2 = columnList
    columnList2.remove(columnList[-1])
    tempRes2 = pd.DataFrame(columns=columnList2)
    tempRes2[columnList[0]] = s[columnList[0]]
    for col in list(result.columns):
        if col == list(result.columns)[-1]:
            continue
        tempRes2[col] = result[col]
    p.arguments.data = tempRes2

#Time Weighted Average function to apply to rolling window
#TODO ALTER STRING TO DATETIME FUNCTION BASED ON FINALIZED INPUT DATA SOURCE
#TODO RIGHT NOW STRING TO DATETIME IS FOR RODO SUPPLIED FOREX.CSV
def weighted_average(x, columnList):
    d = []
    tempL = columnList.copy()
    for col in columnList:
        if col == columnList[0] or col == columnList[-1]:
            d.append(x[col])
            continue
        d.append(x[col].sum()/x[columnList[-1]].sum())
    tempL.remove(tempL[0])
    tempL.remove(tempL[-1])
    toRet = pd.DataFrame(columns=tempL)
    for i in range(len(d)):
        if i == 0 or i == len(d) - 1:
            continue
        toRet[columnList[i]] = d[i]
    return toRet
_Rolling_and_Expanding.weighted_average = weighted_average

#Time Weighted Standard Deviation function to apply to rolling window
def weighted_STD(x, columnList):
    d = []
    tempL = columnList.copy()
    for col in columnList:
        if col == columnList[0] or col == columnList[-1]:
            d.append(x[col])
            continue
        d.append(x[col].std()/x[columnList[-1]].sum())
    tempL.remove(tempL[0])
    tempL.remove(tempL[-1])
    toRet = pd.DataFrame(columns=tempL)
    for i in range(len(d)):
        if i == 0 or i == len(d) - 1:
            continue
        toRet[columnList[i]] = d[i]
    return toRet
_Rolling_and_Expanding.weighted_STD = weighted_STD

def time_Weight_NAN_Filler(timewindow, s):
    elapsedTime = 0
    #timeWindow must be timeDelta object at this point
    windowNanoSeconds = timewindow.seconds * 1000000000
    for index, row in s.iterrows():
        if elapsedTime >= windowNanoSeconds:
            break
        elapsedTime += row.get_values()[-1]
        for col in list(s):
            s.set_value(index=index, col=col, value=np.nan)
    return s


def dateStringtoDateTimeHISTORICAL(s):
    result = pd.DataFrame(columns=list(s))
    for index, row in s.iterrows():
        date_string = row.get_values()[0] + "000"
        date_object = datetime.strptime(date_string, "%Y%m%d %H%M%S%f")
        result.set_value(index=index, col="DateTime", value=date_object)
    return result

def dateStringtoDateTimeFOREXRODO(s):
    result = s.copy()
    result['DateTime'] = pd.to_datetime(s['DateTime'], format="%Y-%m-%d %H:%M")
    tempDuration = result[list(result.columns)[0]].diff()
    tempDuration[0] = result.iloc[1][list(result.columns)[0]] - result.iloc[0][list(result.columns)[0]]
    tempDuration2 = tempDuration / pd.to_timedelta(1)
    result = result.assign(DurationOfPrice_NS=tempDuration2)
    return result

#df["result"] = df["any_col"].rolling(24).apply(dataframe_roll(df), raw=False)
def INTERVALMEAN(p):
    a = p.arguments["series"].parent.arguments.data
    win = p.arguments["window"]
    if type(win) is datetime:
        rolling = a.rolling(win, closed='right')
    elif type(win) is float:
        secs = timedelta(seconds=win)
        rolling = a.rolling(secs)
    elif type(win) is int:
        secs = timedelta(seconds=win)
        rolling = a.rolling(secs)
    if type(a) is pd.DataFrame:
        p.arguments.data = pd.DataFrame(rolling.mean())
    if type(a) is pd.Series:
        p.arguments.data = pd.Series(rolling.mean())


def INTERVALSTD(p):
    a = p.arguments["series"].parent.arguments.data
    win = p.arguments["window"]
    if type(win) is datetime:
        rolling = a.rolling(win, closed='right')
    elif type(win) is float:
        secs = timedelta(seconds=win)
        rolling = a.rolling(secs)
    elif type(win) is int:
        secs = timedelta(seconds=win)
        rolling = a.rolling(secs)
    if type(a) is pd.DataFrame:
        p.arguments.data = pd.DataFrame(rolling.std())
    if type(a) is pd.Series:
        p.arguments.data = pd.Series(rolling.std())

def INTERVALMIN(p):
    a = p.arguments["series"].parent.arguments.data
    win = p.arguments["window"]
    if type(win) is datetime:
        rolling = a.rolling(win, closed='right')
    elif type(win) is float:
        secs = timedelta(seconds=win)
        rolling = a.rolling(secs)
    elif type(win) is int:
        secs = timedelta(seconds=win)
        rolling = a.rolling(secs)
    if type(a) is pd.DataFrame:
        p.arguments.data = pd.DataFrame(rolling.min())
    if type(a) is pd.Series:
        p.arguments.data = pd.Series(rolling.min())

def INTERVALMAX(p):
    a = p.arguments["series"].parent.arguments.data
    win = p.arguments["window"]
    if type(win) is datetime:
        rolling = a.rolling(win, closed='right')
    elif type(win) is float:
        secs = timedelta(seconds=win)
        rolling = a.rolling(secs)
    elif type(win) is int:
        secs = timedelta(seconds=win)
        rolling = a.rolling(secs)
    if type(a) is pd.DataFrame:
        p.arguments.data = pd.DataFrame(rolling.max())
    if type(a) is pd.Series:
        p.arguments.data = pd.Series(rolling.max())

def INTERVALSUM(p):
    a = p.arguments["series"].parent.arguments.data
    win = p.arguments["window"]
    if type(win) is datetime:
        rolling = a.rolling(win, closed='right')
    elif type(win) is float:
        secs = timedelta(seconds=win)
        rolling = a.rolling(secs)
    elif type(win) is int:
        secs = timedelta(seconds=win)
        rolling = a.rolling(secs)
    if type(a) is pd.DataFrame:
        p.arguments.data = pd.DataFrame(rolling.sum())
    if type(a) is pd.Series:
        p.arguments.data = pd.Series(rolling.sum())

def INTERVALCOUNT(p):
    a = p.arguments["series"].parent.arguments.data
    win = p.arguments["window"]
    if type(win) is datetime:
        rolling = a.rolling(win, closed='right')
    elif type(win) is float:
        secs = timedelta(seconds=win)
        rolling = a.rolling(secs)
    elif type(win) is int:
        secs = timedelta(seconds=win)
        rolling = a.rolling(secs)
    if type(a) is pd.DataFrame:
        p.arguments.data = pd.DataFrame(rolling.count())
    if type(a) is pd.Series:
        p.arguments.data = pd.Series(rolling.count())

