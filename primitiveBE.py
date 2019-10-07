import pandas as pd
import numpy as np
import FND
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
        a = p.arguments["a"].parent.arguments.data
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
        p.arguments.data = pd.DataFrame.le(a, b)
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
    coldict = dict(p.arguments["columnDict"])
    newdf = p.arguments["newDf"].parent.arguments.data
    currentColumns = list(newdf)
    if not(set(currentColumns).isdisjoint(coldict.keys())):
        raise Exception('Error colDict cannot contain any columns already in newDf')
    if not(isinstance(newdf,pd.DataFrame)):
        raise Exception('newDf must be a Dataframe object')
    try:
        for key in coldict.keys():
            newdf[key] = coldict[key]
        p.arguments.data = newdf
    except:
        raise Exception("Error appending columns to newDF, make sure indices align correctly")


#Time-Weighted Interval Operations
# def TIMEWEIGHTMEAN(p):
#     s = p.arguments['series'].parent.arguments.data
#     timeWindow = p.arguments['timewindow']
#     if type(timeWindow) is float:
#         hello = 1
#         return
#     elif type(timeWindow) is int: #Rolling over Indices
#         if timeWindow > 0:
#             p.arguments.data = s.rolling(window=timeWindow, closed="left").mean()
#             return
#     elif type(timeWindow) is str:
#         #figure out exact timewindow in ns
#         rollingWindowSize = pd.to_timedelta(timeWindow) / pd.to_timedelta(1)
#         result = s.copy()
#         #tempRes = result.drop(['DateTime', 'DurationOfPrice', 'DurationOfPrice_NS'], axis=1)
#         tempRes = result.drop(['DateTime', 'DurationOfPrice_NS'], axis=1)
#         for column in list(tempRes.columns):
#             tempRes[column] = tempRes[column] * result['DurationOfPrice_NS']
#         Iterator = result.itertuples()
#         for row in Iterator:
#             #DISREGARD FIRST VALUE
#             #next(Iterator)
#             #COUNT ROWS UNTIL ROLLINGWINDOWSIZE HAS BEEN MET
#             IteratorInner = result.itertuples()
#             for x in range(0,row[0]):
#                 next(IteratorInner)
#             count = 0
#             timesummation = rollingWindowSize
#             currIndex = 0
#             while timesummation > float(0):
#                 if currIndex == result.last_valid_index():
#                     break
#                 for rowInner in IteratorInner:
#                     if np.isnan(rowInner[-1]):
#                         continue
#                     timesummation = timesummation - rowInner[-1]
#                     count = count + 1
#                     currIndex = currIndex + 1
#                     if timesummation <= float(0):
#                         break
#             currIndex = row[0]
#             averageHolder = {}
#             for column in list(tempRes.columns):
#                 #TODO POTENTIALLY CHANGE THE RANGE
#                 for x in range(currIndex+1, currIndex+count):
#                     #averageHolder[column] = averageHolder[column] + result[x:column]
#                     if not(column in averageHolder):
#                         averageHolder[column] = tempRes.iloc[x][column]
#                     else:
#                         averageHolder[column] = averageHolder[column] + tempRes.iloc[x][column]
#             #Divide prices by rollingWindowSize
#             for key in averageHolder.keys():
#                 averageHolder[key] = averageHolder[key]/rollingWindowSize
#                 #Update row in result
#                 result.at[row[0]+1, key] = averageHolder[key]
#                 hello = 1
#             hello = 1
#         hello = 1
#         return result
#     elif type(timeWindow) is datetime.timedelta:
#         hello = 1
#         return

# def TIMEWEIGHTMEAN(p):
#     #TODO PRELIMINARY FUNCTION ONLY TESTING STRING TIMEWINDOWS
#     #NOTE - TIMEWEIGHT OF ONLY 1 COLUMN POSSIBLE WITH THIS ITERATION
#     s = p.arguments['series'].parent.arguments.data
#     targetCol = p.arguments['column']
#     timeWindow = p.arguments['timewindow']
#     tempRes = s.copy()
#     for col in list(tempRes.columns):
#         if col == 'DateTime' or col == 'DurationOfPrice_NS':
#             continue
#         tempRes[col] = tempRes[col] * s['DurationOfPrice_NS']
#     result = tempRes.rolling(timeWindow, on=list(tempRes.columns)[0], closed='left').apply(rolling_TWM_1Column(s=tempRes, column=targetCol))

def TIMEWEIGHTMEAN(p):
    #TODO PRELIMINARY FUNCTION ONLY TESTING STRING TIMEWINDOWS
    #NOTE - TIMEWEIGHT OF ONLY 1 COLUMN POSSIBLE WITH THIS ITERATION
    s = p.arguments['series'].parent.arguments.data
    targetCol = p.arguments['column']
    timeWindow = p.arguments['timewindow']
    tempRes = s.copy()
    for col in list(tempRes.columns):
        if col == 'DateTime' or col == 'DurationOfPrice_NS':
            continue
        tempRes[col] = tempRes[col] * s['DurationOfPrice_NS']
    result = tempRes.rolling(timeWindow, on=list(tempRes.columns)[0], closed='left').apply(rolling_TWM(s=tempRes))



def TIMEWEIGHTSTD(p):
    series = p.arguments["series"].parent.arguments.data
    interval = p.arguments["interval"]


#Time Weighted Average function to apply to rolling window
def rolling_TWM(s):
    columnDict = list(s.columns)
    d = []
    for col in columnDict:
        if col == list(s.columns)[0] or col == list(s.columns)[1]:
            continue
        d.append(s[col].sum()/s['DurationOfPrice_NS'].sum())
    toRet = pd.DataFrame(columns=s.columns)
    count = 0
    for col in columnDict:
        if col == list(s.columns)[0] or col == list(s.columns)[-1]:
            toRet[col] = s[col]
            count = count + 1
        else:
            toRet[col] = d[count]
    return

#Time Weighted Average function to apply to rolling window
def rolling_TWM_1Column(s, column):
    d = []
    d.append(s[column].sum()/s[list(s.columns)[-1]].sum())
    return pd.series(d, index=[column])





def dataframe_roll(df):
    def my_fn(window_series):
        # Note: you can do any kind of offset here
        window_df = df[(df.index >= window_series.index[0]) & (df.index <= window_series.index[-1])]
        return window_df["col1"] + window_df["col2"]
    return my_fn


#df["result"] = df["any_col"].rolling(24).apply(dataframe_roll(df), raw=False)
def INTERVALMEAN(p):
    a = p.arguments["series"].parent.arguments.data
    win = p.arguments["window"]
    if type(win) is 'datetime':
        rolling = a.rolling(win, closed='right')
    else:  #TODO ASSUMPTION HERE THAT IF NOT DATETIME, ITS AN INT
        rolling = a.rolling(win)
    if type(a) is pd.DataFrame:
        p.arguments.data = pd.DataFrame(rolling.mean())
    if type(a) is pd.Series:
        p.arguments.data = pd.Series(rolling.mean())


def INTERVALSTD(p):
    a = p.arguments["series"].parent.arguments.data
    win = p.arguments["window"]
    if type(win) is 'datetime':
        rolling = a.rolling(win, closed='right')
    else:  #TODO ASSUMPTION HERE THAT IF NOT DATETIME, ITS AN INT
        rolling = a.rolling(win)
    if type(a) is pd.DataFrame:
        p.arguments.data = pd.DataFrame(rolling.std())
    if type(a) is pd.Series:
        p.arguments.data = pd.Series(rolling.std())

def INTERVALMIN(p):
    a = p.arguments["series"].parent.arguments.data
    win = p.arguments["window"]
    if type(win) is 'datetime':
        rolling = a.rolling(win, closed='right')
    else:  #TODO ASSUMPTION HERE THAT IF NOT DATETIME, ITS AN INT
        rolling = a.rolling(win)
    if type(a) is pd.DataFrame:
        p.arguments.data = pd.DataFrame(rolling.min())
    if type(a) is pd.Series:
        p.arguments.data = pd.Series(rolling.min())

def INTERVALMAX(p):
    a = p.arguments["series"].parent.arguments.data
    win = p.arguments["window"]
    if type(win) is 'datetime':
        rolling = a.rolling(win, closed='right')
    else:  #TODO ASSUMPTION HERE THAT IF NOT DATETIME, ITS AN INT
        rolling = a.rolling(win)
    if type(a) is pd.DataFrame:
        p.arguments.data = pd.DataFrame(rolling.max())
    if type(a) is pd.Series:
        p.arguments.data = pd.Series(rolling.max())

def INTERVALSUM(p):
    a = p.arguments["series"].parent.arguments.data
    win = p.arguments["window"]
    if type(win) is 'datetime':
        rolling = a.rolling(win, closed='right')
    else:  #TODO ASSUMPTION HERE THAT IF NOT DATETIME, ITS AN INT
        rolling = a.rolling(win)
    if type(a) is pd.DataFrame:
        p.arguments.data = pd.DataFrame(rolling.sum())
    if type(a) is pd.Series:
        p.arguments.data = pd.Series(rolling.sum())

def INTERVALCOUNT(p):
    a = p.arguments["series"].parent.arguments.data
    win = p.arguments["window"]
    if type(win) is 'datetime':
        rolling = a.rolling(win, closed='right')
    else:  #TODO ASSUMPTION HERE THAT IF NOT DATETIME, ITS AN INT
        rolling = a.rolling(win)
    if type(a) is pd.DataFrame:
        p.arguments.data = pd.DataFrame(rolling.count())
    if type(a) is pd.Series:
        p.arguments.data = pd.Series(rolling.count())


        #def DELAY(p):
#    a = p.arguments["a"].parent.arguments.data
#    if type(a) is pd.DataFrame:
#        p.arguments.data = pd.DataFrame(a).shift()
#    if type(a) is pd.Series:
#        p.arguments.data = pd.Series(a).shift()
