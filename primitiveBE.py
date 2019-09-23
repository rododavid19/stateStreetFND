import pandas as pd
import numpy as np
import FND

## TODO: FOR ALL add wether Series or DF. Then also check how data was passed??

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
        if type(s) is pd.DataFrame:
            window = p.arguments['window']
            p.arguments.data = s.rolling(window=window).mean()

    # EMA(s, span, ema)
def EMA(p):
        s = p.arguments['series'].parent.arguments.data
        if type(s) is pd.DataFrame:
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
   p.arguments.result = result


def STDEV(p):
    #TODO: add exception for no series or dataframe object?
    a = p.arguments["series"].parent.arguments.data
    window = p.arguments["window"]   # tODO: what is purpose of 'axis'/'window' here??
    a = a.std()
    p.arguments.result = a


def MIN(p):
    # TODO: add exception for no series or dataframe object?
    a = p.arguments["series"].parent.arguments.data
    if type(a) is pd.DataFrame:
        p.arguments.data = pd.DataFrame(a).min()
    if type(a) is pd.Series:
        p.arguments.data = pd.Series(a).min()


def MAX(p):
    # TODO: add exception for no series or dataframe object?
    a = p.arguments["series"].parent.arguments.data
    if type(a) is pd.DataFrame:
        p.arguments.data = pd.DataFrame(a).max()
    if type(a) is pd.Series:
        p.arguments.data = pd.Series(a).max()


def SUM(p):
    # TODO: add exception for no series or dataframe object?
    a = p.arguments["series"].parent.arguments.data
    if type(a) is pd.DataFrame:
        p.arguments.data = pd.DataFrame(a).sum()
    if type(a) is pd.Series:
        p.arguments.data = pd.Series(a).sum()

def DELAY(p):           # is 'shift()' in pandas.series lib okay for this??
    series = p.arguments["series"].parent.arguments.data
    #dly = p.arguments["dly"].parent.argument.data
    samples = p.arguments['samples']
    if isinstance(series, pd.Series):
        result = pd.Series(series).shift(periods=samples)
    elif isinstance(series, pd.DataFrame):
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
    if isinstance(cols, str):
        result = datafr[cols]
        p.arguments.data = result
        return
    elif isinstance(cols, list):
        result = pd.DataFrame(columns=cols)
        for s in cols:
            result[s] = datafr[s]
        p.arguments.data = result
        return



def PUTCOLUMNS(p):
    #TODO: Column dict contains column names and series objects to put into newdf, so what does df do?
    #TODO: Add check to make sure all series in colDict have same or compatible indices to newdf
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


#def DELAY(p):
#    a = p.arguments["a"].parent.arguments.data
#    if type(a) is pd.DataFrame:
#        p.arguments.data = pd.DataFrame(a).shift()
#    if type(a) is pd.Series:
#        p.arguments.data = pd.Series(a).shift()
