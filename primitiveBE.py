import pandas as pd
import numpy as np

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
        window = p.arguments['window']
        p.arguments.data = s.rolling(window=window).mean()

    # EMA(s, span, ema)
def EMA(p):
        s = p.arguments['series'].parent.arguments.data
        s = pd.DataFrame(s)
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
   a = p.arguments["a"].parent.arguments.data
   b = p.arguments["b"].parent.arguments.data
   result = pd.Series(a + b)
   p.arguments.result = result

def CEILING(p):
   a = p.arguments["a"].parent.arguments.data
   b = p.arguments["b"].parent.arguments.data
   result = pd.Series(a + b)
   p.arguments.result = result

def LOG(p):
   a = p.arguments["a"].parent.arguments.data
   result = pd.Series(a)
   result = np.log(result)
   p.arguments.result = result

## COMPARISON OPERATIONS ##

def LT(p):
   a = p.arguments["a"].parent.arguments.data
   b = p.arguments["b"].parent.arguments.data
   result = pd.Series.lt(a, b)
   p.arguments.result = result


def LE(p):
   a = p.arguments["a"].parent.arguments.data
   b = p.arguments["b"].parent.arguments.data
   result = pd.Series.le(a, b)
   p.arguments.result = result

def EQ(p):
   if p.arguments["a"].parent.arguments.data is not 0:
    a = p.arguments["a"].parent.arguments.data
    b = p.arguments["b"].parent.arguments.data
    data = pd.Series.eq(a, b)
    p.arguments.data = data

def GE(p):
   a = p.arguments["a"].parent.arguments.data
   b = p.arguments["b"].parent.arguments.data
   result = pd.Series.ge(a, b)
   p.arguments.result = result

def NE(p):
   a = p.arguments["a"].parent.arguments.data
   b = p.arguments["b"].parent.arguments.data
   result = pd.Series.ne(a, b)
   p.arguments.result = result

def GT(p):
   a = p.arguments["a"].parent.arguments.data
   b = p.arguments["b"].parent.arguments.data
   result = pd.Series.gt(a, b)
   p.arguments.result = result


def PRICEFLOOR(p):
   a = p.arguments["a"].parent.arguments.data
   b = p.arguments["b"].parent.arguments.data
   result = pd.Series.gt(a, b)
   p.arguments.result = result


def PRICECEILING(p):
   a = p.arguments["a"].parent.arguments.data
   b = p.arguments["b"].parent.arguments.data
   result = pd.Series.gt(a, b)
   p.arguments.result = result


def ADDTICKS(p):
   a = p.arguments["a"].parent.arguments.data
   b = p.arguments["b"].parent.arguments.data
   result = pd.Series.gt(a, b)
   p.arguments.result = result


def STDEV(p):
    a = p.arguments["series"].parent.arguments.data
    window = p.arguments["window"]   # tODO: what is purpose of 'axis'/'window' here??
    a = a.std()
    p.arguments.result = a


def MIN(p):
    series = p.arguments["series"].parent.arguments.data
    result = series.min()   #TODO: check if there is 'axis' value and evlaute on that if needed otherwise do this
    p.arguments.result = result


def MAX(p):
    series = p.arguments["series"].parent.arguments.data
    result = series.max()  # TODO: check if there is 'axis' value and evlaute on that if needed otherwise do this
    p.arguments.result = result


def SUM(p):
    series = p.arguments["series"].parent.arguments.data
    result = series.sum()  # TODO: check if there is 'axis' value and evlaute on that if needed otherwise do this
    p.arguments.result = result


def DELAY(p):           # is 'shift()' in pandas.series lib okay for this??
    series = p.arguments["series"].parent.arguments.data
    result = series.shift()  # TODO: check if there is 'axis' value and evlaute with that parameter if needed, otherwise do general op
    p.arguments.result = result



