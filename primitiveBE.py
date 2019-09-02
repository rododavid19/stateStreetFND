import pandas as pd
import numpy as np

## TODO: FOR ALL add wether Series or DF. Then also check how data was passed??
##TODO: FOR ALL Modules that are defined then also make sure that data is appended correctly,
# but O(n^2) time complexity .

#TODO: to add support for non-series constant then just add checker before pd operation and calc. result as necessary
# Add(a, b sum)
def ADD(p):
   a = p.arguments["a"].parent.data  # it references parent because 'p' is the function Object which holds REFERENCE of argument, so by using parent actual data can be fectched
   b = p.arguments["b"].parent.data
   result = pd.Series(a + b)   #TODO: check if Series or Dataframe...etc.
   p.arguments["result"] = result


    # Subtract(a, b, difference)
def SUBTRACT(p):
    a = p.arguments["a"].parent.data
    b = p.arguments["b"].parent.data
    result = pd.Series(a - b)
    p.arguments["result"] = result

    # Multiply(a, b, product)
def MULTIPLY(p):
    a = p.arguments["a"].parent.data
    b = p.arguments["b"].parent.data
    result = pd.Series(a * b)
    p.arguments["result"] = result

    # Divide(a, b, quotient, remainder=None)
def DIVIDE(p):
    a = p.arguments["a"].parent.data
    b = p.arguments["b"].parent.data
    result = pd.Series(a / b)
    p.arguments["result"] = result

    # SMA(s, window, ema)
def SMA(p):
        s = p.arguments["series"].parent.data
        window = p.arguments['window']
        p.arguments['result'] = s.rolling(window=window).mean() # not creating 'result', just replacing argument 'series' reference with calc. value. NOTE: this removes argument reference so this might have to be changed if
                                                                # if reference is needed after network processing??  TODO: this was changed from previous comment and now result is created but this is a waste if 'series' can be replaced.
    # EMA(s, span, ema)
def EMA(p):
        s = p.arguments['series'].parent.data
        s = pd.DataFrame(s)
        span = p.arguments['span']
        p.arguments['result'] = s.ewm(span=span).mean()


def NEG(p):
   a = p.arguments["a"].parent.data
   result = pd.Series(-a)   # TODO: add filters to adjust for data type
   p.arguments["result"] = result

def ABS(p):
   a = p.arguments["a"].parent.data
   result = pd.Series(a).abs()
   p.arguments["result"] = result

def REMAINDER(p):
   a = p.arguments["a"].parent.data
   b = p.arguments["b"].parent.data
   result = pd.Series(a % b)        #fmod() is generally preferred when working with floats, while Python’s x % y is preferred when working with integers.
   p.arguments["result"] = result

def FLOOR(p):
   a = p.arguments["a"].parent.data
   b = p.arguments["b"].parent.data
   result = pd.Series(a + b)
   p.arguments["result"] = result

def CEILING(p):
   a = p.arguments["a"].parent.data
   b = p.arguments["b"].parent.data
   result = pd.Series(a + b)
   p.arguments["result"] = result

def LOG(p):
   a = p.arguments["a"].parent.data
   result = pd.Series(a)
   result = np.log(result)
   p.arguments["result"] = result

## COMPARISON OPERATIONS ##

def LT(p):
   a = p.arguments["a"].parent.data
   b = p.arguments["b"].parent.data
   result = pd.Series.lt(a, b)
   p.arguments["result"] = result


def LE(p):
   a = p.arguments["a"].parent.data
   b = p.arguments["b"].parent.data
   result = pd.Series.le(a, b)
   p.arguments["result"] = result

def EQ(p):
   if p.arguments["a"].parent.data is not 0:
    a = p.arguments["a"].parent.data
    b = p.arguments["b"].parent.data
    result = pd.Series.eq(a, b)
    p.arguments["result"] = result

def GE(p):
   a = p.arguments["a"].parent.data
   b = p.arguments["b"].parent.data
   result = pd.Series.ge(a, b)
   p.arguments["result"] = result

def NE(p):
   a = p.arguments["a"].parent.data
   b = p.arguments["b"].parent.data
   result = pd.Series.ne(a, b)
   p.arguments["result"] = result

def GT(p):
   a = p.arguments["a"].parent.data
   b = p.arguments["b"].parent.data
   result = pd.Series.gt(a, b)
   p.arguments["result"] = result


def PRICEFLOOR(p):
   a = p.arguments["a"].parent.data
   b = p.arguments["b"].parent.data
   result = pd.Series.gt(a, b)
   p.arguments["result"] = result


def PRICECEILING(p):
   a = p.arguments["a"].parent.data
   b = p.arguments["b"].parent.data
   result = pd.Series.gt(a, b)
   p.arguments["result"] = result


def ADDTICKS(p):
   a = p.arguments["a"].parent.data
   b = p.arguments["b"].parent.data
   result = pd.Series.gt(a, b)
   p.arguments["result"] = result


def STDEV(p):
    a = p.arguments["series"].parent.data
    window = p.arguments["window"]   # tODO: what is purpose of 'axis'/'window' here??
    a = a.std()
    p.arguments["result"] = a


def MIN(p):
    series = p.arguments["series"].parent.data
    result = series.min()   #TODO: check if there is 'axis' value and evlaute on that if needed otherwise do this
    p.arguments["result"] = result


def MAX(p):
    series = p.arguments["series"].parent.data
    result = series.max()  # TODO: check if there is 'axis' value and evlaute on that if needed otherwise do this
    p.arguments["result"] = result


def SUM(p):
    series = p.arguments["series"].parent.data
    result = series.sum()  # TODO: check if there is 'axis' value and evlaute on that if needed otherwise do this
    p.arguments["result"] = result


def DELAY(p):           # is 'shift()' in pandas.series lib okay for this??
    series = p.arguments["series"].parent.data
    result = series.shift()  # TODO: check if there is 'axis' value and evlaute with that parameter if needed, otherwise do general op
    p.arguments["result"] = result



