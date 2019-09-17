import pandas as pd
import numpy as np


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
    a = p.arguments["a"].parent.arguments.data
    if type(a) is pd.DataFrame or pd.Series:
        p.arguments.data = np.floor(a)

def CEILING(p):
    a = p.arguments["a"].parent.arguments.data
    if type(a) is pd.DataFrame or pd.Series:
        p.arguments.data = np.ceil(a)

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
    a = p.arguments["a"].parent.arguments.data
    if type(a) is pd.DataFrame:
        p.arguments.data = pd.DataFrame(a).min()
    if type(a) is pd.Series:
        p.arguments.data = pd.Series(a).min()


def MAX(p):
    a = p.arguments["a"].parent.arguments.data
    if type(a) is pd.DataFrame:
        p.arguments.data = pd.DataFrame(a).max()
    if type(a) is pd.Series:
        p.arguments.data = pd.Series(a).max()


def SUM(p):
    a = p.arguments["a"].parent.arguments.data
    if type(a) is pd.DataFrame:
        p.arguments.data = pd.DataFrame(a).sum()
    if type(a) is pd.Series:
        p.arguments.data = pd.Series(a).sum()


def DELAY(p):
    a = p.arguments["a"].parent.arguments.data
    if type(a) is pd.DataFrame:
        p.arguments.data = pd.DataFrame(a).shift()
    if type(a) is pd.Series:
        p.arguments.data = pd.Series(a).shift()
