
import pandas as pd
import numpy as np
from FND import *
from primitiveBE import *



    # SeriesSink(series)
    #
    # Sink the data into the object.
def seriesSink(p):
        series = p.arguments['series']
        p.pandasImmediate = series.pandasImmediate

    # DataFrameSink(*series)
    #
    # The data frame is created from the sunk series, using the names of the incoming signals as the column names.
def dataFrameSink(p):
        series = p.arguments['series']
        dfDict = {}
        columns = []
        for s in series:
            name = s.name
            dfDict[name] = s.pandasImmediate
            columns.append(name)
        p.pandasImmediate = pd.DataFrame(dfDict, columns=columns)

    # BitBucket(*whatever)
    #
    # Fuhgeddaboudit
def bitBucket(p):
    pass



#TODO: Previously string field were local functions, however the reference is messed up and does not allow for isIstance() verification
PRIMITIVE_MAP = {

    "add": ADD,
    "subtract": SUBTRACT,
    "multiply": MULTIPLY,
    "divide": DIVIDE,
    "sma": SMA,
    "ema": EMA,
    "neg": NEG,
    "abs": ABS,
    "remainder": REMAINDER,
    "log": LOG,
    "lessThan": LT,
    "lessOrEqual": LE,
    "equal": EQ,
    "notEqual": NE,
    "greaterThan": GT,
    "greaterOrEqual": GE,
    "priceFloor": PRICEFLOOR,
    "priceCeiling": PRICECEILING,
    "addTicks": ADDTICKS,
    "stdev": STDEV,
    "min": MIN,
    "max": MAX,
    "sum": SUM,
    "delay": DELAY,
    "getColumns": GETCOLUMNS,
    "putColumns": PUTCOLUMNS,

  #  SeriesSink: PandasImmediate.seriesSink,
   # DataFrameSink: PandasImmediate.dataFrameSink,

   # BitBucket: PandasImmediate.bitBucket
}



########################################################################
# piEval()
########################################################################

# To evaluate the network, we first use the sourceDict to map the input
# Series/DataFrames to their associated object, by the name of the object.

# Next, we run down the list of objects in level order,
# mapping the objects to their underlying interpreters, and calling the
# interpreters with the bound arguments of the object. Note that
# Primitive interpreters update all of the Data object they drive, so for
# this particular back end we do not have to interpret the Data objects.
# Also note that# any network object will have been levelled during its
# postprocessing() phase.


def mapSourceDict(network, sourceDict):
    # Get a list of all Source objects in the network
#    network.pop()
    names = []


    ## TODO:
    i = 0
    for curr in network.children:
     i += i
     if curr.type in SOURCE_TYPES:   ## Not checking object itself, only type field. Can be stronger, but had issues with naming in SOURCE_TYPES as it would be referred to as local.Primitive as opposed to seriesSource
         names.append(curr.name)
    keys = set(sourceDict.keys())



    # Make sure that all of the sources will be bound
    unbound = set(names).difference(keys)
    if unbound != set():
        print('Names', names)
        print('Keys', keys)
        raise Exception("The following sources remain unbound: %s" % unbound)

    # Make sure that only the sources are bound
    undefined = set(keys).difference(names)
    if undefined != set():
        raise Exception("The network does not contain the following putative sources: %s" % undefined)

    # Bind sources to objects
    # here check for non-Data 'type' like "add" then map arguments to actual data, .i.e  'a' = source, 'b' = source2

    for obj in network.children:
       if obj.type in SOURCE_TYPES: #TODO: this can be stronger by mapping actual Object type vs. a str as it's now
           obj.arguments.data = sourceDict[obj.name] # load sources by hierarchy, i increments accordingly as SOURCE_TYPE is confirmed

       # 5:47 fix


# Return a dict mapping the names of each Sink object to the result of the sink

def mapSinks(network):
    sinks = [obj for obj in network.children if obj.type not in SOURCE_TYPES]  # TODO: change to check if in SINK_TYPES. Reference bug is current issue
    d = {}
    for sink in sinks:

        if type(sink) is Module:
            for prim in sink.children:
                if prim.type not in SOURCE_TYPES:
                    d[prim.name] = prim.arguments.data
        else:
            d[sink.name] = sink.arguments.data  # here make tuple of name and data of result, TODO:
    return d





## is this panda Series?
def checkSeries( s, fail=True):
    b = type(s) == pd.core.series.Series
    if fail and not b:
        raise Exception("Expecting a Pandas Series object. " +
                        "This object - %r - is not one." % s)
    return b

## panda Time series?
def checkTimeSeries(*series, fail=True):
        b = True
        for s in series:
            b &= (checkSeries(s, fail) and
                  (type(s.index) == pd.core.indexes.datetimes.DatetimeIndex) and
                  (np.all(s.index[:-1] <= s.index[1:])))
            if fail and not b:
                raise Exception("Expecting a Pandas time series with a unique, sorted index. " +
                                "This object - %r - does not meet our specification." % s)
        return b

def seriesSourceChecker(p):
    if not hasattr(p, 'data'):
        raise Exception("SeriesSource object '%s' has not been assigned data" % p.name)
    series = p.data
    if not checkTimeSeries(series, fail=False):
        raise Exception("SeriesSource object '%s' is bound to this non-time-series object: %r" % (p.name, series))


def piEval(network, sourceDict):

    # first confirm that Network is Network!!
    if not isinstance( network, type(Network.instance) ):   # Bug here, Network is seen as fuction. FIXED: use .instance
        raise ValueError(
            'Expecting a Network object, but got this: %r' % network)

    mapSourceDict(network, sourceDict)

    for p in network.children:
        if p.type in SOURCE_TYPES:
         # verifying source as Panda Series/ Time Series
        # seriesSourceChecker(p)  #TODO: this needs to be removed/ placed elsewhere as it will only allow Time Series to work as sources
         continue
        try:

            if type(p) is Module:
                for prim in p.children:

                    if prim.type not in SOURCE_TYPES:

                        f = PRIMITIVE_MAP[prim.type]
                        f(prim)
            else:
                f = PRIMITIVE_MAP[p.type]
                f(p)
        except:
            raise Exception(
                ('Bug?: Object %s is an instance of an ' +
                 'unrecognized class %s') % (p.name, type(p)))
    return mapSinks(network)