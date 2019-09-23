#!/usr/bin/env python
# coding: utf-8

# In[2]:


import inspect

## %matplotlib inline              TODO: weird translate from jupter to .py
import functools


# A tutorial on Python wrappers is at https://realpython.com/primer-on-python-decorators/.
# Some examples were lifted from that tutorial.

# ## Network
# 
# Create a Network object as a singleton *context manager*, suitable to be used with the Python `with` statement. This is a special wrapper just for the Network singleton.
# 
# We also use the term *context* to mean the object that is currently being defined. This is a full hierarchical design, and the elements of the hierarchy are the Network object, Module and Primitive objects.
# 
# The singleton wrapper messes up the way the class is referenced. Probably should avoid it in the future.



def networkSingleton(cls):
    @functools.wraps(cls)
    def _network():
        if _network.instance is None:
            _network.instance = cls()
        return _network.instance
    _network.instance = None
    return _network


@networkSingleton
class Network():
    
    def __init__(self):
        self.reset()

    def reset(self):
        self.active = False
        self.valid = False
        self.hName = ['']       # The current hierarchical name, as a stack
        self.context = None     # Stack of current context objects (Network, Primitive or Module)
        self.nameCounter = 0    # Counter to generate unique names
        self.children = []
        
    # 'with' context entry 
    def __enter__(self):
        self.requireInactiveNetwork()
        self.reset()
        self.active = True
        self.context = [self]
        return self
  

    # 'with' context exit
    def __exit__(self, exc_type, exc_value, traceback):
        Network().requireActiveNetwork()
        self.active = False
        if exc_type is None:
            self.valid = True
        else:
            self.reset()
        
        
    def newChild(self, child):
        self.children.append(child)
        
        
    def requireActiveNetwork(self):
        if self.active:
            return
        raise Exception('Network is not active!')
        
        
    def requireInactiveNetwork(self):
        if self.active:
            raise Exception('Network is active!')
        return        
        
        
    def requirePrimitiveContext(self):
        self.requireActiveNetwork()
        p = self.context[-1]
        if isinstance(p, Primitive):
            return p
        else:
            print(self.context)
            raise Exception('Operation required to be in the context of a Primitive')

            
    def requireNonPrimitiveContext(self):
        self.requireActiveNetwork()
        p = self.context[-1]
        if isinstance(p, Primitive):
            raise Exception('Operation required not to be in the context of a Primitive')
        return p
     
        
    # Create a new Primitive object and make it the current context
    def pushPrimitive(self, func, *args, **kwargs):
        self.requireActiveNetwork()
        parent = self.requireNonPrimitiveContext()
        binding = inspect.signature(func).bind(*args, **kwargs)
        binding.apply_defaults()
        arguments = binding.arguments
        name = self.primitiveOrModuleName(arguments['name'])
        p = Primitive(func.__name__, name, parent, arguments)
        self.context[-1].children.append(p)
        self.context.append(p)
        self.hName.append(name)    
            
            
    # Create a new Module object and make it the current context
    def pushModule(self, func, *args, **kwargs):
        self.requireActiveNetwork()
        parent = self.requireNonPrimitiveContext()
        binding = inspect.signature(func).bind(*args, **kwargs)
        binding.apply_defaults()
        arguments = binding.arguments
        name = self.primitiveOrModuleName(arguments['name'])
        p = Module(func.__name__, name, parent)
        self.context[-1].children.append(p)
        self.context.append(p)
        self.hName.append(name)    
        
        
    # Pop the current context
    def pop(self):
        self.requireActiveNetwork()
        self.context.pop()
        self.hName.pop()
        
        
    # Series are named <primitive>.<series>
    def seriesName(self, name):
        if name is None:
            newName = self.hName[-1] + '.__' + str(self.nameCounter) + '__'
            self.nameCounter += 1
        else:
            newName = self.hName[-1] + '.' + name
        return newName
    
    
    # Hierarchical names of boxes are <parent>/<child>. There is no '/' at the top level, though
    def primitiveOrModuleName(self, name):
        if self.hName[-1] == '':
            sep = ''
        else:
            sep = '/'
        if name is None:
            newName = self.hName[-1] + sep + '__' + str(self.nameCounter) + '__'
            self.nameCounter += 1
        else:
            newName = self.hName[-1] + sep + name
        return newName
    
    
    # Do a depth-first traversal and report
    def report(self):
        
        def prefix(level):
            return '    ' * level
        
        def _report(obj, level):
            if obj == Network():
                print('%sNetwork Report' % prefix(level))
                for child in obj.children:
                    _report(child, level + 1)
            elif isinstance(obj, Module):
                print('%sModule: Type=%s, Name=%s' % (prefix(level), obj.type, obj.name))
                for child in obj.children:
                    _report(child, level + 1)
            elif isinstance(obj, Primitive):
                print('%sPrimitive: Type=%s, Name=%s' % (prefix(level), obj.type, obj.name))
                for made in obj.made:
                    _report(made, level + 1)
            elif isinstance(obj, Series):
                print('%sSignal: Name=%s' % (prefix(level), obj.name))
            elif isinstance(obj, DataFrame):
                print('%sDataFrame: Name=%s, Keys=%s' % (prefix(level), obj.name, obj.dfDict.keys()))
            else:
                raise Exception('What the hell is this doing here -> %r' % obj)
                
        _report(self, 0)
                


# ## Series and DataFrame
#
# Series objects can only be defined in the context of a Primitive,
# namely the primitive that makes them.


class Series():

    def __init__(self, name, consistentWith=None):
        p = Network().requirePrimitiveContext()
        self.parent = p
        p.makes(self)
        self.name = Network().seriesName(name)

    ## Binary Operators ##

    def __sub__(self, other):
        return subtract(self, other)

    def __add__(self, other):
        return add(self, other)

    def __mul__(self, other):
        return multiply(self, other)

    def __truediv__(self, other):
        return divide(self, other)

    def __mod__(self, other):
        return remainder(self, other)

 ## Comparison Operators ##
    def __lt__(self, other):
        return lessThan(self, other)

    def __gt__(self, other):
        return greaterThan(self, other)

    def __le__(self, other):
        return lessOrEqual(self, other)

    def __ge__(self, other):
        return greaterOrEqual(self, other)

    def __eq__(self, other):
        return equal(self, other)

    def __ne__(self, other):
        return notEqual(self, other)

    ## Unary Operators ##

    def __neg__(self):
        return neg(self)

## Assignment operators ##
    def __isub__(self, other):
        return subtract(self, other)

    def __iadd__(self, other):
        return add(self, other)

    def __imul__(self, other):
        return multiply(self, other)

    def __idiv__(self, other):
        return divide(self, other)

    def __imod__(self, other):
        return remainder(self, other)





class DataFrame():

    def __init__(self, name, dfDict):
        p = Network().requirePrimitiveContext()
        self.parent = p
        self.name = Network().seriesName(name)
        for k, v in dfDict.items():
            if type(k) != str:
                raise Exception('Key not a string')
            if not isinstance(v, Series):
                raise Exception('Value is not a Series')
        self.dfDict = dfDict

    def __getitem__(self, name):
        return self.dfDict[name]



def primitive(func):
    def _primitive(*args, **kwargs):
        Network().pushPrimitive(func, *args, **kwargs)
        val = func(*args, **kwargs)
        Network().pop()
        return val
    return _primitive


class Primitive():

    def __init__(self, typ, name, parent, arguments):
        self.type = typ
        self.name = name
        self.parent = parent
        self.arguments = arguments
        self.made = []
        self.taken = []

    def makes(self, data):
        self.made.append(data)


def module(func):
    def _module(*args, **kwargs):
        Network().pushModule(func, *args, **kwargs)
        val = func(*args, **kwargs)
        Network().pop()
        return val
    return _module


class Module():

    def __init__(self, typ, name, parent):
        self.type = typ
        self.name = name
        self.parent = parent
        self.children = []


# Define our primitives and modules

@primitive
def seriesSource(name: str=None, consistentWith=None) -> Series:
    return Series(name, consistentWith)

@primitive
def seriesSink(name: str=None, consistentWith=None) -> Series:
    return Series(name, consistentWith)

@primitive
def dataFrame(dfDict: dict, name=None) -> DataFrame:
    return DataFrame(name, dfDict)


 ## ARITHMETIC OPERATIONS ##
@primitive
def add(a: Series, b:Series, name: str=None) -> Series:
    return Series(name)

@primitive
def subtract(a: Series, b:Series, name: str=None) -> Series:
    return Series(name)

@primitive
def multiply(a: Series, b:Series, name: str=None) -> Series:
    return Series(name)

@primitive
def divide(a: Series, b:Series, name: str=None) -> Series:
    return Series(name)

@primitive
def neg(a: Series, name: str=None) -> Series:
    return Series(name)

@primitive
def abs(a: Series, name: str=None) -> Series:
    return Series(name)

@primitive
def remainder(a: Series, b: Series, name: str=None) -> Series:
    return Series(name)

@primitive
def floor(a: Series, divisor=1, name: str=None) -> Series:
    return Series(name)

@primitive
def ceiling(a: Series, divisor=1, name: str=None) -> Series:
    return Series(name)

@primitive
def log(a: Series, name: str=None) -> Series:
    return Series(name)

#TODO: Given a Boolean series `b` and compatible series `x` and `y`, return a new
# series, element-wise selecting a value from `t` if the value of `b` is `True`,
# otherwise from `f`.

@primitive
def lessThan(a: Series, b:Series, name: str=None) -> Series:
    return Series(name)

@primitive
def lessOrEqual(a: Series, b:Series, name: str=None) -> Series:
    return Series(name)

@primitive
def equal(a: Series, b:Series, name: str=None) -> Series:
    return Series(name)

@primitive
def greaterOrEqual(a: Series, b:Series, name: str=None) -> Series:
    return Series(name)

@primitive
def notEqual(a: Series, b:Series, name: str=None) -> Series:
    return Series(name)

@primitive
def greaterThan(a: Series, b:Series, name: str=None) -> Series:
    return Series(name)

## PRICE ARITHMETIC OPERATIONS ##

@primitive
def priceFloor(a, inst:Series, name: str=None) -> Series:
    return Series(name)

@primitive
def priceCeiling(a, inst:Series, name: str=None) -> Series:
    return Series(name)

@primitive
def addTicks(series: Series, ticks, name: str=None) -> Series:
    return Series(name)


## Operations on Fixed-Size Rolling Windows ##

@primitive
def sma(series: Series, window: int, name: str=None) -> Series:
    return Series(name, series)

@primitive
def stdev(series: Series, window: int=None, name: str=None) -> Series:
    return Series(name, series)

@primitive
def min(series: Series, window: int=None, name: str=None) -> Series:
    return Series(name, series)

@primitive
def max(series: Series, window: int=None, name: str=None) -> Series:
    return Series(name, series)

@primitive
def sum(series: Series, window: int=None, name: str=None) -> Series:
    return Series(name, series)

@primitive
def delay(series: Series, samples: int, name: str=None) -> Series:
    return Series(name, series)



## Exponentially-Weighted Operations ##
@primitive
def ema(series: Series, span: int, name: str=None) -> Series:
    return Series(name, series)


## Time Interval Operations ##

# TODO: discrete events like trades

# These indicators include the final price, i.e., the price that triggers the update.
# They do not consider anything that happened outside of the time window.

@primitive
def intervalMean(series: Series, window: int, name: str=None) -> Series:
    return Series(name, series)

@primitive
def intervalStdev(series: Series, window: int, name: str=None) -> Series:
    return Series(name, series)

@primitive
def intervalMin(series: Series, window: int, name: str=None) -> Series:
    return Series(name, series)

@primitive
def intervalMax(series: Series, window: int, name: str=None) -> Series:
    return Series(name, series)

@primitive
def intervalSum(series: Series, window: int, name: str=None) -> Series:
    return Series(name, series)

@primitive
def intervalCount(series: Series, window: int, name: str=None) -> Series:
    return Series(name, series)

## Time-Weighted Interval Operations ##

#TODO: continuous states
#The time-weighted indicators also include the state that was in effect at the earliest point in the window,
# even though in almost all cases the state transition will have occurred outside of the window.
# The time-weighted indicators also do not include the event that triggers the update, since its time-weight is 0.


@primitive
def timeWeightedMean(series: Series, window: int, name: str=None) -> Series:
    return Series(name, series)

@primitive
def timeWeightedStdev(series: Series, window: int, name: str=None) -> Series:
    return Series(name, series)


## DataFrame Operations ##

@primitive
def getColumns(series: Series, colNames, name: str=None) -> Series:
    return Series(name, series)

@primitive
def putColumns(series: Series, columnDict : dict, newDf: Series, name: str=None) -> Series:
    return Series(name, series)


## MODULES ##

@module
def macd(series: Series, shortSpan: int=12, longSpan: int=22, signalSpan: int=9, name: str=None) -> DataFrame:
    short = ema(series, shortSpan, name='short')
    long = ema(series, longSpan, name='long')
    delta = short - long
    signal = ema(delta, signalSpan, name='signal')
    histogram = delta - signal
    return dataFrame({'delta':delta, 'signal':signal, 'histogram': histogram}, name='df')


SOURCE_TYPES = ['seriesSource', 'dataFrame']
SINK_TYPES = [seriesSink, DataFrame]