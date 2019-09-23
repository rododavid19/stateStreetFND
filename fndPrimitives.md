# Primitive Operations

A large set of `Primitive` objects are provided. The basic form of a
`Primitive` operation is the *declarative* form, for example

    Add(a, b, sum)
	
which instantiates an instance of the `Add` object. For notational convenience
we also define a *functional form* for each primitive. The functional forms
are simple wrappers that instantiate a `Primitive` and any `TimeSeries` driven
by the `Primitive`, and then return the new series as the value of the
function, for example

    sum = add(a, b)
	
For arithmetic and comparison operations we also define *operator* forms, such
as

	sum = a + b
	
These operator overloadings are defined as methods on the `TimeSeries`
objects.
	
All objects have a unique hierarchical name. By default, the hierarchical
names are automatically generated. However, to aid debugging or the
examination of intermediate results, all objects are required to support an
optional `name` paramater. For example

    Add(a, b, sum, name='Adder')
	
instantiates an `Add` primitive with a user-specified hierarchical name.


---

## Sources and Sinks

The abstract language does not define exactly how data enters and exits the
network. Instead, data enters and exits the network through generic source and
sink objects. The back-end implementation will provide protocols for mapping
source data and results into and out of these objects. Most likely the mappings
will be made based on the hierarchical names of the objects, however as
elsewhere explicit naming of these objects is optional.


---

    Declarative Form: SeriesSource(series, name=None)
     Functional Form: series = seriesSource(name=None)

Bring a `Series` into the network in back-end specific way.

---

## Arithmetic

Arithmetic operators operate on TimeSeries and appropriate constants. For
example, the operator-form of `Add()`

    sum = series1 + series2

computes the element-wise sum of `series`1 and `series2`, whereas

    sum = series + 10

adds 10 to each element of the `series` (or to each series embedded in a
DataFrame).

If operating on two time series, the series must have compatible indices, and
the resulting series will be compatible with the input series.

The series can be `Series` of `Bool`, `Int`, `Float` or `Price`, or
`DataFrame` of numeric `Series`, subject to the following rules:

* Consistent with NumPy, the Boolean values `True` and `False` are coerced to
  0 and 1 respectively in many arithmetic contexts. See the individual
  documentation for exceptions.

* Be aware that negative `Price` are defined, although likely meaningless.
  
* If one of the operands is a `Float`, the result will always be a `Float`.

* Addition and subtraction of two `Price` is allowed as long as the tick sizes
  are identical, otherwise the operation is undefined.

* Addition and subtraction of `Price` with `Int` or `Float` always yields a
  `Float`. 
  
* Multiplication of `Price` by `Price` is undefined.

* Division always produces a `Float`.

* Division of `Price` by `Price` is allowed, including prices that have
  different tick sizes.
  
* The `log`, `mean`, `std` etc. of `Price` yields a `Float`.

---

    Declarative Form: Neg(a, ng, name=None)
     Functional Form: ng = neg(a, name=None)
       Operator Form: ng = -a

Compute n = -a. Not defined for `Bool`.
      	    
---

    Declarative Form: Abs(a, av, name=None)
       Operator Form: n = abs(a)

Compute the absolute value. Note that Python defines the operator form as a
functional form here, so the only way to define a specific name to the object
is to use the declarative form.

Consistent with NumPy, this is an identity operation for `Bool` (i.e., not
coerced to 0/1).
      	    
---

    Declarative Form: Add(a, b, sum, name=None)
     Functional Form: sum = add(a, b, name=None)
       Operator Form: sum = a + b

Compute sum = a + b.
      	    
---

    Declarative Form: Subtract(a, b, difference, name=None)	
     Functional Form: difference = subtract(a, b, name=None)
       Operator Form: difference = a - b

Compute difference = a - b.

---

    Declarative Form: Multiply(a, b, product
     Functional Form: product = multiply(name, a, b, name=None)
       Operator Form: product = a * b

Compute product = a * b.


---

    Declarative Form: Divide(a, b, quotient, name=None)
     Functional Form: quotient = divide(a, b, name=None)
       Operator Form: quotient = a / b


Compute quotient = a / b.

---

    Declarative Form: Remainder(a, b, rem, name=None)
     Functional Form: rem = remainder(a, b, name=None)
       Operator Form: rem = a % b

Compute rem = a % b.

---

    Declarative Form: Floor(a, flr, divisor=1, name=None)
     Functional Form: flr = floor(a, divisor=1, name=None)
       Operator Form: flr = a // divisor

Compute the largest integer less than or equal to a / b.

---

    Declarative Form: Ceiling(a, ciel, b=1, name=None)
     Functional Form: ciel = ceiling(a, b=1, name=None)

Compute the smallest integer greater than or equal to a / b.

---

    Declarative Form: log(a, ln, name=None)
     Functional Form: ln = log(a, name=None)

Compute the natural logarithm.

---

## Comparison and Selection Operations

The standard numerical comparison operations are defined in all three
forms. The result is a `Bool` series. A `Bool` series can be used to select
values from 2 other series.

---

    Declarative Form: LT(x, y, b, name=None)
     Functional Form: b = lt(x, y, name=None)
       Operator Form: x < y

---

    Declarative Form: LE(x, y, b, name=None)
     Functional Form: b = le(x, y, name=None)
       Operator Form: x <= y

---

    Declarative Form: EQ(x, y, b, name=None)
     Functional Form: b = eq(x, y, name=None)
       Operator Form: x == y

---

    Declarative Form: NE(x, y, b, name=None)
     Functional Form: b = ne(x, y, name=None)
       Operator Form: x != y

---

    Declarative Form: GE(x, y, b, name=None)
     Functional Form: b = ge(x, y, name=None)
       Operator Form: x >= y

---

    Declarative Form: GT(x, y, b, name=None)
     Functional Form: b = (x, y, name=None)
       Operator Form: x < y

---

    Declarative Form: select(b, t, f, sel, name=None)
     Functional Form: sel = select(b, t, f, name=None)

Given a Boolean series `b` and compatible series `x` and `y`, return a new
series, element-wise selecting a value from `t` if the value of `b` is `True`,
otherwise from `f`.

---

## Price Arithmetic

---

    Declarative Form: PriceFloor(a, inst, price, name=None)
     Functional Form: price = priceFloor(a, inst, name=None)

Given an Instrument, compute the largest `Price` less than or equal to a.

---

    Declarative Form: PriceCieling(a, inst, price, name=None)
     Functional Form: price = priceCieling(a, inst, name=None)

Given an Instrument, compute the smallest `Price` less than or equal to a.

---

    Declarative Form: AddTicks(a, ticks, sum, name=None)
     Functional Form: sum = addTicks(a, ticks, name=None)

Add a signed integer number of ticks to a `Price` series.
	      		  
---

# Operations on Fixed-Size Rolling Windows

---

These operations are applied on fixed-size rolling windows of `samples`
samples. The output series will be compatible with
the input series.

For all of these operations that include a `minSamples` parameter it efaults
to the window size, and the operations are only defined once `minSamples`
samples have been seen.

---

    Declarative Form: Mean(a, samples, sma, minSamples=None, name=None)
     Functional Form: sma = mean(a, samples, minSamples=None, name=None)

Compute a rolling mean, also known as a simple moving average.

---

    Declarative Form: Std(a, samples, sdev, minSamples=None, name=None)
     Functional Form: sdev = std(a, samples, minSamples=None, name=None)
	    	
Compute a rolling sample standard deviation. Note that the first
element is always undefined, regardless of `minSamples`.

---

    Declarative Form: Min(a, samples, mn, minSamples=None, name=None)
     Functional Form: mn = min(a, samples, minSamples=None, name=None)

Compute the rolling minimum over a fixed-size window.

---

    Declarative Form: Max(a, samples, mx, minSamples=None, name=None)
     Functional Form: mx = max(a, samples, minSamples=None, name=None)

Compute the rolling maximum over a fixed-size window.

---

    Declarative Form: Sum(a, samples, sm, minSamples=None, name=None)
     Functional Form: sm = sum(a, samples, minSamples=None, name=None)

Compute the rolling sum over a fixed-size window.

---

    Declarative Form: Delay(a, samples, dly, name=None)
     Functional Form: dly = delay(a, samples, name=None)

Delay the series values by a fixed number of `samples`, where

    samples >= 0

---

## Exponentially-Weighted Operations

These operations use exponential weighting at each point. All of these
operations include a `minSamples` parameter, which defaults to the window
size. The operations are only defined once `minSamples` samples have been
seen. The output series will be compatible with the input series.

---

    Declarative Form: ExponentialMean(a, span, ema, minSamples=None, name=None)
     Functional Form: ema = exponentialMean(a, span, minSamples=None, name=None)

Compute a rolling exponentially-weighted mean, also known as an exponential
moving average.  The `span` parameter defines the exponential `alpha` as

    alpha = 2 / (span + 1)
	
Each new point is given the weight of `alpha`, and the previous series value
is weighted at `1 - alpha`.

---


## Time Interval Operations

These operations compute over rolling windows of time., equally weighting each
point. The output `TimeSeries` is consistent with the input series.  Intervals
are specified either as integer or floating point *seconds*, or via Python
`datetime.timedelta` objects.

These intervals are closed on the right and open on the left, i.e., they
include the point at the current time, but do not include any points exactly
`interval` time behind the current point, or earlier. The common `minInterval`
argument specifies the minimum interval required at the beginning of the
series, defaulting to the `interval`.
	    	    
---

    Declarative Form: IntervalMean(a, interval, mean, minInterval=None, name=None)
     Functional Form: mean = intervalMean(a, interval, minInterval=None, name=None)
	
Compute the mean of points contained in the interval. 

---

    Declarative Form: IntervalStd(a, interval, std, minInterval=None, name=None)
     Functional Form: std = intervalStd(a, interval, minInterval=None, name=None)
			
Compute the sample standard deviation of points contained in the
interval. Note that the standard deviation is undefined if there is only 1
point in the interval.

---

    Declarative Form: IntervalMin(a, interval, min, minInterval=None, name=None)
     Functional Form: min = intervalMin(a, interval, minInterval=None, name=None)
	   
Compute the minimum value over the interval.

---

    Declarative Form: IntervalMax(a, interval, max, minInterval=None, name=None)
     Functional Form: max = intervalMax(a, interval, minInterval=None, name=None)
	   
Compute the maximum value over the interval.

---

    Declarative Form: IntervalSum(a, interval, sum, minInterval=None, name=None)
     Functional Form: sum = intervalSum(a, interval, minInterval=None, name=None)
	   
Compute the rolling sum of the samples in the interval.

---

    Declarative Form: IntervalCount(a, interval, count, minInterval=None, name=None)
     Functional Form: count = intervalCount(a, interval, minInterval=None, name=None)
	   
Compute the rolling count of the number of samples in the interval.

---

## Time-Weighted Interval Operations

These operations compute over rolling windows of time, time-weighting each
point based on the fraction of time the point is active in the interval. The
output `TimeSeries` is consistent with the input series.  Intervals are
specified either as integer or floating point *seconds*, or via Python
`datetime.timedelta` objects.

These intervals are closed on the left and open on the right, i.e., they *do
not* include the point at the current time, as it has a weight of 0. These
intervals also include the interval-fraction weighted value of the point
closet to being either exactly `interval` time behind the current point, or
earlier. 

---

    Declarative Form: TimeWeightedMean(a, interval, mean, minInterval=None, name=None)
     Functional Form: mean = TimeWeightedMean(a, interval, minInterval=None, name=None)
	
Compute the time-weighted mean of points contained in the interval. 

---

    Declarative Form: TimeWeightedStd(a, interval, std, minInterval=None, name=None)
     Functional Form: std = TimeWeightedStd(a, interval, minInterval=None, name=None)
			
Compute the time-weighted sample standard deviation of points contained in the
interval. Note that the standard deviation is undefined if there is
only 1 point in the interval.

---

## DataFrame Operations

In the abstract model, a `DataFrame` is conceptually a simple dictionary
mapping strings to `Series` objects. All of the `Series` objects in the
`DataFrame` must have consistent indices.

---

    Declarative Form: GetColumns(df, columnNames, columns, name=None)
     Functional Form: columns = getColumns(df, columnNames, name=None)
	   Operator Form: columns = df[columnNames]
   
Return one or more columns from a DataFrame, The `columns` parameter is
interpreted as in Pandas (simplified). If `columns` is a string, that column
is returned as a `Series` object. If `columns` is a list of strings, then a
`DataFrame` of those columns is returned.

---

    Declarative Form: PutColumns(df, columnDict, newDf, name=None)
     Functional Form: newDf = putColumns(df, columnDict, name=None)
	   Operator Form: df[column] = series
   
Add one or more columns to a `DataFrame`. The `columnDict` is a dictionary
mapping strings (the names of the new columns) to `Series` objects. All column
names are required to be unique within the `DataFrame`, and all `Series`
objects must have indices compatible with the `DataFrame`. 


Note that the declarative and operator forms create and return new `DataFrame`
objects, wheras the functional form destructively modifies the `DataFrame`.
