"Streaming Data"
1) Automatically updates data with new market data every minute
2) Can call realTimeUpdate() to force an update
3) DB called "rtData"

"Historical Data"
1) 19 year data starting 01/02/01, ending 05/31/19
2) DB using arctic wrapper. Use the following lines to initialize historical data
        store = Arctic('localhost')
        store.initialize_library('HistTickStore')
        histlibrary = store['HistTickStore']
3) 6gb of raw data, compressed to 2gb with arctic store
4) Access dataframe objects representing a months worth of data by using data = histLibrary.read("YYYY-MM")

"Cached Algorithm Data"
1) Caches all algorithms run, including the data it is run on
2) If the user wants to run X algorithm with Y Data and Z options, and this combo is already in the cached algorithm data, then simply return result of the cached run
3) Everytime a new calculation is done, add it to the cached algorithm database

"Tick Data Format"
1) "20180101 170014370 | 1.20037 | 1.20087 | 0"
    A) "20180101 170014370" - Represents 2018/01/01 17:00:14:370(HH:MM:SS:NNN) -> HH = Hours, MM = Minutes, SS = Seconds, NNN = Millisecond
    B) "1.20037" - Represents Bid Quote
    C) "1.20087" - Represents Ask Quote
    D) "0" - Represents Volume
2) Timezone of all data is EST WITHOUT Daylight Savings adjustment
3) No Volume information from HISTDATA
    A) No aggregate volume information is available in forex. The only volume data available is broker specific volume, and is not free.

"Hashing & Storing"
1) hashPrimitiveAndStore(inspect.getsource(realTimeUpdate), input_data, result_data)
    A) realTimeUpdate is any primitive function,
        TODO: Utilize https://github.com/manahl/arctic to store result_data


TODO:
2) Correctly import and format rtData
3) Get tick data with volume data?
4) Create query methods to find and extract data from db
5) Create update/add methods to add data to the db
6) Create method to extrapolate minute data from tick data
7) https://github.com/manahl/arctic implement this in order to store time series, tick data and np data quickly into mongodb


