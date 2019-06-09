"Streaming Data"
1) Automatically updates data with new market data every minute
2) Can call realTimeUpdate() to force an update
3) DB called "rtData"

"Historical Data"
1) 19 year data starting 01/02/01, ending 05/31/19
2) DB called "histData"
3) 6gb worth of data, probably too much
4) DB Entry Format
    i) HistoricalData contains Collections based on [YYYY-MM]
    ii) Each tick object in the collection is formatted as follows
        {_id: ObjectId("auto generated hex id")
         DateTime: "20010102 041733000" ->(YYYYMMDD HHMMSSNNN)
         Day: "02"
         Hour: "04"
         Minute "17"
         BidQuote: "0.947"
         AskQuote: "0.9465"
         Volume: "0"
        }

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

TODO:
1) Correctly import and format histDATA
    A) Currently tick data is imported correctly, however it takes ages to import all data. Took 3-4 hours to import the first 3 years of data
2) Correctly import and format rtData
3) Get tick data with volume data?
4) Create query methods to find and extract data from db
5) Create update/add methods to add data to the db

