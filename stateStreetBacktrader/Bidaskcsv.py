class BidAskCSV(btfeeds.GenericCSVData):
    linesoverride = True # discard usual OHLC structure
    # datetime must be present and last
    lines = ('bid', 'ask','low','high','datetime','volume', 'TWAten',
             'TWAtwenty','predsignal', 'probability', 'macdSlow', 'macdFast') #'close', 'open' , 'ema1', 'ema4', 'ema10'
    # datetime (always 1st) and then the desired order for
    params = (
        # (datetime, 0), # inherited from parent class
        ('bid', 1), # default field pos 1
        ('ask', 2), # default field pos 2
        # ('close', -1), ('open', -1),
        ('TWAten', -1), ('TWAtwenty', -1), ('predsignal', -1), ('probability', -1), #used for ML
        ('macdSlow', -1), ('macdFast', -1), #, ('pricetobuy', 8),('size', 9),
        ('low', -1), ('high',-1),
        ('volume', -1),
        ('timeframe', bt.TimeFrame.MicroSeconds)
    )