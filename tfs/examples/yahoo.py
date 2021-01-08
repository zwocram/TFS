from yahoo_fin.stock_info import *
from yahoo_fin import options

import pandas_datareader.data as web
import pandas as pd


import datetime as dt
from datetime import timedelta


yr_delta = timedelta(days=545)
start = dt.datetime.today()-yr_delta

start = (dt.datetime.today() - yr_delta).date()
end = dt.date.today()

ticker = 'EURUSD=X'
try:
    df = web.DataReader(ticker, 'yahoo', start, end)
except KeyError as err:
    print(err)


print(df)
