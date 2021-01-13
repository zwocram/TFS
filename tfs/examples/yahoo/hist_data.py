import pandas as pd
import pandas_datareader.data as web
import datetime as dt
from datetime import timedelta


def test_pandas_datareader():
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


if __name__ == "__main__":
    test_pandas_datareader()
