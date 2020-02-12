import datetime as dt
from datetime import timedelta

import matplotlib.pyplot as plt
from matplotlib import style
import pandas as pd
import pandas_datareader.data as web
import numpy
import talib
from sqlalchemy import create_engine

import pdb


def _get_engine():
    engine = create_engine('sqlite:///tfs/db/sts.db', echo=False)
    return engine


def get_spy():
    engine = _get_engine()
    strSQL = """
        select
            ticker, date, atr14, close, atr14_perc
            , atr14_1, atr14_perc_1, MA_200D
        from mkt_data
        where ticker = 'SPY'
        order by date desc limit 101;
    """

    df = pd.read_sql_query(strSQL, engine,
                           parse_dates={"Date": "%Y-%m-%d %H:%M:%S"},
                           index_col="Date")
    df.to_csv('tfs/db/spy.csv')


def plot(df):

    style.use('ggplot')  # have a loot at other styles

    ax1 = plt.subplot2grid((6, 1), (0, 0), rowspan=5, colspan=1)  # 6 rows, 1 colimn
    ax2 = plt.subplot2grid((6, 1), (5, 0), rowspan=1, colspan=1, sharex=ax1)  # 6 rows, 1 colimn

    ax1.plot(df.index, df['Close'])
    ax1.plot(df.index, df['200D_MA'])
    ax2.bar(df.index, df['Volume'])

    plt.show()


def write_csv(df):

    df_concat = pd.concat([d.tail(1) for d in df])
    df_concat.to_csv('tfs/db/data.csv')


def to_db(df):
    """
    create empty database:
    sqlite3 sts.db "create table new(f int);drop table new;"
    """
    engine = _get_engine()
    for market in enumerate(df):
        if market[0] == 0:
            market[1].to_sql('mkt_data', con=engine, if_exists='replace')
        else:
            market[1].to_sql('mkt_data', con=engine, if_exists='append')
    #test = engine.execute("select * from mkt_data")


def add_ta(mkt_data):
    for df in mkt_data:
        df['PrevHigh'] = df['High'].shift(1)
        df['PrevLow'] = df['Low'].shift(1)
        df['MA_10D'] = df['Adj Close'].rolling(window=10).mean()
        df['MA_20D'] = df['Adj Close'].rolling(window=20).mean()
        df['MA_50D'] = df['Adj Close'].rolling(window=50).mean()
        df['MA_200D'] = df['Adj Close'].rolling(window=200).mean()
        df['ATR14'] = talib.ATR(df['High'], df['Low'], df['Adj Close'], timeperiod=14)
        df['ATR14_1'] = df['ATR14'].shift(1)
        df['ATR14_PERC'] = df['ATR14'] / df['Adj Close']
        df['ATR14_PERC_1'] = df['ATR14_PERC'].shift(1)
        df['ADX'] = talib.ADX(df['High'], df['Low'], df['Adj Close'], timeperiod=25)
        df['MINUS_DM'] = talib.MINUS_DM(df['High'], df['Low'], timeperiod=10)
        df['PLUS_DM'] = talib.PLUS_DM(df['High'], df['Low'], timeperiod=10)
        df['HIGH_10D'] = talib.MAX(df['High'], timeperiod=10)
        df['HIGH_1_10D'] = talib.MAX(df['PrevHigh'], timeperiod=10)
        df['LOW_10D'] = talib.MIN(df['Low'], timeperiod=10)
        df['LOW_1_10D'] = talib.MIN(df['PrevLow'], timeperiod=10)
        df['change_1D'] = df['Adj Close'].pct_change(periods=1)
        df['change_10D'] = df['Adj Close'].pct_change(periods=10)
        df['change_1M'] = df['Adj Close'].pct_change(periods=20)
        df['change_3M'] = df['Adj Close'].pct_change(periods=60)
        df['change_6M'] = df['Adj Close'].pct_change(periods=120)
        df['change_12M'] = df['Adj Close'].pct_change(periods=260)
        df['RSI2'] = talib.MAX(df['Adj Close'], timeperiod=2)

    return mkt_data


def get_market_data(tickers):

    yr_delta = timedelta(days=545)
    start = dt.datetime.today()-yr_delta

    start = (dt.datetime.today() - yr_delta).date()
    end = dt.date.today()

    df = [web.DataReader(t, 'yahoo', start, end) for t in tickers]
    for d in enumerate(df):
        d[1]['ticker'] = tickers[d[0]]

    return df


if __name__ == "__main__":

    tickers = ['SPY', 'QQQ', 'DIA', 'MDY', 'IWM', 'EFA', 'EPP', 'ILF', 'EEM', 'IEV']
    df_mkt = get_market_data(tickers)
    df_mkt = add_ta(df_mkt)
    to_db(df_mkt)
    write_csv(df_mkt)
    get_spy()
    # plot(df_mkt)
