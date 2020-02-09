import datetime as dt
import matplotlib.pyplot as plt
from matplotlib import style
import pandas as pd
import pandas_datareader.data as web
import numpy
import talib


def tinker():
    ticker = "DIA"
    style.use('ggplot')  # have a loot at other styles

    start = dt.datetime(2018, 1, 1)
    end = dt.datetime(2019, 12, 31)

    try:
        with open(ticker+'.csv', 'rb') as csvfile:
            df = pd.read_csv(csvfile, parse_dates=True, index_col=0)
    except FileNotFoundError as err:
        print(err, 'proceed with data source')
        df = web.DataReader(ticker, 'yahoo', start, end)
        # df.to_csv(ticker+'.csv')

    df['200MA'] = df['Close'].rolling(window=200).mean()
    # plt.show()

    ax1 = plt.subplot2grid((6, 1), (0, 0), rowspan=5, colspan=1)  # 6 rows, 1 colimn
    ax2 = plt.subplot2grid((6, 1), (5, 0), rowspan=1, colspan=1, sharex=ax1)  # 6 rows, 1 colimn

    df['ATR15'] = talib.ATR(df['High'], df['Low'], df['Adj Close'], timeperiod=15)
    df['ADX'] = talib.ADX(df['High'], df['Low'], df['Adj Close'], timeperiod=25)
    df['MINUS_DM'] = talib.MINUS_DM(df['High'], df['Low'], timeperiod=10)
    df['PLUS_DM'] = talib.PLUS_DM(df['High'], df['Low'], timeperiod=10)
    df['MAX'] = talib.MAX(df['High'], timeperiod=10)
    df['MIN'] = talib.MAX(df['Low'], timeperiod=10)
    df['ticker'] = ticker

    print(df.tail())

    ax1.plot(df.index, df['Close'])
    ax1.plot(df.index, df['200MA'])
    ax2.bar(df.index, df['Volume'])

    plt.show()


if __name__ == "__main__":
    tinker()
