import logging
import datetime as dt
import time
import sys
from datetime import timedelta
import norgatedata
from trading import tibsystem

from optparse import OptionParser
import matplotlib.pyplot as plt
from matplotlib import style
import pandas as pd
import pandas_datareader.data as web
import talib
from sqlalchemy import create_engine

import pdb

INSTR_DATA = r'C:/Users/Marco van der Zwan/Documents/trading/Laurens Bensdorp/'
MIN_ROWS_TIME_SERIES = 200
PRICE_FLOOR = 1
PRICE_CAP = 250
NR_DAYS_HIST = 365
NORGATE_TIMESERIESFORMAT = 'pandas-dataframe'


logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def _get_engine():
    engine = create_engine('sqlite:///db/tib.db', echo=False)
    return engine


def cleanup_norgate(df):
    """
    Clean up raw data

    Rules:
    """
    return df


def cleanup(df):
    """
    Clean up raw data

    Rules:
    """

    add_tickers = ['SPY']
    cleaned = df.copy()
    # drop ETFs/Unkown tickers
    keep_list = pd.read_csv(
        INSTR_DATA + 'keepers.csv',
        names=['ticker'], skiprows=1)
    cleaned = df[df.index.isin(
        keep_list.ticker.unique().tolist(),
        level=0)]
    clean_temp = cleaned.groupby(level=0).tail(1)
    drop_indices = clean_temp[
        (clean_temp['Close'] < PRICE_FLOOR) |
        (clean_temp['Close'] > PRICE_CAP)].index.unique(level=0)
    cleaned_result = cleaned.copy()
    cleaned_result.drop(
        drop_indices.drop(pd.Index(add_tickers), errors='ignore'),
        inplace=True)
    cleaned_result = cleaned_result.append(
        df[df.index.isin(add_tickers, level=0)])

    return cleaned_result


def get_symbols_from_norgate_watchlist(watchlist_name):
    symbols = norgatedata.watchlist_symbols(watchlist_name)

    return symbols


def drop_columns_from_norgate_dataset(df):
    df.drop(['Turnover', 'Unadjusted Close', 'Dividend'], axis=1, inplace=True)

    return df


def get_prices(ticker, start_date):

    prices_df = norgatedata.price_timeseries(
        ticker, start_date=start_date, format=NORGATE_TIMESERIESFORMAT
    )
    prices_df['TICKER'] = ticker
    prices_df.index.rename('DATE', inplace=True)
    prices_df = drop_columns_from_norgate_dataset(prices_df)

    return prices_df.groupby(['TICKER', 'DATE']).min()


def get_eod_hist_norgate():

    additional_symbols = ['SPY']
    yr_delta = timedelta(days=NR_DAYS_HIST)
    start = dt.datetime.today()-yr_delta

    start = (dt.datetime.today() - yr_delta).date()

    start_pd = pd.Timestamp(start)

    watchlist_name = 'daily ordinary stocks'
    symbols = get_symbols_from_norgate_watchlist(watchlist_name)
    prices_multi = [get_prices(s, start_pd)
                    for s in symbols + additional_symbols]
    prices_combo = pd.concat(prices_multi)

    return prices_combo


def get_eod_hist():
    engine = _get_engine()
    strSQL = """
        SELECT *
        FROM prices_eoddata
        WHERE TICKER IN (
            SELECT TICKER
            FROM prices_eoddata
            GROUP BY TICKER
            HAVING max(date(DATE)) = (
                SELECT max(date(DATE)) FROM prices_eoddata)
        )
        ORDER BY
            TICKER, DATE
    """

    df = pd.read_sql_query(strSQL, engine,
                           index_col=['TICKER', 'DATE']
                           )
    return df


def get_spy_from_hist(df):

    pdb.set_trace()
    spy = df.loc[pd.IndexSlice['SPY', ['ticker', 'date', 'atr20', 'close',
                                       'atr20_perc']]]
    spy.to_csv(INSTR_DATA + 'spy.csv')


def get_spy():
    engine = _get_engine()
    strSQL = """
        select
            ticker, date, atr20, close, atr20_perc
            , MA_100D, MA_200D, ATR40, LOW_CLOSE_50D
            , HIGH_CLOSE_70D
        from mkt_data
        where ticker = 'SPY'
        order by date desc limit 300;
    """

    df = pd.read_sql_query(strSQL, engine,
                           parse_dates={"Date": "%Y-%m-%d %H:%M:%S"},
                           index_col="Date")
    df.to_csv(INSTR_DATA + 'spy.csv')


def plot(df):

    style.use('ggplot')  # have a loot at other styles

    ax1 = plt.subplot2grid((6, 1), (0, 0), rowspan=5, colspan=1)  # 6 rows, 1 colimn
    ax2 = plt.subplot2grid((6, 1), (5, 0), rowspan=1, colspan=1, sharex=ax1)  # 6 rows, 1 colimn

    ax1.plot(df.index, df['Close'])
    ax1.plot(df.index, df['200D_MA'])
    ax2.bar(df.index, df['Volume'])

    plt.show()


def write_csv(df, file_name):

    df_concat = pd.concat([d.tail(1) for d in df])
    df_concat.to_csv(INSTR_DATA + file_name)


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
    # test = engine.execute("select * from mkt_data")


def add_ta(df_1):

    df = df_1.copy()
    # df.sort_index()
    try:
        df['AVG_DOLLAR_VOL_20D'] = \
            (df.Volume * df.Close).rolling(window=20).mean()
        df['AVG_DOLLAR_VOL_50D'] = \
            (df.Volume * df.Close).rolling(window=50).mean()
        df['HIST_VOL_20'] = \
            df.Close.pct_change().rolling(window=100).std() * 16
        df['PrevOpen'] = df['Open'].shift(1)
        df['PrevHigh'] = df['High'].shift(1)
        df['PrevLow'] = df['Low'].shift(1)
        df['PrevClose'] = df['Close'].shift(1)
        df['Close_MIN2'] = df['Close'].shift(2)
        df['Close_MIN3'] = df['Close'].shift(3)
        df['Close_MIN4'] = df['Close'].shift(4)
        df['Close_MIN5'] = df['Close'].shift(5)
        df['AVG_VOL_50'] = df['Volume'].rolling(window=50).mean()
        df['AVG_VOL_20'] = df['Volume'].rolling(window=20).mean()
        df['MA_25D'] = df['Close'].rolling(window=25).mean()
        df['MA_50D'] = df['Close'].rolling(window=50).mean()
        df['MA_100D'] = df['Close'].rolling(window=100).mean()
        df['MA_150D'] = df['Close'].rolling(window=150).mean()
        df['MA_200D'] = df['Close'].rolling(window=200).mean()
        df['ATR40'] = talib.ATR(df['High'], df['Low'], df['Close'],
                                timeperiod=40)
        df['ATR40_PERC'] = df['ATR40'] / df['Close']
        df['ATR20'] = talib.ATR(df['High'], df['Low'], df['Close'],
                                timeperiod=20)
        df['ATR20_PERC'] = df['ATR20'] / df['Close']
        df['ATR10'] = talib.ATR(df['High'], df['Low'], df['Close'],
                                timeperiod=10)
        df['ATR10_PERC'] = df['ATR10'] / df['Close']
        df['ADX'] = talib.ADX(df['PrevHigh'], df['PrevLow'], df['PrevClose'],
                              timeperiod=14)
        df['ADX_7'] = talib.ADX(df['PrevHigh'], df['PrevLow'], df['PrevClose'],
                                timeperiod=7)
        df['LOW_CLOSE_50D'] = talib.MIN(df['Close'], timeperiod=50)
        df['HIGH_CLOSE_70D'] = talib.MAX(df['Close'], timeperiod=70)
        df['change_3D'] = df['Close'].pct_change(periods=3)
        df['change_6D'] = df['Close'].pct_change(periods=6)
        df['change_200D'] = df['Close'].pct_change(periods=200)
        df['RSI3'] = talib.RSI(df['Close'], timeperiod=3)
        df['RSI4'] = talib.RSI(df['Close'], timeperiod=4)
    except Exception as exp:
        print('Error bij')
        print(exp)
        pdb.set_trace()

    return df


def import_data(file_name, table_name):

    colnames = ['TICKER', 'DATE', 'Open', 'High', 'Low', 'Close', 'Volume']
    prices = pd.read_csv(file_name, names=colnames, header=None)
    prices['DATE'] = pd.to_datetime(prices['DATE'], format="%d-%b-%Y").dt.date

    engine = _get_engine()
    try:
        prices.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(len(prices), " records ingelezen.")
    except Exception as exp:
        print(exp)


def add_ta_prep(mkt_data):

    days_10d = 11
    days_1m = 23
    days_3m = 68
    days_6m = 134
    days_12m = 262
    for df in mkt_data:
        add_ta(df)

    return mkt_data


def include_ticker(df, max_datum):

    include = True
#    ticker = df.TICKER.max()
    # check 1: is ticker still in today's dataset?
#    if df.index.max() != max_datum:
#        include = False
    """
    if '.' in ticker:
        include = False
    elif '-' in ticker:
        include = False
    """
    return include


def include_ticker_symbol(ticker, df):

    include = True
    if '.' in ticker:
        include = False
    elif '-' in ticker:
        include = False

    return include


def get_market_data(tickers):

    yr_delta = timedelta(days=545)
    start = dt.datetime.today()-yr_delta

    start = (dt.datetime.today() - yr_delta).date()
    end = dt.date.today()

    try:
        df = [web.DataReader(t, 'yahoo', start, end) for t in tickers]
    except KeyError as err:
        print(err)

    for d in enumerate(df):
        d[1]['ticker'] = tickers[d[0]]

    return df


def run_systems():
    pass


if __name__ == "__main__":

    log.info('Starting TIB system.')

    # parse arguments
    parser = OptionParser()
    parser.add_option("-i", "--import_data",
                      default=False, dest="impdata",
                      type='str', help="Import data")
    parser.add_option("-s", "--data_source",
                      default='eoddata', dest="datasource",
                      type='str', help="Which data source to use?"
                      " Default is eoddata. Other options:\n"
                      " - norgate")
    parser.add_option("-a", "--account_value",
                      dest="account_value",
                      type='float', help="The value of broker account.")
    parser.add_option("-t", "--test", action='store_true',
                      default=False, dest="test_mode",
                      help="Puts the program in test mode.")
    parser.add_option("-p", "--placeorders", action='store_true',
                      default=False, dest="place_orders",
                      help="Place orders at broker.")
    (options, args) = parser.parse_args()
    import_csv = options.impdata
    data_source = options.datasource
    test_mode = options.test_mode
    place_orders = options.place_orders
    account_value = 0 if not options.account_value else options.account_value

    if test_mode:
        eod_hist = pd.read_csv(
            INSTR_DATA + 'eod_hist.csv', index_col=['DATE'])
        tibsystem.Centurion.start_trading(
            eod_hist, place_orders=place_orders,
            account_value=account_value)
        sys.exit()

    if import_csv:
        import_data(import_csv, table_name='prices_eoddata')
        sys.exit()

    start = time.time()
    if data_source == 'eoddata':
        eod_1 = get_eod_hist()
        eod = cleanup(eod_1)
    elif data_source == 'norgate':
        eod_1 = get_eod_hist_norgate()
        eod = cleanup_norgate(eod_1)
    end = time.time()
    print(end - start)

    start = time.time()
    ta_df = []
    for ticker in eod.index.unique(level=0):
        eod_sub = eod.loc[pd.IndexSlice[ticker, :]]
        if len(eod_sub) > MIN_ROWS_TIME_SERIES:
            if include_ticker_symbol(ticker, eod_sub):
                eod_sub['TICKER'] = ticker
                eod_sub_ta = add_ta(eod_sub)
                if ticker == 'SPY':
                    spy = eod_sub_ta.loc[:,
                                         ['TICKER', 'ATR20', 'Close',
                                          'ATR20_PERC', 'MA_100D',
                                          'MA_200D', 'ATR40',
                                          'LOW_CLOSE_50D',
                                          'HIGH_CLOSE_70D']]
                    spy.to_csv(INSTR_DATA + 'spy.csv')
                ta_df.append(eod_sub_ta.tail(1))
    end = time.time()
    print(len(ta_df), 'instrumenten geselecteerd. Worden nu weggeschreven...')
    write_csv(ta_df, 'eod_hist.csv')
    # get_spy_from_hist(eod)
    print(end - start)

    """
    df_mkt = get_market_data(tickers)
    df_mkt = add_ta(df_mkt)
    to_db(df_mkt)
    write_csv(df_mkt, 'instr_data.csv')
    get_spy()
    """
    # plot(df_mkt)
