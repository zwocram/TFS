import pdb
import time
import requests
import pandas as pd

import ticker_lists
from yahoo_fin.stock_info import *

DATA = r'C:/Users/Marco van der Zwan/Documents/trading/Laurens Bensdorp/'


def test_yahoo_fin(ticker):
    """
    print(get_analysts_info())
    print(get_balance_sheet(ticker))
    print(get_cash_flow(ticker))
    print(get_data(ticker))
    print(get_dividends(ticker))
    print(get_earnings(ticker))
    print(get_financials(ticker))
    print(get_holders(ticker))
    print(get_income_statement(ticker))
    print(get_live_price(ticker))
    print(get_quote_table(ticker))
    print(get_splits(ticker))
    """
    get_stats(ticker)
    # print(get_stats_valuation('AME'))


def yahoo_stats(ticker, keep, drop, idx):
    stats = get_stats(ticker)
    if len(stats[stats['Attribute'].str[:18] == 'Shares Outstanding']) > 0:
        print(idx, ticker, 'is een common stock.')
        keep.append(ticker)
    else:
        print(idx, ticker, 'is geen common stock')
        drop.append(ticker)


def get_ticker_list():
    ticker_list = ticker_lists.VALIDATE_TICKERS

#    [
#        'MAV', 'MAXR', 'MBI', 'MBT', 'MC', 'MCA', 'MCB', 'MCC', 'MCD', 'MCI', 'MCK', 'MCN', 'MCO'
#    ]
    return ticker_list


if __name__ == "__main__":

    keep_list = []
    drop_list = []
    tick_list = get_ticker_list()

    start = time.time()
    for i, ticker in enumerate(tick_list):
        if i % 200 == 0:
            keepers = pd.Series(keep_list)
            keepers.to_csv(DATA + 'keepers.csv')
            droppers = pd.Series(drop_list)
            droppers.to_csv(DATA + 'droppers.csv')
        try:
            yahoo_stats(ticker, keep_list, drop_list, i)
        except ValueError:
            print(i, ticker, 'is geen common stock.')
            drop_list.append(ticker)
        except IndexError:
            print(i, ticker, 'bestaat waarschijnlijk niet.')
            drop_list.append(ticker)
        except Exception as exp:
            print(i, ticker, 'levert iets heel raars op:', exp)
            drop_list.append(ticker)
    end = time.time()

    print('tickers to keep:', keep_list)
    print('tickers to drop:', drop_list)
    print('het duurde allemaal', end - start, 'seconden.')
    keepers = pd.Series(keep_list)
    keepers.to_csv(DATA + 'keepers.csv')
    droppers = pd.Series(drop_list)
    droppers.to_csv(DATA + 'droppers.csv')
