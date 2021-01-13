from ib_insync import *
import quandl
from brokers.broker import (IBBroker,
                            QuandlBroker,
                            Yahoo)

# new comment
import copy
import configparser
import pdb
import logging
import math
import pandas as pd
from optparse import OptionParser

from utils.correlations import Correlations
from utils.corrutil import CorrelationUtils

from decimal import Decimal

MAX_DAYS_HISTORY = '150 D'
NR_LEAST_CORRELATED_ITEMS = 3
PORTFOLIO_SIZE = 25


def calc_mkt_shares(markets_dist=None, markets_available=None,
                    markets_count=None, portf_size=30):

    # calculate number of items for each market that
    # has to be present in the portfolio
    theo_market_shares = [(i, round(portf_size * Decimal(
        [m[1] for m in markets_dist if m[0] == i][0])))
        for i in markets_available]

    # if optimal market share < # market elements ==> take optimal
    # else divide market elements by 2 and floor it.
    """
    markets_zipped = list(zip(markets_count, theo_market_shares))
    result = [(z[0][0], z[1][1])
              if z[0][1] > z[1][1] else (z[0][0], math.floor(z[0][1] / 2))
              for z in markets_zipped]
    """
    return theo_market_shares


def calc():
    # parse arguments
    parser = OptionParser()
    parser.add_option("-i", "--import_correlations", dest="import_corr",
                      help="File that contains correlations")
    parser.add_option("-g", "--correlation_group", dest="corr_group",
                      help="Group in settings file that contains the "
                      " instruments.")
    parser.add_option("-f", "--fx_conv", action="store_true", default=False,
                      dest="fx_conv", help="Indicates whether to convert"
                      " time series to native currency.")
    parser.add_option("-d", "--dimension", dest="corr_dimension", default=4,
                      help="Sets the dimension of the correlation "
                      " submatrix.")
    parser.add_option("-c", "--categorize", action="store_true", default=True,
                      dest="categorize", help="Indicates whether to group"
                      " data based on e.g. asset class "
                      "(energy, financial, etc)")

    (options, args) = parser.parse_args()
    import_corr = options.import_corr
    corr_group = options.corr_group
    fx_conv = options.fx_conv
    bool_cat = options.categorize
    max_correlated_items = int(options.corr_dimension)

    corr_util = Correlations()

    # set up logging
    logging.basicConfig(format='%(levelname)s %(asctime)s: %(message)s',
                        filename='logs/log.log',
                        filemode='w', level=logging.INFO)
    logging.warning('loggin created')

    # read config file
    config = configparser.ConfigParser(delimiters=(':'))
    config.read('config/settings.cfg')

    # get markets distribution
    section = [i[1] for i in config.items() if i[0] == corr_group][0]

    # get set of distinct data suppliers
    data_suppliers = [s[1].split(',')[0].strip().lower()
                      for s in section.items()]
    data_suppliers = list(dict.fromkeys(data_suppliers))  # make unique

    # get data
    dataset = []

    if 'quandl' in data_suppliers:
        broker = QuandlBroker()
        data_quandl = broker.get_historical_data(section, fx_conv)
        dataset.append(data_quandl)

    if 'yahoo' in data_suppliers:
        broker = Yahoo()
        data_yahoo = broker.get_historical_data(section)
        dataset.append(data_yahoo)

    all_data = pd.concat(dataset, axis=1)

    markets_dist = config.items("markets_distribution")

    if bool_cat:
        market_set = all_data.corr().columns.tolist()
        if len(market_set) < len(section):
            print("WARNING: correlation matrix contains fewer items"
                  " than config file.")
            pdb.set_trace()
        market_classes = [(s[1].split(',')[1].strip(),
                           s[1].split(',')[3].strip())
                          for s in section.items()]
        available_markets = [m[1] for m in market_classes
                             if m[0] in market_set]

        unique_markets = list(dict.fromkeys(available_markets))
        markets_count = [(m, available_markets.count(m))
                         for m in unique_markets]
        markets_shares = calc_mkt_shares(markets_dist, unique_markets,
                                         markets_count, PORTFOLIO_SIZE)

        print(markets_count)
        print(markets_shares)
        print(markets_dist)

    corr_utils = CorrelationUtils()
    nr_uncorr_items = min(max_correlated_items, all_data.columns.size)

    sub_corr_opt = corr_utils.least_correlated_sub_matrix_by_optimization_grouped(
        all_data.corr().abs(), max_dimension=markets_shares,
        markets_count=markets_count)
    sub_corr_opt.columns = [''] * sub_corr_opt.columns.size
    print(sub_corr_opt.round(decimals=3))
    pdb.set_trace()

    sub_corr = corr_utils.least_correlated_sub_matrix_by_approx(
        all_data.corr().abs(), max_dimension=nr_uncorr_items)
    sub_corr.columns = [''] * sub_corr.columns.size
    print(sub_corr.round(decimals=3))

    sub_corr_test = corr_utils.least_correlated_sub_matrix_by_simu(
        all_data.corr().abs(),
        max_dimension=nr_uncorr_items,
        nr_trials=100000,
        corr_type="least")
    sub_corr_test.columns = [''] * sub_corr_test.columns.size
    print(sub_corr_test.round(decimals=3))

    sub_corr_opt = corr_utils.least_correlated_sub_matrix_by_optimization(
        all_data.corr().abs(), max_dimension=nr_uncorr_items)
    sub_corr_opt.columns = [''] * sub_corr_opt.columns.size
    print(sub_corr_opt.round(decimals=3))


if __name__ == '__main__':
    calc()
