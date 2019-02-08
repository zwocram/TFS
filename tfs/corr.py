from ib_insync import *
import quandl
from brokers.broker import (IBBroker,
                            QuandlBroker)

import configparser
import pdb
import logging
from optparse import OptionParser

from utils.correlations import Correlations
from utils.corrutil import CorrelationUtils

MAX_DAYS_HISTORY = '150 D'
NR_LEAST_CORRELATED_ITEMS = 20


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
    (options, args) = parser.parse_args()
    import_corr = options.import_corr
    corr_group = options.corr_group
    fx_conv = options.fx_conv
    max_correlated_items = int(options.corr_dimension)

    corr_util = Correlations()

    # set up logging
    logging.basicConfig(format='%(levelname)s %(asctime)s: %(message)s',
                        filename='logs/log.log',
                        filemode='w', level=logging.INFO)
    logging.warning('loggin created')

    # read config file
    config = configparser.ConfigParser()
    config.read('config/settings.cfg')

    section = [i[1] for i in config.items() if i[0] == corr_group][0]
    if "IB_" in corr_group:
        ib_broker = IBBroker()
        all_data = ib_broker.get_historical_data(section)
    elif "QUANDL_" in corr_group:
        quandl_broker = QuandlBroker()
        all_data = quandl_broker.get_historical_data(section, fx_conv)

    corr_utils = CorrelationUtils()
    nr_uncorr_items = min(max_correlated_items, all_data.columns.size)

    sub_corr = corr_utils.least_correlated_sub_matrix_by_approx(
        all_data.corr().abs(), max_dimension=nr_uncorr_items)
    sub_corr.columns = [''] * sub_corr.columns.size
    print(sub_corr.round(decimals=3))

    sub_corr_test = corr_utils.least_correlated_sub_matrix_by_simu(
        all_data.corr().abs(),
        max_dimension=nr_uncorr_items,
        nr_trials=100000)
    sub_corr_test.columns = [''] * sub_corr_test.columns.size
    print(sub_corr_test.round(decimals=3))

    sub_corr_opt = corr_utils.least_correlated_sub_matrix_by_optimization(
        all_data.corr().abs(), max_dimension=nr_uncorr_items)
    sub_corr_opt.columns = [''] * sub_corr_opt.columns.size
    print(sub_corr_opt.round(decimals=3))


if __name__ == '__main__':
    calc()
