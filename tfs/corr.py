from ib_insync import *
import quandl
from brokers.broker import (IBBroker,
                            QuandlBroker)

# new comment
import configparser
import pdb
import logging
from optparse import OptionParser

from utils.correlations import Correlations
from utils.corrutil import CorrelationUtils

MAX_DAYS_HISTORY = '150 D'
NR_LEAST_CORRELATED_ITEMS = 3


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

<<<<<<< HEAD
    if import_corr is not None:
        df_corr = pd.read_csv(import_corr, index_col=['Unnamed: 0'],
                              na_values=[" ", "  ", " ", ""])
        df_corr = corr_util.fill_upper_triangle(df_corr)
        df_corr = df_corr.abs()
        # sys.exit()
        # df_corr = df.drop(columns=['Unnamed: 0'])
        nr_columns = df_corr.shape[1]
        # df_corr = df_corr.corr().abs()
    else:
        try:
            app = ib.IB("127.0.0.1", 4011, 10)
        except AttributeError as exp:
            print("Could not connect to the TWS API application.")
            sys.exit()

        # set up logging
        logging.basicConfig(format='%(levelname)s %(asctime)s: %(message)s',
                            filename='logs/log.log',
                            filemode='w', level=logging.INFO)
        logging.warning('login created')

        # read config file
        config = configparser.ConfigParser()
        config.read('config/settings.cfg')

        if corr_group is not None:
            instrument_list = config.items(corr_group)
        else:
            instrument_list = config.items('correlation_set')

        corr_df = pd.DataFrame()
        first_instrument = False
        nr_instruments = len(instrument_list)

        for p in instrument_list:
            print(p)
            identifier = p[0].upper()

            exchange = p[1].split(',')[1].lstrip()
            sec_type = p[1].split(',')[2].lstrip()
            currency = p[1].split(',')[3].lstrip().upper()
            ticker = p[1].split(',')[4].lstrip().upper()

            ibcontract = Contract()
            ibcontract.secType = sec_type
            ibcontract.symbol = ticker
            ibcontract.exchange = exchange
            ibcontract.currency = currency
            print('processing', identifier)

            resolved_ibcontract = app.resolve_ib_contract(ibcontract)
            historic_data = app.get_IB_historical_data(
                resolved_ibcontract,
                duration=MAX_DAYS_HISTORY)

            if historic_data is not None:
                df = pd.DataFrame(historic_data,
                                  columns=['date', 'open', 'high',
                                           'low', identifier, 'volume'])
                df = df.set_index('date')
                df = df.drop(columns=['open', 'high', 'low', 'volume'])
                df = corr_util.calc_returns(df, identifier)
                if first_instrument is False:
                    first_instrument = True
                    corr_df = df
                else:
                    corr_df = corr_df.join(df)
                # print(corr_df)

        df_corr = corr_df.corr().abs()
=======
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
        nr_trials=100000,
        corr_type="least")
    sub_corr_test.columns = [''] * sub_corr_test.columns.size
    print(sub_corr_test.round(decimals=3))

    sub_corr_opt = corr_utils.least_correlated_sub_matrix_by_optimization(
        all_data.corr().abs(), max_dimension=nr_uncorr_items)
    sub_corr_opt.columns = [''] * sub_corr_opt.columns.size
    print(sub_corr_opt.round(decimals=3))
>>>>>>> dev_macmini


if __name__ == '__main__':
    calc()
