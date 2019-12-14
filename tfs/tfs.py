from ib_insync import *
import quandl
from brokers.broker import (IBBroker,
                            QuandlBroker)

import configparser
import pdb
import sys
import logging
import decimal
from optparse import OptionParser
import pandas as pd
# from datetime import datetime, time
import datetime
import time
from utils import futures

from utils.strategies import TFS, Unit
from utils.driver import Driver
from db.database import Database

from config import tfslog

EOD_TIME = datetime.time(22, 0)


def get_hist_time_series(portfolio_group_name,
                         config_file_section):

    col_names = [s[1].split(',')[0].strip()
                 for s in config_file_section.items()]
    if portfolio_group_name == "portfolio_IB":
        broker = IBBroker()
    elif portfolio_group_name == "portfolio_QUANDL":
        broker = QuandlBroker()

    time_series = broker.get_historical_data(config_file_section)
    time_series_dict = dict(zip(col_names, time_series))

    return time_series_dict


if __name__ == '__main__':
    # Check that the port is the same as on the Gateway
    # ipaddress is 127.0.0.1 if one same machine, clientid is arbitrary

    # set up logging
    # setup log
    tfslog.setup_logging()
    logger = logging.getLogger()
    logger.info("create logger object")

    # read config file
    config = configparser.ConfigParser()
    config.read('config/settings.cfg')

    max_units = int(config['tfs']['max_units'])

    futures_list = {}
    for f in config.items('futures_list'):
        futures_list[f[0].upper()] = f[1]

    futures_utils = futures.FuturesUtils()
    expiration_dates = futures_utils.resolve_expiration_month_codes()
    expiration_month_codes = expiration_dates[0]
    expiration_months = expiration_dates[1]

    atr_horizon = int(config['tfs']['atr_horizon'])
    entry_breakout_periods = int(config['tfs']['entry_breakout_periods'])
    exit_breakout_periods = int(config['tfs']['exit_breakout_periods'])
    account_risk = decimal.Decimal(config['tfs']['account_risk'])
    unit_stop = int(config['tfs']['unit_stop'])
    first_unit_stop = int(config['tfs']['first_unit_stop'])
    nr_equities = int(config['tfs']['nr_equities'])
    nr_units = int(config['tfs']['nr_units'])

    # parse arguments
    parser = OptionParser()
    parser.add_option("-e", "--eod", action="store_true", default=False,
                      dest="eod", help="Perform end of day actions.")
    parser.add_option("-t", "--test", action="store_true", default=False,
                      dest="test", help="Run program in test mode.")
    (options, args) = parser.parse_args()
    eod = options.eod
    test_mode = options.test

    db = Database()
    tfs_strat = TFS()
    driver = Driver()

    minute_interval = 5
    second_interval = 5
    eod_job_started = None

    print("Waiting till markets are closed.")

    portfolio_sections = ['portfolio_IB', 'portfolio_QUANDL']

    while(True):
        time.sleep(0.2)  # prevent the CPU from going wild
        curr_time = datetime.datetime.now().time()
        date_today_iso = datetime.datetime.now().date().isoformat()

        if (curr_time > EOD_TIME and not eod_job_started) or test_mode:

            print("Starting end of day process at {0}."
                  "".format(datetime.datetime.now().time()))
            eod_job_started = True

            # retrieve account data
            """
            account_info = driver.get_account_data(app)
            if account_info is not None:
                buying_power = account_info[0]
                account_size = account_info[1]
            """

            def _format_output(vals):
                """
                use ANSI code like:
                    red_white = '0;37;41'
                    green_black = '0;30;42'
                    color_string_1 += '\x1b[%sm %s \x1b[0m' \
                        % (green_black, '%.4f' % D120_min)
                """
                new_vals = vals
                # pdb.set_trace()
                close = vals.loc[pd.IndexSlice[:, ['close']]][0]
                D120_min = vals.loc[pd.IndexSlice[:, ['D120-']]][0]
                D120_max = vals.loc[pd.IndexSlice[:, ['D120+']]][0]
                D20_max = vals.loc[pd.IndexSlice[:, ['D20+']]][0]
                D20_min = vals.loc[pd.IndexSlice[:, ['D20-']]][0]
                D55_max = vals.loc[pd.IndexSlice[:, ['D55+']]][0]
                D55_min = vals.loc[pd.IndexSlice[:, ['D55-']]][0]

                def spot_breakouts(max_ind_name, min_ind_name,
                                   max_ind_val, min_ind_val, close_price):
                    """
                    based on donchian high and low levels
                    determine if breakout has occurred and if
                    yes, reformat the entry
                    """

                    if close_price > max_ind_val:
                        new_vals.loc[pd.IndexSlice[:, [max_ind_name]]] = \
                            chr(187) + '%.4f' % max_ind_val + chr(171)
                    elif close_price < min_ind_val:
                        new_vals.loc[pd.IndexSlice[:, [min_ind_name]]] = \
                            chr(187) + '%.4f' % min_ind_val + chr(171)

                    return new_vals

                if close > D120_max:
                    new_vals.loc[pd.IndexSlice[:, ['D120+']]] = \
                        chr(187) + '%.4f' % D120_max + chr(171)
                elif close < D120_min:
                    new_vals.loc[pd.IndexSlice[:, ['D120-']]] = \
                        chr(187) + '%.4f' % D120_min + chr(171)

                if close > D20_max:
                    new_vals.loc[pd.IndexSlice[:, ['D20+']]] = \
                        chr(187) + '%.4f' % D20_max + chr(171)
                elif close < D20_min:
                    new_vals.loc[pd.IndexSlice[:, ['D20-']]] = \
                        chr(187) + '%.4f' % D20_min + chr(171)

                if close > D55_max:
                    new_vals.loc[pd.IndexSlice[:, ['D55+']]] = \
                        chr(187) + '%.4f' % D55_max + chr(171)
                elif close < D55_min:
                    new_vals.loc[pd.IndexSlice[:, ['D55-']]] = \
                        chr(187) + '%.4f' % D55_min + chr(171)

                return new_vals

            time_series = [get_hist_time_series(group_name,
                                                [i[1] for i in config.items()
                                                 if i[0] == group_name][0])
                           for group_name in portfolio_sections]
            time_series_merged = {ticker: data
                                  for dict in time_series
                                  for ticker, data in dict.items()}
            cleaned_data = tfs_strat.clean_up_data(time_series_merged)
            cleaned_data = cleaned_data.apply(_format_output, axis=1)

            print(cleaned_data)

            pdb.set_trace()
            # retrieve current exchange rate data
            hist_data = []
            for instr in config.items('forex'):
                forex_data = driver.get_historical_data(
                    app,
                    instr,
                    "1 D",
                    sleep_time=4)
                hist_data.append(forex_data)

            eod_data = tfs_strat.eod_data(
                ib=app,
                portfolio_list=config.items('portfolio'),
                tfs_settings=config['tfs'],
                account_size=account_size)

            # add stop orders to eod data
            new_dataset = driver.add_stop_orders(eod_data, app)
            eod_data = new_dataset[0]
            eod_data = driver.add_columns(eod_data, ['next_price_target'])
            print(eod_data)
            driver.update_stop_orders(new_dataset)

            prepared_orders = driver.prepare_orders(
                eod_data,
                config.items('portfolio'))

            try:
                chart = driver.draw_bulletgraph(eod_data)
            except Exception as e:
                logging.error("error generating bullet graph: ", e)

            # store account numbers in database
            date = eod_data.iloc[0, eod_data.columns.get_loc('date')]
            db.insert_account_numbers(date, account_size, buying_power)

            # create report
            # TODO make code to create reports

            print("=============================")
            print("Account size: ", account_size)
            print("=============================")
            print("Forex market data:\n")
            print(forex_data)
            print("=============================")
            print("Daily recap:\n", eod_data)
            shorts = eod_data.loc[eod_data['close'] < eod_data['55DayLow']]
            if shorts.shape[0] > 0:
                print("=============================")
                print("Potential short candidates:\n\n",
                      shorts[['date', 'close', 'atr',
                              'pos_size (1st)']])
            longs = eod_data.loc[eod_data['close'] > eod_data['55DayHigh']]
            if longs.shape[0] > 0:
                print("=============================")
                print("Potential long candidates:\n\n",
                      longs[['date', 'close', 'atr',
                             'pos_size (1st)']])

            print("\n=============================\n")
            for index, row in eod_data.iterrows():
                # https://stackoverflow.com/questions/25478528/updating-value-in-iterrow-for-pandas
                recommendations, updated_row = \
                    driver.spot_trading_opportunities(
                        row,
                        config['tfs'],
                        account_size)
                eod_data.loc[index, 'stop_price'] = updated_row['stop_price']
                eod_data.loc[index, 'next_price_target'] = \
                    updated_row['next_price_target']

            # n_pos_instrument = db.get_position_size(ticker)
        elif curr_time < EOD_TIME:
            eod_job_started = False

        # store stuff in Database
        # blabla

    if 1 == 2:
        for future in futures_list:
            future_meta_data = futures_list[future].split(',')
            description_future = future_meta_data[0]
            exchange_future = future_meta_data[1].lstrip()
            security_type = future_meta_data[2].lstrip()
            for month in expiration_months:
                ibcontract = Contract()
                # ibcontract.secType = security_type
                # ibcontract.symbol = future.upper()
                # ibcontract.exchange = exchange_future
                # ibcontract.lastTradeDateOrContractMonth = month

                ibcontract.secType = "FUT"
                ibcontract.symbol = "GE"
                ibcontract.exchange = "GLOBEX"
                ibcontract.lastTradeDateOrContractMonth = "201812"

                resolved_ibcontract = app.resolve_ib_contract(ibcontract)
                if resolved_ibcontract is not None:
                    print(resolved_ibcontract)
                    tickerid = app.start_getting_IB_market_data(resolved_ibcontract)
                    time.sleep(30)

                    # What have we got so far?
                    market_data1 = app.get_IB_market_data(tickerid)

                    try:
                        print(market_data1[0])
                        market_data1_as_df = market_data1.as_pdDataFrame()
                        print(market_data1_as_df)

                        time.sleep(30)

                        # stops the stream and returns all the data we've got so far
                        market_data2 = app.stop_getting_IB_market_data(tickerid)

                        # glue the data together
                        market_data2_as_df = market_data2.as_pdDataFrame()
                        all_market_data_as_df = pd.concat([market_data1_as_df, market_data2_as_df])

                        # show some quotes
                        some_quotes = all_market_data_as_df.resample("1S").last(
                        )[["bid_size", "bid_price", "ask_price", "ask_size"]]
                        print(some_quotes.head(10))

                        # show some trades
                        some_trades = all_market_data_as_df.resample(
                            "10L").last()[["last_trade_price", "last_trade_size"]]
                        print(some_trades.head(10))
                    except:
                        print('iets niet lekker gegaan')

                    input('stop even')

                    # historic_data = app.get_IB_historical_data(resolved_ibcontract)
                    # if historic_data is not None:
                    # df = pd.DataFrame(historic_data)
                    # print(df) # voor later

    print('\n\nFinished.')
