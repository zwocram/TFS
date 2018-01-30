import configparser
import time
import pdb
import sys
import logging
import decimal
from optparse import OptionParser
import pandas as pd
from datetime import datetime
from utils import futures

from utils.strategies import Strategy, Unit

from ib import ib
from ibapi.contract import Contract

if __name__ == '__main__':
    ##
    # Check that the port is the same as on the Gateway
    # ipaddress is 127.0.0.1 if one same machine, clientid is arbitrary

        # set up logging
    logging.basicConfig(format='%(levelname)s %(asctime)s: %(message)s',
                        filename='logs/log.log',
                        filemode='w', level=logging.INFO)
    logging.warning('loggin created')

    # read config file
    config = configparser.ConfigParser()
    config.read('config/settings.cfg')

    atr_horizon = int(config['tfs']['atr_horizon'])
    entry_breakout_periods = int(config['tfs']['entry_breakout_periods'])
    exit_breakout_periods = int(config['tfs']['exit_breakout_periods'])
    account_risk = decimal.Decimal(config['tfs']['account_risk'])
    unit_stop = int(config['tfs']['unit_stop'])
    first_unit_stop = int(config['tfs']['first_unit_stop'])
    nr_equities = int(config['tfs']['nr_equities'])
    nr_units = int(config['tfs']['nr_units'])

    futures_list = {}
    for f in config.items('futures_list'):
        futures_list[f[0].upper()] = f[1]

    futures_utils = futures.FuturesUtils()
    expiration_dates = futures_utils.resolve_expiration_month_codes()
    expiration_month_codes = expiration_dates[0]
    expiration_months = expiration_dates[1]

    # parse arguments
    parser = OptionParser()
    parser.add_option("-e", "--eod", action="store_true", default=False,
                      dest="eod", help="Perform end of day actions.")
    (options, args) = parser.parse_args()
    eod = options.eod

    try:
        app = ib.IB("127.0.0.1", 4011, 10)
    except AttributeError as exp:
        print("Could not connect to the TWS API application.")
        sys.exit()

    current_time = app.get_time()

    strat = Strategy()

    if eod:
        for p in config.items('portfolio'):
            ticker = p[0].upper()
            exchange = p[1].split(',')[1].lstrip()
            sec_type = p[1].split(',')[2].lstrip()

            ibcontract = Contract()
            ibcontract.secType = sec_type
            ibcontract.symbol = ticker
            ibcontract.exchange = exchange
            ibcontract.currency = 'USD'

            resolved_ibcontract = app.resolve_ib_contract(ibcontract)

            historic_data = app.get_IB_historical_data(resolved_ibcontract)

            time.sleep(5)
            if historic_data is not None:
                eod_data = {}
                df = pd.DataFrame(historic_data,
                                  columns=['date', 'open', 'high',
                                           'low', 'close', 'volume'])
                df = df.set_index('date')

                eod_data['ticker'] = ticker
                eod_data['atr'] = strat.calculate_atr(atr_horizon, df)
                eod_data['entry_high'] = strat.calc_nday_high(
                    entry_breakout_periods, df)
                eod_data['entry_low'] = strat.calc_nday_low(
                    entry_breakout_periods, df)
                eod_data['exit_high'] = strat.calc_nday_high(
                    exit_breakout_periods, df)
                eod_data['exit_low'] = strat.calc_nday_low(
                    exit_breakout_periods, df)

                capital = 10500
                price = eod_data['entry_high']
                unit = Unit(capital, price, eod_data['atr'],
                            account_risk=account_risk, unit_stop=unit_stop,
                            first_unit_stop=first_unit_stop,
                            nr_equities=nr_equities, nr_units=nr_units)
                position_size = unit.calc_position_size()
                stop_level = unit.calc_stop_level(eod_data['atr'],
                                                  price, first_unit=False)

                print(eod_data, position_size, stop_level)
                # print(df)

            app.init_error()

        try:
            app.disconnect()
        except AttributeError as exp:
            print('Error while disconnecting from TWS: ', exp)
        except:
            print("Unexpected error while disconnecting: ", sys.exc_info()[0])
            sys.exit()

    for future in futures_list:
        future_meta_data = futures_list[future].split(',')
        description_future = future_meta_data[0]
        exchange_future = future_meta_data[1].lstrip()
        security_type = future_meta_data[2].lstrip()
        for month in expiration_months:
            ibcontract = Contract()
            #ibcontract.secType = security_type
            #ibcontract.symbol = future.upper()
            #ibcontract.exchange = exchange_future
            #ibcontract.lastTradeDateOrContractMonth = month

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

                #historic_data = app.get_IB_historical_data(resolved_ibcontract)
                # if historic_data is not None:
                #df = pd.DataFrame(historic_data)
                # print(df) # voor later

    print('Finished.')
