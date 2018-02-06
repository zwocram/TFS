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

from utils.strategies import TFS, Unit

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

    tfs_strat = TFS()

    if eod:
        eod_data = tfs_strat.eod_data(config.items('portfolio'), ib=app,
                                      config=config)
        print(eod_data)
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

    print('Finished.')
