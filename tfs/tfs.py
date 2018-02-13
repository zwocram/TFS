import configparser
import time
import pdb
import sys
import logging
import decimal
from optparse import OptionParser
import pandas as pd
from datetime import datetime, time
from utils import futures

from utils.strategies import TFS, Unit
from db.database import Database

from ib import ib
from ibapi.contract import Contract

EOD_TIME = time(22, 0)

if __name__ == '__main__':
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

    max_units = int(config['tfs']['max_units'])

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

    db = Database()
    tfs_strat = TFS()

    account_info = app.get_account_summary(9001)
    buying_power = float([a[3] for a in account_info
                          if a[2] == 'BuyingPower'][0])
    account_size = float([a[3] for a in account_info
                          if a[2] == 'NetLiquidation'][0])

    if eod:
        eod_data = tfs_strat.eod_data(config.items('portfolio'), ib=app,
                                      config=config, account_size=account_size)

        # store account numbers in database
        date = eod_data.iloc[0, eod_data.columns.get_loc('date')]
        # db.insert_account_numbers(date, account_size, buying_power)

        # create report
        # TODO make code to create reports

        print("=============================")
        print("Account size: ", account_size)
        print("=============================")
        print("Daily recap:\n", eod_data)
        shorts = eod_data.loc[eod_data['close'] < eod_data['55DayLow']]
        if shorts.shape[0] > 0:
            print("=============================")
            print("Potential short candidates:\n\n",
                  shorts[['date', 'close', 'atr',
                          'position_size', 'stp_short']])
        longs = eod_data.loc[eod_data['close'] > eod_data['55DayHigh']]
        if longs.shape[0] > 0:
            print("=============================")
            print("Potential long candidates:\n\n",
                  longs[['date', 'close', 'atr',
                         'position_size', 'stp_long']])

        print("\n=============================\n")
        for index, row in eod_data.iterrows():
            close_price = row['close']
            lt_day_high = row['55DayHigh']
            lt_day_low = row['55DayLow']
            ticker = row['ticker']
            pos_size_df = db.get_position_size(ticker)
            if pos_size_df.shape[0] == 0:
                if close_price > lt_day_high:
                    print('ready to enter 1st long position '
                          'for {0}'.format(ticker))
                elif close_price < lt_day_low:
                    print('ready to enter 1st short position '
                          'for {0}'.format(ticker))
                else:
                    print('not ready to enter new position '
                          'for {0}'.format(ticker))
            elif pos_size_df.shape[0] > 0:
                pos_size = pos_size_df.pos_size.min()
                risk_exposure = pos_size_df.risk_exposure.min()
                max_unit_id = pos_size_df.unit_id.max()
                price_target = pos_size_df.next_price_target.max()
                if pos_size < 0:
                    pos_type = "short"
                elif pos_size > 0:
                    pos_type = "long"
                if ((close_price > price_target and pos_type == "long") or
                        (close_price < price_target and pos_type == "short")):
                    if max_unit_id < max_units:
                        print('add new unit for {0}'.format(ticker))
                    elif max_unit_id == max_units:
                        if risk_exposure > 0:
                            print("move up stop for {0}".format(ticker))

                print('positing size {0}: {1}'.format(ticker, pos_size))
                # 1) get target price of last unit ==> target_price
                # if close_price > (or <) target_price:

            # n_pos_instrument = db.get_position_size(ticker)

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
