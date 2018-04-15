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

from ib import ib
from ibapi.contract import Contract

EOD_TIME = datetime.time(22, 0)

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

    try:
        app = ib.IB("127.0.0.1", 4011, 10)
    except AttributeError as exp:
        print("Could not connect to the TWS API application.")
        sys.exit()

    db = Database()
    tfs_strat = TFS()
    driver = Driver()

    current_time = app.get_time()

    minute_interval = 5
    second_interval = 5
    eod_job_started = None

    print("Waiting till markets are closed.")

    """ONLY EXECUTE BELOW CODE IF TESTING
    temp_time = datetime.datetime.now().time()
    EOD_TIME = datetime.time(temp_time.hour, temp_time.minute + 1)
    print(EOD_TIME)
    """

    while(True):
        time.sleep(0.2)  # prevent the CPU from going wild
        curr_time = datetime.datetime.now().time()
        date_today_iso = datetime.datetime.now().date().isoformat()

        if (curr_time > EOD_TIME and not eod_job_started) or test_mode:
            print("Starting end of day process at {0}."
                  "".format(datetime.datetime.now().time()))
            eod_job_started = True

            # retrieve account data
            account_info = driver.get_account_data(app)
            if account_info is not None:
                buying_power = account_info[0]
                account_size = account_info[1]

            # retrieve current exchange rate data
            hist_data = []
            for instr in config.items('forex'):
                forex_data = driver.get_historical_data(app,
                                                        instr,
                                                        "1 D",
                                                        sleep_time=4)
                hist_data.append(forex_data)

            eod_data = tfs_strat.eod_data(ib=app,
                                          portfolio_list=config.items('portfolio'),
                                          tfs_settings=config['tfs'],
                                          account_size=account_size)
            print(eod_data)

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
                close_price = row['close']
                lt_day_high = row['55DayHigh']
                lt_day_low = row['55DayLow']
                st_day_high = row['20DayHigh']
                st_day_low = row['20DayLow']
                ticker = row['ticker']
                instrument_id = db.get_instrument_id(ticker).instr_id[0]
                atr = row['atr']
                position_info = db.get_position_size(ticker)

                if position_info.shape[0] == 0:
                    create_new_unit = False
                    if close_price > lt_day_high:
                        print('ready to enter 1st long position '
                              'for {0}'.format(ticker))
                        pos_type = "long"
                        create_new_unit = True
                    elif close_price < lt_day_low:
                        print('ready to enter 1st short position '
                              'for {0}'.format(ticker))
                        pos_type = "short"
                        create_new_unit = True
                    else:
                        print('not ready to enter new position '
                              'for {0}'.format(ticker))

                    if create_new_unit:
                        new_position = db.create_position(instrument_id,
                                                          date_today_iso)
                        unit = Unit(account_size=account_size, atr=atr,
                                    account_risk=account_risk,
                                    unit_stop=unit_stop,
                                    first_unit_stop=first_unit_stop,
                                    nr_equities=nr_equities, nr_units=nr_units,
                                    ticker=ticker, price=close_price,
                                    pos_type=pos_type, first_unit=True)
                        new_unit = db.create_unit(unit, 1, new_position,
                                                  pos_type)
                        position_info = db.get_position_size(ticker)
                        updated_pos = db.update_position(
                            position_info=position_info)

                elif position_info.shape[0] > 0:
                    pos_id = position_info.pos_id.min()
                    pos_size = position_info.pos_size.min()
                    risk_exposure = position_info.risk_exposure.min()
                    max_unit_id = position_info.unit_id.max()
                    price_target = position_info.next_price_target.max()
                    stop_price = position_info.stop_price.max()
                    if pos_size < 0:
                        pos_type = "short"
                    elif pos_size > 0:
                        pos_type = "long"

                    # check if we have to add units or move up stops
                    if ((close_price > price_target and pos_type == "long") or
                            (close_price < price_target
                             and pos_type == "short")):
                        if max_unit_id < max_units:
                            print('add new unit for {0}'.format(ticker))
                            unit = Unit(account_size=account_size, atr=atr,
                                        account_risk=account_risk,
                                        unit_stop=unit_stop,
                                        first_unit_stop=first_unit_stop,
                                        nr_equities=nr_equities,
                                        nr_units=nr_units,
                                        ticker=ticker, price=close_price,
                                        pos_type=pos_type, first_unit=True)
                            new_unit = db.create_unit(unit, max_unit_id + 1,
                                                      pos_id, pos_type)
                            position_info = db.get_position_size(ticker)
                            updated_pos = db.update_position(
                                position_info=position_info)
                            # update position info
                        elif max_unit_id == max_units:
                            if risk_exposure > 0:
                                print("move up stop for {0} and "
                                      "set new stop level.".format(ticker))
                                # calculate new stop, move it up only 1 ATR
                                updated_pos = db.update_position(
                                    position_info=position_info,
                                    break_even=True)

                    # check if 20 day high/low crossed the stop

                    print('positing size {0}: {1}'.format(ticker, pos_size))
                # 1) get target price of last unit ==> target_price
                # if close_price > (or <) target_price:

            """ONLY EXECUTE BELOW CODE IF TESTING
            temp_time = datetime.datetime.now().time()
            EOD_TIME = datetime.time(temp_time.hour, temp_time.minute + 1)
            print(EOD_TIME)
            """

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
