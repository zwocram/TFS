import pdb
import time
import decimal
import datetime
import logging

import pandas as pd
import numpy as np

from utils.unit import Unit
from utils.charts import BulletGraph

from db.database import Database

from ibapi.contract import Contract

from ib.ibexceptions import GetAccountDataException, \
    TransformEODDataException, \
    InsertNewUnitException, \
    UpdateStopOrderException, \
    AddStopOrdersException, \
    GetDataFromMarketDataException


class Driver(object):
    """Class that drives 'The Process'.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def spot_trading_opportunities(self, instrument_eod_data, tfs_settings,
                                   account_size, port_config):
        """Checks the EOD data and checks if there are trading
        opportunities.

        :param instrument_eod_data: dataframe row containing
            the EOD data for an instrument
        :param tfs_settings: tfs section of the settings file
        :param account_size: equity value of IB account
        :param port_config: portfolio data from config file
        """

        db = Database()

        eod_data = self._transform_eod_data(instrument_eod_data)  # dict

        ticker = eod_data['ticker']
        eod_data['instrument_id'] = db.get_instrument_id(ticker).instr_id[0]
        eod_data['position_info'] = db.get_position_size(ticker)

        stop, target = self._get_stop_and_target(eod_data)

        if not pd.isnull(stop):
            instrument_eod_data.loc['stop_price'] = stop
            instrument_eod_data.loc['next_price_target'] = target

        new_positions = self._check_positions(
            eod_data,
            tfs_settings,
            account_size,
            port_config)

        return new_positions, instrument_eod_data

    def draw_bulletgraph(self, instr_data):
        bg = BulletGraph()

        chart = bg.draw_graph(
            instr_data,
            size=(8, 5),
            axis_label="EOD", label_color="black",
            bar_color="#252525", target_color='#f7f7f7',
            title="My title")

        return chart

    def add_columns(self, df, column_names=None):
        """Add columns to a dataframe

        :param df: dataframe to which columns have to be added
        :param column_names: list of strings with column names
        """

        if column_names is not None:
            for c in column_names:
                df[c] = np.nan

        return df

    def get_account_data(self, ib, sleep_time=3):
        """Retrieves some data about the IB account.

        :param ib: The IB application
        :return: buying power and account size

        """

        try:
            account_info = ib.get_account_summary(9001)
            account_number = account_info[0][1]
            buying_power = float([a[3] for a in account_info
                                  if a[2] == 'BuyingPower'][0])
            account_size = float([a[3] for a in account_info
                                  if a[2] == 'NetLiquidation'][0])
            time.sleep(sleep_time)
            return buying_power, account_size, account_number
        except Exception as e:
            raise GetAccountDataException(e)

    def get_historical_data(self, ib, instrument, duration="60 D",
                            sleep_time=5):
        """Get daily forex market data.

        :param ib: the ib application
        :param forex_list: list of forex items to be processed.

        :return: returns dictionary with forex data.
        """

        f_metadata = self._get_instrument_metadata(instrument)

        contract = self._get_ib_contract(
            ib,
            f_metadata["sec_type"],
            f_metadata["ticker"],
            f_metadata["exchange"],
            f_metadata["currency"])

        historic_data = ib.get_IB_historical_data(contract,
                                                  duration=duration)

        # close_set = f_metadata["identifier"], \
        #    float("{0:.4f}".format(historic_data[0][4]))
        time.sleep(sleep_time)
        ib.init_error()

        return historic_data

    def _get_stop_orders(self, open_orders):
        """Get stop orders from open order information obect.

        :param open_orders: set of open orders

        :return: dataframe with stop orders
        """

        data_dict = {}
        data_dict['stop_price'] = []
        data_dict['quantity'] = []
        data_dict['close_action'] = []
        data_dict['order_id'] = []
        df_index = []

        for order_id in open_orders.keys():
            order = open_orders[order_id].order
            if order.orderType == "STP":
                contract = open_orders[order_id].contract
                data_dict['stop_price'].append(order.auxPrice)
                data_dict['quantity'].append(int(order.totalQuantity))
                data_dict['close_action'].append(order.action)
                data_dict['order_id'].append(order_id)
                if contract.secType == "CASH":
                    df_index.append(contract.symbol + contract.currency)
                else:
                    df_index.append(contract.symbol)

            stop_orders = pd.DataFrame(data_dict, index=df_index)

        return stop_orders

    def _get_orders(self, open_orders, order_type=None):
        """Retrieves specific order types from the open orders information
        object.

        :param open_orders: set of open orders to choose from.
        :param order_type: specific order type to look for (None=every type)

        :return: dataframe of orders
        """

        """
        print(tempOrderId, order.orderType, order.action, contract.symbol,
              "(" + contract.secType + ")",
              order.totalQuantity, "@", order.auxPrice, order.tif)
        """
        stop_orders = None
        if order_type == "STP":
            stop_orders = self._get_stop_orders(open_orders)

        return stop_orders

    def add_stop_orders(self, add_to_df, ib):
        """Retrieves current stop orders and adds them
        to a specified datasetself.

        :param add_to_df: dataset to which orders have to  added
        :param to_add: data to add
        :param ib: the ib api

        :return: dataframe and contracts
        """

        try:
            open_orders = ib.get_open_orders()
            stop_orders = self._get_orders(
                open_orders, order_type="STP")
        except Exception as e:
            raise AddStopOrdersException(e)

        new_eod_data = add_to_df.join(stop_orders, how='left')

        return new_eod_data, open_orders

    def update_stop_orders(self, dataset):
        """Evaluates a dataframe to check if stop orders
        have to be updated or not.

        :param dataset: 2D tuple,
            (0) dataframe with eod_data
            (1) dictionaary with contracts, dict key = order id

        :return: ???
        """

        instruments = dataset[0]
        contracts = dataset[1]
        try:
            open_positions = instruments.loc[instruments['stop_price'] > 0]
            for index, row in open_positions.iterrows():
                if row['close_action'] == "BUY":  # we're short
                    if row['20DayHigh'] < row['stop_price']:
                        # update/modify stop order
                        print("update stop order:\n",
                              "\tSTP {0} {1} {2}@{3}".format(
                                  row['close_action'],
                                  row['ticker'],
                                  int(row['quantity']),
                                  row['20DayHigh']))
                elif row['close_action'] == "SELL":
                    if row['20DayLow'] > row['stop_price']:
                        # update/modify stop order
                        print("update stop order:\n",
                              "\tSTP {0} {1} {2}@{3}".format(
                                  row['close_action'],
                                  row['ticker'],
                                  int(row['quantity']),
                                  row['20DayLow']))
        except Exception as e:
            raise UpdateStopOrderException(e)

        # eod_data.loc[eod_data['close'] > eod_data['55DayHigh']]

    def _get_ib_contract(self, ib, security_type, symbol, exchange_name,
                         currency):
        """Gets the contract from interactive brokers.

        :param ib: the ib application
        :param security_type: type of instrument to search
        :param symbol: the symbol or ticker of the instrument
        :param exchange_name: the name of the exchange to search
        :param currency: currency in which instrument is denominated

        :return: the IB contract
        """

        ibcontract = Contract()
        ibcontract.secType = security_type
        ibcontract.symbol = symbol
        ibcontract.exchange = exchange_name
        ibcontract.currency = currency

        return ib.resolve_ib_contract(ibcontract)

    def _get_instrument_metadata(self, instrument):
        """Retrieves the metadata for each instrument in the
        instrument list.

        :param instrument: instrument as read from the config file

        :return: dictionary of instrument properties
        """

        instrument_props = {}
        instrument_props["identifier"] = instrument[0].upper()
        instrument_props["exchange"] = instrument[1].split(',')[1].lstrip()
        instrument_props["sec_type"] = instrument[1].split(',')[2].lstrip()
        instrument_props["currency"] = instrument[1].split(', ')[3].lstrip().upper()
        instrument_props["ticker"] = instrument[1].split(',')[4].lstrip().upper()

        return instrument_props

    def get_tfs_settings(self, tfs_settings=None):
        """Gets the settings for the trend following strategy
        from the settings.cfg file.

        :param tfs_settings: the 'tfs' section of the settings file

        :return: returns a dictionary with the tfs settings.
        """

        tfs_settings_dict = {}
        tfs_settings_dict['atr_horizon'] = int(tfs_settings['atr_horizon'])

        tfs_settings_dict['entry_breakout_periods'] = int(tfs_settings['entry_breakout_periods'])
        tfs_settings_dict['exit_breakout_periods'] = int(tfs_settings['exit_breakout_periods'])
        tfs_settings_dict['account_risk'] = decimal.Decimal(tfs_settings['account_risk'])
        tfs_settings_dict['unit_stop'] = int(tfs_settings['unit_stop'])
        tfs_settings_dict['first_unit_stop'] = int(tfs_settings['first_unit_stop'])
        tfs_settings_dict['nr_equities'] = int(tfs_settings['nr_equities'])
        tfs_settings_dict['nr_units'] = int(tfs_settings['nr_units'])
        tfs_settings_dict['max_units'] = int(tfs_settings['max_units'])

        return tfs_settings_dict

    def _transform_eod_data(self, data_row):
        """Transforms eod data in a dataframe row
        into a dictionary.

        :param data_row: dataframe row containing the data

        :return: dict containing EOD data.
        """

        eod_transform = {}

        try:
            eod_transform['close_price'] = data_row['close']
            eod_transform['lt_day_high'] = data_row['55DayHigh']
            eod_transform['lt_day_low'] = data_row['55DayLow']
            eod_transform['st_day_high'] = data_row['20DayHigh']
            eod_transform['st_day_low'] = data_row['20DayLow']
            eod_transform['ticker'] = data_row['ticker']
            eod_transform['atr'] = data_row['atr']
            eod_transform['pos_size_1'] = data_row['pos_size (1st)']
            eod_transform['pos_size_2'] = data_row['pos_size (other)']
        except Exception as e:
            raise TransformEODDataException(e)

        return eod_transform

    def _insert_new_unit(
            self,
            instrument_data,
            tfs_settings,
            account_size,
            position_type,
            create_new_position=False,
            first_unit_bool=True,
            position_id=0):
        """Creates a unit, adds it to a position and update
        position data.

        """
        db = Database()

        max_unit_id = instrument_data['position_info'].unit_id.max()
        pos_id = instrument_data['position_info'].pos_id.min()

        if first_unit_bool:
            unit_id = 1
        else:
            unit_id = max_unit_id + 1

        try:
            unit = Unit(
                account_size=account_size,
                atr=instrument_data['atr'],
                account_risk=tfs_settings['account_risk'],
                unit_stop=tfs_settings['unit_stop'],
                first_unit_stop=tfs_settings['first_unit_stop'],
                nr_equities=tfs_settings['nr_equities'],
                nr_units=tfs_settings['nr_units'],
                ticker=instrument_data['ticker'],
                price=instrument_data['close_price'],
                pos_type=position_type,
                first_unit=first_unit_bool)
            new_unit = db.create_unit(
                unit,
                unit_id,
                position_id,
                position_type)
            position_info = db.get_position_size(instrument_data['ticker'])
            updated_pos = db.update_position(
                position_info=position_info)
        except Exception as e:
            raise InsertNewUnitException(
                "Could not add new unit to the "
                "database. Error: \n", e)

    def _create_first_unit(
            self,
            instrument_data,
            tfs_settings,
            account_size,
            portfolio_config):
        """Checks if a brand new position has to be opened. If not,
        no action is taken.

        :param instrument_data: row with instrument specific data:
            'close_price'
            'lt_day_high'
            'lt_day_low'
            'st_day_high'
            'st_day_low'
            'ticker'
            'atr'
            'pos_size_1'
            'pos_size_2'
        :param tfs_settings: tfs settings from config file
        :param account_size: IB account size
        :param portfolio_config: portfolio data from config file

        :return:

        """
        db = Database()
        ticker = instrument_data['ticker']

        potential_new_unit = self._potential_new_unit(
            instrument_data['close_price'],
            instrument_data['lt_day_high'],
            instrument_data['lt_day_low'])

        if potential_new_unit is not None:
            create_new_unit = potential_new_unit[0]  # True/False
            pos_type = potential_new_unit[1]  # long/short
            action = "BUY" if pos_type == "long" else "SELL"
            if create_new_unit:
                instrument_id = db.get_instrument_id(ticker).instr_id[0]
                self.logger.info("Ready to create %s position for %s." %
                                 (pos_type.upper(), ticker))
                # position_info = db.get_position_size(ticker)
                prepared_order = self._prepare_order(
                    instrument_data,
                    portfolio_config,
                    action)

                """
                new_position = db.create_position(
                    instrument_id,
                    date_today_iso)
                self._insert_new_unit(
                    instrument_data,
                    tfs_settings,
                    account_size,
                    pos_type,
                    first_unit_bool=True,
                    position_id=new_position)
                position_info = db.get_position_size(ticker)
                updated_pos = db.update_position(
                    position_info=position_info)
                """

    def _add_new_unit(
            self,
            instrument_data,
            tfs_settings,
            account_size,
            portfolio_config):
        """Checks if we have to add a new unit to an existing
        position.

        :param portfolio_config: portfolio data from config file

        """

        db = Database()

        ticker = instrument_data['ticker']
        pos_id = instrument_data['position_info'].pos_id.min()
        pos_size = instrument_data['position_info'].pos_size.min()
        risk_exposure = instrument_data['position_info'].risk_exposure.min()
        max_unit_id = instrument_data['position_info'].unit_id.max()

        if pos_size < 0:
            pos_type = "short"
            price_target = instrument_data['position_info'].next_price_target.min()
            stop_price = instrument_data['position_info'].stop_price.min()
        elif pos_size > 0:
            pos_type = "long"
            price_target = instrument_data['position_info'].next_price_target.max()
            stop_price = instrument_data['position_info'].stop_price.min()

        action = "BUY" if pos_type == "long" else "SELL"

        # check if we have to add units or move up stops
        if ((instrument_data['close_price'] > price_target
             and pos_type == "long")
            or (instrument_data['close_price'] < price_target
                and pos_type == "short")):
            if max_unit_id < tfs_settings['max_units']:
                print('add new unit for {0}'.format(ticker))
                self.logger.info("Add new unit for %s." % ticker)
                prepared_order = self._prepare_order(
                    instrument_data,
                    portfolio_config,
                    action,
                    unit_nr=max_unit_id + 1)
                """
                self._insert_new_unit(
                    instrument_data,
                    tfs_settings,
                    account_size,
                    pos_type,
                    first_unit_bool=False,
                    position_id=pos_id)
                position_info = db.get_position_size(ticker)
                updated_pos = db.update_position(
                    position_info=position_info)
                """
            elif max_unit_id == tfs_settings['max_units']:
                if risk_exposure > 0:
                    print("move up stop for {0} and "
                          "set new stop level.".format(ticker))
                    self.logger.info(
                        "Move up stop for %s and "
                        "set new stop level" % ticker.upper())
                    # calculate new stop, move it up only 1 ATR
                    updated_pos = db.update_position(
                        position_info=position_info,
                        break_even=True)

    def _check_positions(
            self,
            instrument_data,
            tfs_settings,
            account_size,
            portfolio_config):
        """Checks if we can open new positions
        or scale in on existing ones.

        :param instrument_data: dict containing essential
            information about the instrument:
            'close_price'
            'lt_day_high'
            'lt_day_low'
            'st_day_high'
            'st_day_low'
            'ticker'
            'atr'
            'pos_size_1'
            'pos_size_2'
        :param tfs_data: contains data in tfs section of config file
        :param account_size: value of the trading account
        :param portfolio_config: portfolio data from config file

        :return: ???
        """
        tfs_settings = self.get_tfs_settings(tfs_settings)  # dict

        if instrument_data['position_info'].shape[0] == 0:  # no positions
            self._create_first_unit(
                instrument_data,
                tfs_settings,
                account_size,
                portfolio_config)
        elif instrument_data['position_info'].shape[0] > 0:  # >=1 positions
            new_unit = self._add_new_unit(
                instrument_data,
                tfs_settings,
                account_size,
                portfolio_config)

    def _potential_new_unit(self, close_price, lt_day_high, lt_day_low):
        """Checks if a new unit has to be created.

        :param close_price: close price of instrument
        :param lt_day_high: the long term (55 day) high
        :param lt_day_low: the long term (55 day) low

        :return: (True/False, long/short)
        """

        pos_type = "long"
        create_new_unit = False

        if close_price > lt_day_high:
            create_new_unit = True
        elif close_price < lt_day_low:
            pos_type = "short"
            create_new_unit = True
        else:
            return

        return (create_new_unit, pos_type)

    def _get_stop_and_target(self, position_info):
        """
        Retrieves stop price and next target price
        for a given position.

        :param position_info: information about the positions
        :return: 2 dimensional tuple with stop price and
            next target price, np.nan if empty (so no open positions)
        """

        pos_size = position_info['position_info'].pos_size.min()
        if pos_size < 0:
            price_target = position_info['position_info'].next_price_target.min()
            stop_price = position_info['position_info'].stop_price.min()
        elif pos_size > 0:
            price_target = position_info['position_info'].next_price_target.max()
            stop_price = position_info['position_info'].stop_price.max()

        if not pd.isnull(pos_size):
            return stop_price, price_target
        else:
            return np.nan, np.nan

    def get_specific_data_from_mkt_data(self, market_data, atts=None):
        """Retrieves specific attributes from the market data dataframe

        :param market_data: dataframe with market data
        :param atts: list of attributes to retrieve

        :return: tuple with required attributes
        """

        if atts is None:
            return
        else:
            try:
                att_values = []
                for att in atts:
                    att_value = market_data[att].dropna()[-1:].min()
                    # float("{0:.4f}".format(today_low))
                    att_values.append(float("{0:.4f}".format(att_value)))
            except Exception as e:
                raise GetDataFromMarketDataException(e)
            return att_values

    def prepare_orders(self, eod_data, instr_list):
        """Checks if orders have to be prepared based on
        the EOD data.

        :param eod_data: dataset with all relevant EOD data
        :param instr_list: portfolio data from config file

        :return: ???
        """

        prepared_orders = []
        db = Database()

        candidates = \
            eod_data.loc[(eod_data['close'] < eod_data['55DayLow'])
                         | (eod_data['close'] > eod_data['55DayHigh'])]

        if not candidates.empty:
            for index, row in candidates.iterrows():
                ticker = row['ticker']
                quantity = row['pos_size (1st)']
                if row['close'] > row['55DayHigh']:
                    action = "BUY"
                elif row['close'] < row['55DayLow']:
                    action = "SELL"

                meta = [ins for ins in instr_list
                        if ins[0].upper() == ticker][0]
                meta_dict = self._get_instrument_metadata(meta)
                db.add_order_to_queue(quantity, action, "LMTADP",
                                      ticker=ticker,
                                      sectype=meta_dict['sec_type'],
                                      exchange=meta_dict['exchange'],
                                      currency=meta_dict['currency'])

                prepared_orders.append(ticker)

        return prepared_orders

    def _prepare_order(self, eod_data, portfolio_config, action,
                       unit_nr=1):
        """Checks if orders have to be prepared based on
        the EOD data.

        :param eod_data: dataset with all relevant EOD data
            'close_price'
            'lt_day_high'
            'lt_day_low'
            'st_day_high'
            'st_day_low'
            'ticker'
            'atr'
            'pos_size_1'
            'pos_size_2'
        :param portfolio_config: portfolio data from config file

        :return: ???
        """

        db = Database()

        ticker = eod_data['ticker']
        if unit_nr == 1:
            quantity = eod_data['pos_size_1']
        else:
            quantity = eod_data['pos_size_2']

        meta = [ins for ins in portfolio_config
                if ins[0].upper() == ticker][0]
        meta_dict = self._get_instrument_metadata(meta)

        db.add_order_to_queue(quantity, action, "LMTADP",
                              ticker=ticker,
                              sectype=meta_dict['sec_type'],
                              exchange=meta_dict['exchange'],
                              currency=meta_dict['currency'],
                              unit_nr=unit_nr)

        return ticker

    def trace_order_status(self, orderId, order_details):
        """Monitor the status of an order and
        takes action to update the pending orders
        table.

        :param order_details: object with order info
                    orderInformation(
                    orderId, status=status, filled=filled,
                    avgFillPrice=avgFillPrice, permid=permid,
                    parentId=parentId, lastFillPrice=lastFillPrice,
                    clientId=clientId, whyHeld=whyHeld)
        :return:
        """

        self.logger.info(
            "ORDER DETAILS UPDATE: "
            "Order status: %s, "
            "filled: %s, "
            "avgFillPrice: %s, "
            "lastFillPrice: %s, "
            "order id: %s." %
            (order_details.status, order_details.filled,
             order_details.avgFillPrice, order_details.lastFillPrice,
             orderId))

        if order_details.status.lower() == "filled":
            self.logger.info("ORDER %s FILLED!" % (orderId))
