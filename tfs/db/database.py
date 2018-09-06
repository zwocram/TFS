import sqlite3
import pickle
import pandas as pd
import datetime
import logging
from ib.ibexceptions import *

import pdb


class Database(object):
    """Make connection to the database.

    :Example:

    >>> from db.database import Database
    >>> db = Database()
    >>> db.<function that generates query>

    More info: https://www.pythoncentral.io/introduction-to-sqlite-in-python/
    """
    _db_connection = None
    _db_cursor = None

    def __init__(self):
        self._db_connection = sqlite3.connect('db/tfs.db')
        self._db_cursor = self._db_connection.cursor()
        self.logger = logging.getLogger(__name__)

    def _exec_query(self, query, params=None):
        """Executes a query against the database. Any query can be
        executed: select, insert or update statements
        """
        crs = None

        try:
            crs = self._db_cursor.execute(query, params)
        except sqlite3.IntegrityError:
            print("Can't execute query; record already exists: \n", query)
        except Exception as e:
            self.logger.warning("Unexpected error ('%s') while executing: %s",
                                e, query)
            print('Unexpected error while executing: \n', e)
        finally:
            self._db_connection.commit()

        return crs

    def __del__(self):
        self._db_connection.close()

    def _get_category_id(self, category_name):
        """Retrieves the category id for a given instrument
        category name.

        :param category_name: category name of instrument

        :return: the category_id of the given category name
        """

        category_id = 0
        params = (category_name,)
        try:
            sql = """
                select cat_id
                from InstrumentCategory
                where cat_description = ?
                """

            df = pd.read_sql_query(sql, self._db_connection, params=params)
            category_id = df.cat_id.min()
            return category_id
        except:
            raise GetCategoryIDException("error getting category "
                                         "id for category name {1}".format(category_name))
            return category_id

    def insert_account_numbers(self, date, account_size, buying_power):
        """
        Add account size and buying power to the database.
        :param conn:
        :param project:
        :return: project id
        """

        params = (date, account_size, buying_power)

        sql = """insert into Account(date, account_size, buying_power)
            values(?, ?, ?)"""

        self._exec_query(sql, params)

    def select_equity_timeseries(self):
        """
        Select all data from the Account tabel.
        """

        sql = "select * from Account"

        return self._exec_query(sql)

    def get_settings_from_db(self, params=None):
        """
        Select all or specific settings from the
        Settings table.

        :param params: parameters to retrieve. If None, get all

        :return: dictionary with params and values
        """

        sql = """
            select *
            from Settings
            where param in (%s)
            """ % ','.join('?' * len(params))

        settings_dict = {}
        settings = self._exec_query(sql, params).fetchall()
        for setting in settings:
            settings_dict[setting[0]] = setting[1]

        return settings_dict

    def get_position_size(self, ticker):
        """
        Get the active position size of an instrument.

        :param ticker:
        """
        params = (ticker,)

        sql = """
            select
                pos.pos_size
                , pos.pos_id
                , pos.risk_exposure
                , u.entry_price as unit_entry_price
                , u.unit_id
                , u.next_price_target
                , u.stop_price
                , u.pos_size as unit_pos_size
                , tc.transaction_costs as tc
            from Position pos
                inner join instrument ins
                    on pos.instr_id = ins.instr_id
                inner join TransactionCosts tc
                    on ins.instr_category_id = tc.instr_cat_id
                inner join Unit u
                    on u.pos_id = pos.pos_id
            where ins.ticker = ?
                and date_closed is null;
            """

        df = pd.read_sql_query(sql, self._db_connection, params=params)
        return df

    def create_unit(self, unit, unit_id, position_id,
                    position_type=None):
        """Insert a unit in the database

        :param unit: the unit to be inserted
        :param unit_id: this has to be smaller than the max allowed
                        number of units
        :param position_id: a unit has to have a position attached to it
        :param position_type: long or short
        """

        price_target = 0

        if position_type == "long":
            price_target = unit.price + unit.atr
        elif position_type == "short":
            price_target = unit.price - unit.atr

        if unit_id > 1:
            first_unit = False
        else:
            first_unit = True

        params = (unit_id,
                  position_id,
                  unit.price,
                  unit.atr, price_target,
                  unit.calc_position_size_risk_perc(
                      first_unit=first_unit,
                      long_short=position_type),
                  unit.stop_level)

        sql = """
            insert into Unit(unit_id, pos_id, entry_price,
                atr, next_price_target, pos_size, stop_price)
            values(?, ?, ?, ?, ?, ?, ?)
            """

        return self._exec_query(sql, params)

    def create_position(self, instrument_id, date_open):
        """Create a new position in the database.

        :param instr_id: the id of the instrument for which the
        position is created
        :param date_open: the date on which the position was openend.
        """

        params = (instrument_id, date_open)

        sql = """
            insert into Position(instr_id, date_open)
            values(?, ?)
            """

        return self._exec_query(sql, params)

    def get_instrument_id(self, instrument_ticker):
        """retrieves the instrument id

        :param instrument_ticker: the ticker for the instrument_ticker
        """

        params = (instrument_ticker,)
        sql = """
            select instr_id
            from Instrument
            where ticker = ?
            """

        df = pd.read_sql_query(sql, self._db_connection, params=params)
        return df

    def update_position(self, position_info=None,
                        break_even=False):
        """Updates position information for an instrument

        :param position_info: current information for the position
        :param break_even: boolean indicating that we are already fully
            loaded so we only have to update the stop level of the
            position (True).
        """
        if not break_even:
            # update average_price
            total_pos_size = position_info.unit_pos_size.sum()
            position_id = position_info.pos_id.min()
            position_info['temp_total_costs'] = (
                position_info['unit_entry_price'] *
                position_info['unit_pos_size']) + position_info['tc']
            average_price = float("{0:.3f}".format(
                position_info.temp_total_costs.sum() /
                total_pos_size))

            stop_price = float("{0:.2f}".format(position_info.loc[
                position_info['unit_id'] ==
                position_info.unit_id.max()].stop_price.iloc[0]))

            params = (average_price, total_pos_size, stop_price,
                      position_id)
            sql = """
                update Position
                set avg_price = ?
                    , pos_size = ?
                    , stop_price = ?
                where pos_id = ?
                """

            self._exec_query(sql, params)

    def exists_instrument(self, instrument):
        """Checks if instrument exists in databaseself.

        :param instrument: tuple of instrument metadata

        :return: False/True if instrument exists or not
        """

        exists = True

        try:
            ticker = instrument[0].upper()
            instrument_category = instrument[1].split(',')[5].lstrip()

            params = (ticker, instrument_category)
            sql = """
                select
                    i.ticker
                    , i.description
                    , c.cat_description
                from Instrument i inner join InstrumentCategory c
                    on i.instr_category_id = c.cat_id
                where
                    i.ticker = ?
                    and c.cat_description = ?
                """

            df = pd.read_sql_query(sql, self._db_connection, params=params)
            if df.size == 0:
                exists = False
            return exists
        except Exception as e:
            raise CheckInstrumentExistenceException(e)

    def get_pending_orders(self):
        """Selects pending orders from the database.

        :return: set with pending orders
        """

        params = ('pending',)

        sql = """
            select *
            from OrderQueue
            where status = ?
            """

        return self._exec_query(sql, params)

    def add_order_to_queue(self, quantity, action, order_type,
                           ibcontract=None, ticker="", sectype="",
                           exchange="", currency="", unit_nr=1):
        """Add contract data to order queue tabel
        to prepare for the next trading session.

        :param contract: ib contract
        :param quantity: how much to buy/sell
        :param action: do we buy or sell?

        :return: inserted item id in OrdersQueue table
        """

        if ibcontract is not None:
            ticker = ibcontract.symbol
            sectype = ibcontract.secType
            exchange = ibcontract.exchange
            currency = ibcontract.currency

        status = "pending"
        dat_entered = datetime.datetime.now().isoformat()

        params = (ticker, sectype, exchange, currency, order_type,
                  quantity, action, status, dat_entered, unit_nr)

        sql = """
            insert into OrderQueue(ticker, contract_sectype,
                contract_exchange, contract_currency, order_type,
                quantity, action, status, dat_entered, unit_nr)
            values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

        print("Executing sql in add_order_to_queue.\n"
              "params: {0}\n"
              "sql: {1}".format(params, sql)
              )
        try:
            self._exec_query(sql, params)
        except Exception as e:
            raise AddContractToQueueException(e)

    def insert_new_instrument(self, instrument):
        """Checks if instrument exists in databaseself.

        :param instrument: tuple of instrument metadata

        :return: False/True if instrument exists or not
        """

        try:
            ticker = instrument[0].upper()
            instrument_description = instrument[1].split(',')[0].lstrip()
            instrument_category = instrument[1].split(',')[5].lstrip()
            category_id = self._get_category_id(instrument_category)

            params = (ticker, instrument_description, category_id)
            sql = """
                insert into
                    Instrument(ticker, description, instr_category_id)
                values(?, ?, ?)
                """

            return self._exec_query(sql, params).lastrowid
        except Exception as e:
            raise InsertNewInstrumentException(e)

    def update_order_queue(self, order_queue_id, ib_orderId):
        """Updates the OrderQueue table with an actual IB orderId

        :param order_queue_id: record id of the order to update
        :param ib_orderId: orderId that IB provided
        """

        params = (ib_orderId, order_queue_id)
        sql = """
            update OrderQueue
            set order_id = ?
            where order_queue_id = ?
            """

        self._exec_query(sql, params)
