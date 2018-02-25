from utils.strategies import Unit

import sqlite3
import pandas as pd

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

    def _exec_query(self, query):
        """Executes a query against the database. Any query can be
        executed: select, insert or update statements
        """
        crs = None

        try:
            crs = self._db_cursor.execute(query)
        except sqlite3.IntegrityError:
            print("Can't execute query; record already exists: \n", query)
        except StandardError as e:
            print('Unexpected error: \n', e)
        finally:
            self._db_connection.commit()

        return crs

    def __del__(self):
        self._db_connection.close()

    def insert_account_numbers(self, date, account_size, buying_power):
        """
        Add account size and buying power to the database.
        :param conn:
        :param project:
        :return: project id
        """

        sql = """insert into Account(date, account_size, buying_power)
            values({0}, {1}, {2})""".format(date, account_size,
                                            buying_power)

        self._exec_query(sql)

    def select_equity_timeseries(self):
        """
        Select all data from the Account tabel.
        """

        sql = "select * from Account"

        return self._exec_query(sql)

    def get_position_size(self, ticker):
        """
        Get the active position size of an instrument.

        :param ticker:
        """

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
            where ins.ticker = '{0}'
                and date_closed is null;
            """.format(ticker)

        df = pd.read_sql_query(sql, self._db_connection)
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

        sql = """
            insert into Unit(unit_id, pos_id, entry_price,
                atr, next_price_target, pos_size, stop_price)
            values({0}, {1}, {2}, {3}, {4}, {5}, {6})
            """.format(unit_id, position_id, unit.price,
                       unit.atr, price_target, unit.pos_size,
                       unit.stop_level)

        return self._exec_query(sql).lastrowid

    def create_position(self, instrument_id, date_open):
        """Create a new position in the database.

        :param instr_id: the id of the instrument for which the
        position is created
        :param date_open: the date on which the position was openend.
        """

        sql = """
            insert into Position(instr_id, date_open)
            values({0}, '{1}')
            """.format(instrument_id, date_open)

        return self._exec_query(sql).lastrowid

    def get_instrument_id(self, instrument_ticker):
        """retrieves the instrument id

        :param instrument_ticker: the ticker for the instrument_ticker
        """

        sql = """
            select instr_id
            from Instrument
            where ticker = '{0}'
            """.format(instrument_ticker)

        df = pd.read_sql_query(sql, self._db_connection)
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
                position_info.unit_id.max()].stop_price[1]))

            sql = """
                update Position
                set avg_price = {0}
                    , pos_size = {1}
                    , stop_price = {2}
                where pos_id = {3}
                """.format(average_price, total_pos_size, stop_price,
                           position_id)

            self._exec_query(sql)