import sqlite3
import pandas as pd


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
                , pos.risk_exposure
                , u.unit_id
                , u.next_price_target
            from Position pos
                inner join instrument ins
                    on pos.instr_id = ins.instr_id
                inner join Unit u
                    on u.pos_id = pos.pos_id
            where ins.ticker = '{0}'
                and date_closed is not null;
            """.format(ticker)

        df = pd.read_sql_query(sql, self._db_connection)
        return df
