import sqlite3


class DBConnect(object):
    '''Make connection to the database.

    Usage::

        >>> import db
        >>> db = DBConnect()
        >>> db.query(query, params)
    '''
    _db_connection = None
    _db_cursor = None

    def __init__(self):
        self._db_connection = sqlite3.connect('db/tfs.db')
        self._db_cursor = self._db_connection.cursor()

    def query(self, query, params):
        return self._db_cursor.execute(query, params)

    def insert_query(self, table_name, params):
        query = "insert into Metrics() values()"
        return self._db_cursor.execute(query)

    def __del__(self):
        self._db_connection.close()
