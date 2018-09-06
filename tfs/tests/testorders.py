import pandas as pd
import sqlite3
from nose.tools import *
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from db.database import Database


class Prepare(object):
    pass


class TestOrderQueue(object):

    def setup(self):
        self.params = ('DBA', 'STK', 'SMART', 'USD', 'LMTADP',
                       429, 'SELL', 'pending', '2018-08-29T21:21:41.925384',
                       None, 4)
        self.sql = """
            insert into OrderQueue(ticker, contract_sectype,
                contract_exchange, contract_currency, order_type,
                quantity, action, status, dat_entered, dat_updated, unit_nr)
            values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        self.conn = sqlite3.connect("/Users/Marco/Documents/python/TFS/tfs/db/tfs.db")
        db = Database()

    def teardown(self):
        pass

    def test_add_order_to_queue(self):
        crs = self.conn.cursor()
        crs.execute(self.sql, self.params)
        # self.conn.commit()

    def test_add_order_function(self):
        quantity = 201
        action = "BUY"
        order_type = "short"
        ticker = "SD"
        sectype = "STK"
        exchange = "ARCA"
        currency = "EUR"
        unit_nr = 4
        self.db.add_order_to_queue(quantity, action, order_type,
                                   ticker=ticker, sectype=sectype, exchange=exchange,
                                   currency=currency, unit_nr=unit_nr)
