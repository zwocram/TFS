import logging
import time
import pandas as pd
from utils import orders
from utils.driver import Driver
from utils.strategies import Strategy

from db.database import Database
from ib.ibexceptions import NoBidAskPricesAvailable
import pdb

from ibapi.contract import Contract


class SOD(object):
    """Defines the start of day process
    """

    def __init__(self, ib):
        self.logger = logging.getLogger(__name__)
        self.driver = Driver()
        self.ib = ib
        self.db = Database()
        self.strategy = Strategy()

    def _create_contract(self, pending_order):
        """Creates an IB contract.

        :param pending_order: data to create a new order

        :return: IB Contract
        """

        ibcontract = Contract()
        ticker = pending_order[1]
        sectype = pending_order[2]
        exchange = pending_order[3]
        currency = pending_order[4]

        ibcontract.secType = sectype
        ibcontract.symbol = ticker
        ibcontract.exchange = exchange
        ibcontract.currency = currency

        return self.ib.resolve_ib_contract(ibcontract)

    def _send_order(self, prices, pending_order, ibcontract):
        """Send an order to IB.

        :param prices: bid/ask prices
        :param pending_order: the pending order

        :return: nothing
        """
        os = orders.OrderSamples()

        order_queue_id = pending_order[0]
        order_type = pending_order[5]
        quantity = pending_order[6]
        action = pending_order[7]

        price = prices[0] if action == "BUY" else prices[1]
        adptv = True if order_type == "LMTADP" else False
        order = os.LimitOrder(action, quantity, price,
                              adaptive=adptv, priority="Normal")

        orderId = self.ib.place_new_IB_order(ibcontract, order)
        self.db.update_order_queue(order_queue_id, orderId)

    def start(self, live_act_nbt, conn_acct_nbr):
        """Starts the start of day process.

        :param live_act_nbt: the live account number
        :param conn_acct_nbr: the account the program is connected to
        """

        self.logger.info("Check if we have to send orders to IB...")
        if conn_acct_nbr != live_act_nbt:  # we're on simu!
            self.logger.info("Security check: we're using simulated "
                             "account so continue safely.")

            pending_orders = self.db.get_pending_orders().fetchall()
            for row in pending_orders:
                ibcontract = self._create_contract(row)
                prices = self.strategy.get_specific_market_data(
                    self.ib, ibcontract, req_atts=['bid_price', 'ask_price'])
                if prices is None:
                    self.logger.error(
                        "No prices found for %s." %
                        ibcontract.symbol)
                else:
                    send_order = self._send_order(prices, row, ibcontract)
        else:
            self.logger.info("Be careful, we're on live account. Don't trade!")
