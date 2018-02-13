# Gist example of IB wrapper ...
#
# Download API from http://interactivebrokers.github.io/#
#
# Install python API code /IBJts/source/pythonclient $ python3 setup.py install
#
# Note: The test cases, and the documentation refer to a python package called IBApi,
#    but the actual package is called ibapi. Go figure.
#
# Get the latest version of the gateway:
# https://www.interactivebrokers.com/en/?f=%2Fen%2Fcontrol%2Fsystemstandalone-ibGateway.php%3Fos%3Dunix
#    (for unix: windows and mac users please find your own version)
#
# Run the gateway
#
# user: edemo
# pwd: demo123
#
# Now I'll try and replicate the time telling example


from ibapi.wrapper import EWrapper
from ibapi.client import EClient
from ibapi.contract import Contract
from threading import Thread
import queue
import time
import datetime
import logging
import pandas as pd
import numpy as np

import pdb
from ib.ibexceptions import *

MAX_WAIT_SECONDS = 10
DEFAULT_GET_CONTRACT_ID = 1001
DEFAULT_HISTORIC_DATA_ID = 50
DEFAULT_GET_CONTRACT_ID = 43
MAX_DAYS_HISTORY = '60 D'
DEFAULT_MARKET_DATA_ID = 50
DEFAULT_GET_CONTRACT_ID = 43

# marker for when queue is finished
FINISHED = object()
STARTED = object()
TIME_OUT = object()

ACCOUNT_UPDATE_FLAG = "update"
ACCOUNT_VALUE_FLAG = "value"
ACCOUNT_TIME_FLAG = "time"


class finishableQueue(object):

    def __init__(self, queue_to_finish):

        self._queue = queue_to_finish
        self.status = STARTED

    def get(self, timeout):
        """
        Returns a list of queue elements once timeout is finished,
        or a FINISHED flag is received in the queue

        :param timeout: how long to wait before giving up
        :return: list of queue elements
        """
        contents_of_queue = []
        finished = False

        while not finished:
            try:
                current_element = self._queue.get(timeout=timeout)
                if current_element is FINISHED:
                    finished = True
                    self.status = FINISHED
                else:
                    contents_of_queue.append(current_element)
                    # keep going and try and get more data

            except queue.Empty:
                # If we hit a time out it's most probable we're not
                # getting a finished element any time soon
                # give up and return what we have
                finished = True
                self.status = TIME_OUT

        return contents_of_queue

    def timed_out(self):
        return self.status is TIME_OUT


def _nan_or_int(x):
    if not np.isnan(x):
        return int(x)
    else:
        return x


class stream_of_ticks(list):
    """
    Stream of ticks
    """

    def __init__(self, list_of_ticks):
        super().__init__(list_of_ticks)

    def as_pdDataFrame(self):

        if len(self) == 0:
            # no data; do a blank tick
            return tick(datetime.datetime.now()).as_pandas_row()

        pd_row_list = [tick.as_pandas_row() for tick in self]
        pd_data_frame = pd.concat(pd_row_list)

        return pd_data_frame


class tick(object):
    """
    Convenience method for storing ticks
    Not IB specific, use as abstract
    """

    def __init__(self, timestamp, bid_size=np.nan, bid_price=np.nan,
                 ask_size=np.nan, ask_price=np.nan,
                 last_trade_size=np.nan, last_trade_price=np.nan,
                 ignorable_tick_id=None):

        # ignorable_tick_id keyword must match what is used in the IBtick class

        self.timestamp = timestamp
        self.bid_size = _nan_or_int(bid_size)
        self.bid_price = bid_price
        self.ask_size = _nan_or_int(ask_size)
        self.ask_price = ask_price
        self.last_trade_size = _nan_or_int(last_trade_size)
        self.last_trade_price = last_trade_price

    def __repr__(self):
        return self.as_pandas_row().__repr__()

    def as_pandas_row(self):
        """
        Tick as a pandas dataframe, single row, so we can concat together
        :return: pd.DataFrame
        """

        attributes = ['bid_size', 'bid_price', 'ask_size', 'ask_price',
                      'last_trade_size', 'last_trade_price']

        self_as_dict = dict([(attr_name, getattr(self, attr_name)) for attr_name in attributes])

        return pd.DataFrame(self_as_dict, index=[self.timestamp])


class IBtick(tick):
    """
    Resolve IB tick categories
    """

    def __init__(self, timestamp, tickid, value):

        resolve_tickid = self.resolve_tickids(tickid)
        super().__init__(timestamp, **dict([(resolve_tickid, value)]))

    def resolve_tickids(self, tickid):

        tickid_dict = dict([("0", "bid_size"), ("1", "bid_price"),
                            ("2", "ask_price"), ("3", "ask_size"),
                            ("4", "last_trade_price"),
                            ("5", "last_trade_size")])

        if str(tickid) in tickid_dict.keys():
            return tickid_dict[str(tickid)]
        else:
            # This must be the same as the argument name in the parent class
            return "ignorable_tick_id"


class IBWrapper(EWrapper):
    """
    The wrapper deals with the action coming back from the IB
    gateway or TWS instanc. We override methods in EWrapper
    that will get called when this action happens, like currentTime.
    """

    _msg_queue = queue.Queue()
    _contract_details = {}
    _my_historic_data_dict = {}
    _accepted_error_codes = [2106, 2107]
    _my_accounts = {}

    def __init__(self):
        self._my_contract_details = {}
        self._my_market_data_dict = {}

    def add_to_queue_class(ibwrapper_function):
        def wrap_the_wrapper(*args, **kwargs):
            _self = args[0]  # just in case
            ibwrapper_function(*args, **kwargs)
        return wrap_the_wrapper

    @add_to_queue_class
    def currentTime(self, time_from_server):
        # Overriden method
        self._msg_queue.put(time_from_server)
        # self._time_queue.put(time_from_server)

    # error handling code
    def init_error(self):
        error_queue = queue.Queue()
        self._my_errors = error_queue

    def get_error(self, timeout=5):
        if self.is_error():
            try:
                return self._my_errors.get(timeout=timeout)
            except queue.Empty:
                return None

        return None

    def is_error(self):
        an_error_if = not self._my_errors.empty()
        return an_error_if

    def error(self, id, errorCode, errorString):
        # Overriden method
        if errorCode not in self._accepted_error_codes:
            errormsg = "IB error id %d errorcode %d string %s" % (id, errorCode, errorString)
            self._my_errors.put(errormsg)

    # get contract details code
    def init_contractdetails(self, reqId):
        contract_details_queue = self._my_contract_details[reqId] = queue.Queue()

        return contract_details_queue

    def contractDetails(self, reqId, contractDetails):
        # overridden method

        if reqId not in self._my_contract_details.keys():
            self.init_contractdetails(reqId)

        self._my_contract_details[reqId].put(contractDetails)

    def contractDetailsEnd(self, reqId):
        # overriden method
        if reqId not in self._my_contract_details.keys():
            self.init_contractdetails(reqId)

        self._my_contract_details[reqId].put(FINISHED)

    # Historic data code
    def init_historicprices(self, tickerid):
        historic_data_queue = self._my_historic_data_dict[tickerid] = queue.Queue()

        return historic_data_queue

    def historicalData(self, tickerid, bar):

        # Overriden method
        # Note I'm choosing to ignore barCount, WAP and hasGaps but you could use them if you like
        bardata = (bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume)

        historic_data_dict = self._my_historic_data_dict

        # Add on to the current data
        if tickerid not in historic_data_dict.keys():
            self.init_historicprices(tickerid)

        historic_data_dict[tickerid].put(bardata)

    def historicalDataEnd(self, tickerid, start: str, end: str):
        # overriden method

        if tickerid not in self._my_historic_data_dict.keys():
            self.init_historicprices(tickerid)

        self._my_historic_data_dict[tickerid].put(FINISHED)

    # market data
    def init_market_data(self, tickerid):
        market_data_queue = self._my_market_data_dict[tickerid] = queue.Queue()

        return market_data_queue

    def get_time_stamp(self):
        # Time stamp to apply to market data
        # We could also use IB server time
        return datetime.datetime.now()

    def tickPrice(self, tickerid, tickType, price, attrib):
        # overriden method

        # For simplicity I'm ignoring these but they could be useful to you...
        # See the documentation http://interactivebrokers.github.io/tws-api/md_receive.html#gsc.tab=0
        # attrib.canAutoExecute
        # attrib.pastLimit

        this_tick_data = IBtick(self.get_time_stamp(), tickType, price)
        self._my_market_data_dict[tickerid].put(this_tick_data)

    def tickSize(self, tickerid, tickType, size):
        # overriden method

        this_tick_data = IBtick(self.get_time_stamp(), tickType, size)
        self._my_market_data_dict[tickerid].put(this_tick_data)

    def tickString(self, tickerid, tickType, value):
        # overriden method

        # value is a string, make it a float, and then in the parent class will be resolved to int if size
        this_tick_data = IBtick(self.get_time_stamp(), tickType, float(value))
        self._my_market_data_dict[tickerid].put(this_tick_data)

    def tickGeneric(self, tickerid, tickType, value):
        # overriden method

        this_tick_data = IBtick(self.get_time_stamp(), tickType, value)
        self._my_market_data_dict[tickerid].put(this_tick_data)

    # get account summary
    def init_accounts(self, reqId):
        accounting_queue = self._my_accounts[reqId] = queue.Queue()

        return accounting_queue

    def accountSummary(self, reqId: int, account: str, tag: str, value: str,
                       currency: str):

        # use this to seperate out different account data
        data = (reqId, account, tag, value, currency)
        self._my_accounts[reqId].put(data)

    def accountSummaryEnd(self, reqId: int):
        self._my_accounts[reqId].put(FINISHED)


class IBClient(EClient):
    """
    The client method
    We don't override native methods,
    but instead call them from our own wrappers
    """

    def __init__(self, wrapper):
        # Set up with a wrapper inside
        EClient.__init__(self, wrapper)
        self._market_data_q_dict = {}

    def get_time(self):
        """
        Basic example to tell the time
        :return: unix time, as an int
        """

        print("Getting the time from the server... ")

        # This is the native method in EClient,
        # asks the server to send us the time please
        self.reqCurrentTime()

        try:
            current_time = self._msg_queue.get(timeout=MAX_WAIT_SECONDS)
            print('and the time is....', current_time)
        except queue.Empty:
            print("Exceeded maximum wait for wrapper to respond")

    def resolve_ib_contract(self, ibcontract, reqId=DEFAULT_GET_CONTRACT_ID):
        """
        From a partially formed contract, returns a fully fledged version
        :returns fully resolved IB contract
        """

        # Make a place to store the data we're going to return
        contract_details_queue = finishableQueue(self.init_contractdetails(reqId))

        self.reqContractDetails(reqId, ibcontract)

        # Run until we get a valid contract(s) or get bored waiting
        new_contract_details = contract_details_queue.get(timeout=MAX_WAIT_SECONDS)

        try:
            while self.wrapper.is_error():
                raise ResolveContractDetailsException(
                    "ResolveContractDetailsException", ibcontract.symbol,
                    self.get_error())
                # print(self.get_error())

            if contract_details_queue.timed_out():
                raise IBTimeoutException("IBTimeoutException",
                                         ibcontract.symbol, "Exceeded maximum wait for wrapper "
                                         " to confirm finished")

            if len(new_contract_details) == 0:
                raise UnresolvedContractException(
                    "UnresolvedContractException",
                    ibcontract.symbol,
                    "Failed to get additional "
                    "contract details: returning "
                    "unresolved contract")

            if len(new_contract_details) > 1:
                raise MultipleContractException("MultipleContractException",
                                                ibcontract.symbol, "got multiple contracts using first one")
        except (ResolveContractDetailsException, IBTimeoutException,
                UnresolvedContractException, MultipleContractException) as exp:
            logging.error(exp)
            return

        new_contract_details = new_contract_details[0]

        resolved_ibcontract = new_contract_details.summary
        logging.info("resolved contract")
        return resolved_ibcontract

    def start_getting_IB_market_data(self, resolved_ibcontract,
                                     tickerid=DEFAULT_MARKET_DATA_ID):
        """
        Kick off market data streaming
        :param resolved_ibcontract: a Contract object
        :param tickerid: the identifier for the request
        :return: tickerid
        """

        self._market_data_q_dict[tickerid] = self.wrapper.init_market_data(tickerid)
        self.reqMktData(tickerid, resolved_ibcontract, "", False, False, [])

        return tickerid

    def stop_getting_IB_market_data(self, tickerid):
        """
        Stops the stream of market data and returns all the data
        we've had since we last asked for it

        :param tickerid: identifier for the request
        :return: market data
        """

        # native EClient method
        self.cancelMktData(tickerid)

        # Sometimes a lag whilst this happens, this prevents 'orphan'
        # ticks appearing
        time.sleep(5)

        market_data = self.get_IB_market_data(tickerid)

        # output ay errors
        while self.wrapper.is_error():
            print(self.get_error())

        return market_data

    def get_IB_market_data(self, tickerid):
        """
        Takes all the market data we have received so far out of the
        stack, and clear the stack
        :param tickerid: identifier for the request
        :return: market data
        """

        # how long to wait for next item
        MAX_WAIT_MARKETDATEITEM = 5
        market_data_q = self._market_data_q_dict[tickerid]

        market_data = []
        finished = False

        while not finished:
            try:
                market_data.append(market_data_q.get(timeout=MAX_WAIT_MARKETDATEITEM))
            except queue.Empty:
                # no more data
                finished = True

        return stream_of_ticks(market_data)

    def get_IB_historical_data(self, ibcontract, duration=MAX_DAYS_HISTORY,
                               barSizeSetting="1 day",
                               tickerid=DEFAULT_HISTORIC_DATA_ID):
        """
        Returns historical prices for a contract, up to today
        ibcontract is a Contract
        :returns list of prices in 4 tuples: Open high low close volume
        """

        # Make a place to store the data we're going to return
        historic_data_queue = finishableQueue(self.init_historicprices(tickerid))

        # Request some historical data. Native method in EClient
        self.reqHistoricalData(
            tickerid,  # tickerId,
            ibcontract,  # contract,
            datetime.datetime.today().strftime("%Y%m%d %H:%M:%S %Z"),  # endDateTime,
            duration,
            barSizeSetting,
            "TRADES",  # whatToShow,
            1,  # useRTH,
            1,  # formatDate
            False,  # KeepUpToDate <<==== added for api 9.73.2
            []  # chartoptions not used
        )

        # Wait until we get a completed data, an error, or get bored waiting
        historic_data = historic_data_queue.get(timeout=MAX_WAIT_SECONDS)

        try:
            while self.wrapper.is_error():
                raise HistoricalDataRetrieveException(ibcontract.symbol,
                                                      self.get_error())

            if historic_data_queue.timed_out():
                raise HistoricalDataTimeoutException(ibcontract.symbol,
                                                     "Exceeded maximum wait \
                                                     for wrapper to confirm \
                                                     finished.")
        except (HistoricalDataRetrieveException,
                HistoricalDataTimeoutException) as exp:
            logging.error(exp)
            return

        self.cancelHistoricalData(tickerid)

        return historic_data

    def get_account_summary(self, reqId):
        """i
        Get the accounting values from IB server
        :return: accounting values as served up by IB
        """

        info_to_retrieve = "NetLiquidation,BuyingPower"
        accounting_queue = finishableQueue(self.init_accounts(reqId))
        self.reqAccountSummary(reqId, "All", info_to_retrieve)

        # Wait until we get a complete account data set.
        accounting_data = accounting_queue.get(timeout=MAX_WAIT_SECONDS)
        return accounting_data


class IB(IBWrapper, IBClient):
    def __init__(self, ipaddress, portid, clientid):
        IBWrapper.__init__(self)
        IBClient.__init__(self, wrapper=self)

        self.connect(ipaddress, portid, clientid)

        thread = Thread(target=self.run)
        thread.start()

        setattr(self, "_thread", thread)
        self.init_error()
