import math
import numpy as np
import pandas as pd
import logging
from sqlalchemy import create_engine
from pytz import timezone

from pandas.tseries.holiday import AbstractHolidayCalendar, Holiday, nearest_workday, \
    USMartinLutherKingJr, USPresidentsDay, GoodFriday, USMemorialDay, \
    USLaborDay, USThanksgivingDay


from ib_insync import *


import datetime
import pdb


class USTradingCalendar(AbstractHolidayCalendar):
    rules = [
        Holiday('NewYearsDay', month=1, day=1, observance=nearest_workday),
        USMartinLutherKingJr,
        USPresidentsDay,
        GoodFriday,
        USMemorialDay,
        Holiday('USIndependenceDay', month=7, day=4,
                observance=nearest_workday),
        USLaborDay,
        USThanksgivingDay,
        Holiday('Christmas', month=12, day=25, observance=nearest_workday)
    ]


EAST_TZ = timezone('US/Eastern')
NL_TZ = timezone('Europe/Amsterdam')


EOD_NY_TIME = datetime.time(16, 0)
SOD_NY_TIME = datetime.time(9, 30)

TIB_DB = 'sqlite:///tfs/db/tib.db'

AVG_DOLLAR_20D_SYS_1 = 50000000
AVG_DOLLAR_20D_SYS_2 = 25000000
ATR_10_PERC_FLOOR_SYS3 = 0.05
AVG_VOL_50D = 1000000

FOREX_LIST = ['EURUSD']

SYS_PRICE_CHECK_1 = 5
SYS_PRICE_CHECK_2 = 1
ATR_40_PERC_CAP = 0.10
THREE_DAY_PERC_CHANGE_SYS_3_CAP = -0.125
ATR_10_PERC_FLOOR_SYS2 = 0.03
RSI_3D_FLOOR_SYS_2 = 90

SYS_1_ATR = 'ATR20'
SYS_1_ATR_FACTOR = 2
SYS_1_MAX_EXPOSURE = 0.10
SYS_1_BUDGET = 0.25
SYS_1_RISK_PERC = 0.02


SYS_2_ATR = 'ATR10'
SYS_2_ATR_FACTOR = 3
SYS_2_MAX_EXPOSURE = 0.10
SYS_2_BUDGET = 0.50
SYS_2_RISK_PERC = 0.02
SYS_2_LMT_PRICE_PERC = 0.04
SYS_2_EXPIRATION_DATE = pd.offsets.CustomBusinessDay(
    2, calendar=USTradingCalendar())

SYS_3_ATR = 'ATR10'
SYS_3_ATR_FACTOR = 2
SYS_3_MAX_EXPOSURE = 0.10
SYS_3_BUDGET = 0.25
SYS_3_RISK_PERC = 0.02
SYS_3_LMT_PRICE_PERC = 0.07
SYS_3_EXPIRATION_DATE = pd.offsets.CustomBusinessDay(
    3, calendar=USTradingCalendar())

logging.basicConfig()
log = logging.getLogger(__name__)


class Utils:

    @staticmethod
    def trades_to_df(trades):

        last_log_entry = None
        last_log_message = None

        try:
            last_log_entry = [trade.log[-1].time
                              if len(trade.log) > 0 else None
                              for trade in trades]
            last_log_entry_date = [trade.log[-1].time.strftime('%Y-%m-%d')
                                   if len(trade.log) > 0 else None
                                   for trade in trades]
            last_log_message = [trade.log[-1].message
                                if len(trade.log) > 0 else None
                                for trade in trades]
        except IndexError:
            pass

        trade_df = pd.DataFrame(
            {'TICKER': [trade.contract.symbol for trade in trades],
             'CONTRACT_ID': [trade.contract.conId for trade in trades],
             'ORDER_REF': [trade.order.orderRef for trade in trades],
             'SYS_ID': [int(trade.order.orderRef[:1]) if len(trade.order.orderRef) > 0 else np.nan
                        for trade in trades],
             'ORDER_ID': [trade.order.orderId for trade in trades],
             'ACTION': [trade.order.action for trade in trades],
             'QUANTITY': [trade.order.totalQuantity for trade in trades],
             'ORDER_TYPE': [trade.order.orderType for trade in trades],
             'LMT_PRICE': [trade.order.lmtPrice for trade in trades],
             'STP_PRICE': [trade.order.auxPrice for trade in trades],
             'ACCOUNT': [trade.order.account for trade in trades],
             'STATUS': [trade.orderStatus.status for trade in trades],
             'TIB_STATUS': ['OPEN' if trade.orderStatus.status == 'Filled'
                            else np.nan for trade in trades],
             'FILLED': [trade.orderStatus.filled for trade in trades],
             'LAST_LOG_MESSAGE': last_log_message,
             'LAST_LOG_TIME': last_log_entry,
             'LAST_LOG_DATE': last_log_entry_date})

        return trade_df


class IBEventsHandling:
    ib = None

    def __init__(self, IB):
        self.ib = IB

    def timeoutEvent(self, idlePeriod):
        print('waited long enough; exiting the program...')
        self.ib.disconnect()
        exit()

    def newOrderEvent(self, Trade):
        trade = Trade
        # print('new order: ', Utils.trade_to_df(trade))

    def openOrderEvent(self, Trade):
        pass

    def cancelOrderEvent(self, Trade):
        pass

    def orderStatusEvent(self, Trade):
        trade = Trade
        # print(Utils.trade_to_df(trade))

    def positionEvent(self, Trade):
        pass


class Centurion:
    """
    Makes sure the entire trading process for the TIB
    system gets executed.
    """

    @staticmethod
    def start_trading(eod_data):
        """
        Args:
            conId (int): The unique IB contract identifier.
        """

        # do some date logic here
        today = datetime.date.today()
        next_business_day = pd.offsets.CustomBusinessDay(
            calendar=USTradingCalendar())
        print('today is', today)
        trading_calendar = USTradingCalendar()
        if today in trading_calendar.holidays(
                start=today, end=today).to_pydatetime():
            print('WATCH IT: today is NOT a us trading day')
        if today.weekday() in (5, 6):
            print('WATCH IT: today is a WEEKEND DAY')
        print('next business day is', today + next_business_day)
        print('system 2 expiration date is', today + SYS_2_EXPIRATION_DATE)
        print('system 3 expiration date is', today + SYS_3_EXPIRATION_DATE)

        try:

            tib_db = TIBDB()
            ib = IB()
            ib.connect('127.0.0.1', 4002, clientId=15)
            ibe = IBEventsHandling(ib)

            # get account value
            account_value = [v.value for v in ib.accountValues()
                             if v.tag == 'NetLiquidationByCurrency'
                             and v.currency == 'BASE'][0]

            forex_contracts = [Forex(f) for f in FOREX_LIST]
            forex_close_prices = [(
                contract.symbol+contract.currency,
                ib.reqHistoricalData(
                    contract, endDateTime='', durationStr='3 D',
                    barSizeSetting='1 day', whatToShow='MIDPOINT',
                    useRTH=True, formatDate=1, keepUpToDate=False)[-1].close)
                for contract in forex_contracts]

            print('account value: ', account_value)
            print('forex data: ', forex_close_prices)
            # portfolio = ib.portfolio()

        except ConnectionRefusedError:
            print("Cannot connect to IB but let's continue")

            # sys_ids = [1, 2, 3]
        sys_ids = [1, 2, 3]

        for sys_id in sys_ids:
            system = System(sys_id, eod_data, ib)
            result_set = system.filter_eod_data()
            result_set['SYS_ID'] = sys_id
            result_set['SHORTABLE'] = np.nan
            result_set['SEQ_NR'] = range(1, 1 + len(result_set))
            if sys_id == 2:
                result_set = system.check_shortability(result_set.copy())

            result_set = tib_db.check_portfolio_for_ticker(result_set.copy())
            # system.placeOrder(result_set)
            # https://stackoverflow.com/questions/30045086/pandas-left-join-and-update-existing-column
            # print(pd.merge(result_set, open_positions, on='TICKER', how='left'))
            # result_set = system.check_portfolio_for_position(result_set.copy())
            # df.insert(0, 'New_ID', range(880, 880 + len(df)))

            print(result_set)

        pd.set_option("max_rows", None)
        # pdb.set_trace()
        print(Utils.trades_to_df(ib.trades()))
        tib_db.save_trades_to_db(Utils.trades_to_df(ib.trades()))

        if ib.isConnected():
            # redirect to event handlers
            ib.timeoutEvent += ibe.timeoutEvent
            ib.newOrderEvent += ibe.newOrderEvent
            ib.openOrderEvent += ibe.openOrderEvent
            ib.cancelOrderEvent += ibe.cancelOrderEvent
            ib.orderStatusEvent += ibe.orderStatusEvent
            ib.positionEvent += ibe.positionEvent

            # HANDLE THE WAITING
            # ib.setTimeout(120)
            current_year = datetime.date.today().year
            current_month = datetime.date.today().month
            current_day = datetime.date.today().day

            us_time = EAST_TZ.localize(datetime.datetime(
                current_year,
                current_month,
                current_day, 16, 0, 0))

            nl_time = us_time.astimezone(NL_TZ).time()
            pdb.set_trace()

            # for t in ib.timeRange(right_now, right_now + ml, 60):
            print('waiting until', nl_time)
            if ib.waitUntil(nl_time):
                ib.sleep(10)  # make sure trading session is finished
                trades_df = Utils.trades_to_df(ib.trades())

                # save new filled trades to database
                filled_open = trades_df.query(
                    '(ORDER_REF.str.contains("-C") == False) '
                    '& (STATUS == "Filled")')
                tib_db.save_trades_to_db(filled_open, filled_open)

                # update position information on closed trades
                filled_close = trades_df.query(
                    '(ORDER_REF.str.contains("-C")) & '
                    '(STATUS == "Filled")')
                tib_db.update_closed_positions(filled_close)

                ib.disconnect()
                pdb.set_trace()
                exit()

            """
            right_now = datetime.datetime.now()
            if ib.waitUntil(right_now + md):
                ib.disconnect()
                exit()
            """

            # ib.run()


class System:
    """
    This class represents a trading system.
    """

    pandas_query = None
    sys_id = None
    eod_data = None
    ranking = None
    ranking_order = None
    ib = None

    def _set_ranking_attributes(self):

        if self.sys_id == 1:
            self.ranking = 'change_200D'
            self.ranking_order = 'Desc'
        elif self.sys_id == 2:
            self.ranking = 'ADX_7'
            self.ranking_order = 'Desc'
        elif self.sys_id == 3:
            self.ranking = 'change_3D'
            self.ranking_order = 'Asc'

    def _set_pandas_query(self):

        query = None

        if self.sys_id == 1:
            if len(self.eod_data.query(
                    '(TICKER == "SPY") and (Close > MA_100D)')) == 1:
                query = ('(AVG_DOLLAR_VOL_20D > @AVG_DOLLAR_20D_SYS_1) '
                         ' and (Close > @SYS_PRICE_CHECK_1)'
                         'and (MA_25D > MA_50D) ')
                # query += 'and ATR40_PERC > @ATR_40_PERC_CAP'
        elif self.sys_id == 2:
            query = ('(AVG_DOLLAR_VOL_20D > @AVG_DOLLAR_20D_SYS_2)'
                     ' and (Close > @SYS_PRICE_CHECK_1)'
                     ' and (ATR10_PERC > @ATR_10_PERC_FLOOR_SYS2)'
                     ' and (RSI3 > @RSI_3D_FLOOR_SYS_2)'
                     ' and ((Close - PrevClose) > 0)'
                     ' and ((PrevClose - Close_MIN2) > 0)'
                     )
        elif self.sys_id == 3:
            query = ('(AVG_VOL_50 > @AVG_VOL_50D)'
                     ' and (Close > @SYS_PRICE_CHECK_2)'
                     ' and (ATR10_PERC > @ATR_10_PERC_FLOOR_SYS3)'
                     ' and (Close > MA_150D)'
                     ' and (change_3D < @THREE_DAY_PERC_CHANGE_SYS_3_CAP)'
                     )

        return query

    def __init__(self, sys_id, eod_data, ib):

        self.sys_id = sys_id
        self.eod_data = eod_data
        self._set_ranking_attributes()
        self.ib = ib

        self.pandas_query = self._set_pandas_query()

    def filter_eod_data(self):

        filtered_data = None

        sort_ascending = False
        if self.ranking_order == 'Asc':
            sort_ascending = True

        if self.pandas_query:
            filtered_data = self.eod_data.query(
                self.pandas_query).sort_values(
                by=self.ranking,
                ascending=sort_ascending).head(10)[['TICKER', 'ATR10',
                                                    'ATR20', 'Close', self.ranking]]

        return filtered_data

    def _update_inportfolio(self, row, tickers=[]):
        if row['TICKER'] in tickers:
            return True
        else:
            return False

    def check_in_portfolio(self, ticker_set, portfolio):
        ts = ticker_set
        ts['InPortfolio'] = False
        portfolio_tickers = ['CBAT']
        ts['InPortfolio'] = ts.apply(
            self._update_inportfolio, tickers=portfolio_tickers, axis=1)
        # ts.loc[ts['TICKER'] in portfolio_tickers, 'InPortfolio'] = True
        print(ticker_set)

    def _calc_position_size(self, ticker, system_id, price, atr, account_value, eur_usd):

        system_budgt = 0
        max_risk_loss = 0
        pos_risk_size = 0
        pos_max_exposure = 0

        account_value_eur = account_value * eur_usd

        if system_id == 1:
            system_budget = account_value_eur * SYS_1_BUDGET
            max_risk_loss = account_value_eur * SYS_1_RISK_PERC
            pos_risk_size = max_risk_loss / (atr * SYS_1_ATR_FACTOR)
            pos_max_exposure = system_budget * SYS_1_MAX_EXPOSURE / price
        elif system_id == 2:
            system_budget = account_value_eur * SYS_2_BUDGET
            max_risk_loss = account_value_eur * SYS_2_RISK_PERC
            pos_risk_size = max_risk_loss / (atr * SYS_2_ATR_FACTOR)
            pos_max_exposure = system_budget * SYS_2_MAX_EXPOSURE / price
        elif system_id == 3:
            system_budget = account_value_eur * SYS_3_BUDGET
            max_risk_loss = account_value_eur * SYS_3_RISK_PERC
            pos_risk_size = max_risk_loss / (atr * SYS_3_ATR_FACTOR)
            pos_max_exposure = system_budget * SYS_3_MAX_EXPOSURE / price

        pos_size = min(pos_max_exposure, pos_risk_size)
        return pos_size

    def placeOrder(self, ticker_list, account_value=36800, eur_usd=1.23):

        for row_idx, row in ticker_list.query('IN_PORT == False').iterrows():
            ticker_symbol = row['TICKER']
            price = row['Close']
            seq_nr = row['SEQ_NR']

            sys_id = row['SYS_ID']
            if sys_id == 1:
                atr = row[SYS_1_ATR]
            elif sys_id == 2:
                atr = row[SYS_2_ATR]
            elif sys_id == 3:
                atr = row[SYS_2_ATR]

            order_ref = str(sys_id) + '-' + \
                datetime.datetime.today().strftime('%Y%m%d') + "-" + str(seq_nr)

            totalQuantity = self._calc_position_size(
                ticker_symbol, sys_id, price, atr, account_value, eur_usd)

            contract = Stock(ticker_symbol, 'SMART', 'USD')
            self.ib.qualifyContracts(contract)

            order_action = 'BUY' if (sys_id == 1 or sys_id == 3) else 'SELL'
            if sys_id == 1:
                order = MarketOrder(order_action, math.floor(totalQuantity),
                                    orderRef=order_ref)
                trade = self.ib.placeOrder(contract, order)
                self.ib.sleep(1)
            elif (sys_id == 2 or sys_id == 3):
                limit_price = price
                if sys_id == 2:
                    limit_price = price * (1 + SYS_2_LMT_PRICE_PERC)
                elif sys_id == 3:
                    limit_price = price * (1 - SYS_3_LMT_PRICE_PERC)

                order = LimitOrder(order_action,
                                   totalQuantity=math.floor(totalQuantity),
                                   lmtPrice=round(limit_price, 2),
                                   orderRef=order_ref)
                trade = self.ib.placeOrder(contract, order)
                self.ib.sleep(1)

        """
        try:
            contract = Stock(ticker, 'SMART', 'USD')
            self.ib.qualifyContracts(contract)

            order = LimitOrder('BUY', 10, 1500, orderRef='1.254')
            trade = self.ib.placeOrder(contract, order)

            self.ib.sleep(2)
            self.ib.cancelOrder(order)

            self.ib.sleep(2)
            print(trade.log)

        except Exception as exp:
            print(exp)
        """

    def check_shortability(self, ticker_list):
        """
        https://ib-insync.readthedocs.io/api.html?highlight=reqMktData#ib_insync.ib.IB.reqMktData
        """

        temp_list = ticker_list.set_index('TICKER', append=True)

        for row_idx, row in temp_list.iterrows():
            date = row_idx[0]
            ticker_symbol = row_idx[1]
            log.info('Getting numer of shortable share for ' + ticker_symbol)

            try:
                contract = Stock(ticker_symbol, 'SMART', 'USD')
                self.ib.qualifyContracts(contract)
                ticker = self.ib.reqMktData(contract, '236')
                self.ib.sleep(2)
                nr_shortable_shares = ticker.shortableShares
                if nr_shortable_shares:
                    temp_list.loc[row_idx, ['SHORTABLE']] = nr_shortable_shares
            except Exception as exp:
                log.error(exp)

        return temp_list.reset_index(level=1)


class TIBDB:

    tib_engine = None

    def __init__(self):
        self.tib_engine = create_engine(TIB_DB, echo=False)

    def check_portfolio_for_ticker(self, ticker_list):

        system_id = ticker_list.SYS_ID.min()
        ticker_list['IN_PORT'] = False

        strSQL = """
            SELECT
                TICKER
                , STATUS
                , SYS_ID
            FROM POSITION
            WHERE
                STATUS = 'open'
                AND SYS_ID = {0};
        """.format(system_id)

        positions_info = None

        try:
            df_positions = pd.read_sql_query(
                strSQL,
                self.tib_engine,
                parse_dates={"DATE": "%Y-%m-%d %H:%M:%S"})
            if len(df_positions) > 0:
                positions_info = pd.merge(
                    ticker_list, df_positions[['TICKER', 'STATUS']],
                    on='TICKER', how='left')
                positions_info['IN_PORT'] = positions_info['STATUS'].apply(
                    lambda x: True if x == 'open' else False)
                positions_info.drop('STATUS', axis=1, inplace=True)
            else:
                return ticker_list

        except Exception as exp:
            log.error("Error reading positions from database: ", exp)

        return positions_info

    def save_trades_to_db(self, *trades):
        """
        save trades to db
        """

        try:
            for trade_set in trades:
                trade_set.to_sql('TRADES', con=self.tib_engine,
                                 if_exists='append')
        except Exception as exp:
            log.error('Error saving trades to database:', exp)

    def update_closed_positions(self, trades):

        sql = """
            UPDATE TRADES
            SET TIB_STATUS = 'CLOSED'
            WHERE ORDER_REF IN ({0})
            """.format(','.join(["'" + order_ref[:len(order_ref)-2] + "'" for
                                 order_ref in trades.ORDER_REF]))

        try:
            self.tib_engine.execute(sql)
        except Exception as exp:
            log.error('Error update open positions to closed: ', exp)
