import numpy as np
import pandas as pd
import time
import decimal
import numpy as np
import pandas as pd
import pdb

from utils.unit import Unit

from utils.driver import Driver
from db.database import Database
from ibapi.contract import Contract


class Strategy(object):
    def __init__(self):
        self.driver = Driver()


class TFS(Strategy):
    def eod_data(self, ib=None, portfolio_list=None, tfs_settings=None,
                 account_size=0):
        """Retrieves market data at end of day.

        :param portfolio: portfolio list as in settings.cfg
        :param ib

        """
        db = Database()

        app = ib
        tfs_settings = self.driver.get_tfs_settings(tfs_settings)

        eod_df = pd.DataFrame()

        for p in portfolio_list:
            if not db.exists_instrument(p):
                result = db.insert_new_instrument(p)
            historic_data = self.driver.get_historical_data(ib, p)

            if historic_data is not None:
                eod_data = {}
                df = pd.DataFrame(historic_data,
                                  columns=['date', 'open', 'high',
                                           'low', 'close', 'volume'])
                eod_data['date'] = df.iloc[-1, df.columns.get_loc('date')]

                df = df.set_index('date')

                # ret_values = self._invert_exchange_rate(df, identifier)
                # identifier = ret_values[0]
                # df = ret_values[1]

                eod_data = self._enrich_eod(p[0].upper(), eod_data, df,
                                            tfs_settings, account_size)

                df_temp = pd.DataFrame(eod_data, index=[p[0].upper()])
                eod_df = eod_df.append(df_temp)

        return eod_df

    def _invert_exchange_rate(self, df, ticker):
        """invert prices of exchange rates, e.g.
        from USDJPY to JPYUSD

        :param df: dataframe for which values have to inverted
        :param ticker: ticker whose values have to be inverted

        :return: inverted tickername, inverted values
        """
        to_invert = ['USDJPY', 'EURGBP', 'EURCHF', 'EURHKD', 'EURJPY',
                     'EURAUD', 'EURCAD', 'EURCNH', 'EURCZK', 'EURDKK',
                     'EURHUF', 'EURILS', 'EURMXN', 'EURNOK', 'EURNZD',
                     'EURPLN', 'EURRUB', 'EURSEK', 'EURSGD', 'EURUSD',
                     'EURZAR']

        df_temp = df
        if ticker in to_invert:
            ticker = ticker[3:6] + ticker[0:3]
            df_temp['open'] = 1 / df_temp['open']
            df_temp['high'] = 1 / df_temp['high']
            df_temp['low'] = 1 / df_temp['low']
            df_temp['close'] = 1 / df_temp['close']

        return ticker, df_temp

    def _calculate_atr(self, period, df):
        df_temp = df

        df_temp['close_prev'] = df_temp.close.shift(1)
        df_temp['TR1'] = df_temp.high - df_temp.low
        df_temp.TR1 = df_temp.TR1.abs()
        df_temp['TR2'] = df_temp.close_prev - df_temp.high
        df_temp.TR2 = df_temp.TR2.abs()
        df_temp['TR3'] = df_temp.close_prev - df_temp.low
        df_temp.TR3 = df_temp.TR3.abs()
        df_temp['TR'] = df_temp[['TR1', 'TR2', 'TR3']].max(axis=1)
        df_temp.iloc[0, df_temp.columns.get_loc('TR')] = np.nan
        df_temp['ATR_current'] = df_temp.TR.rolling(window=period).mean()
        df_temp['ATR_prev'] = df_temp.ATR_current.shift(1)
        df_temp['ATR'] = (df_temp.ATR_current / period) \
            + df_temp.ATR_prev * ((period - 1) / period)

        atr = df_temp.iloc[-1, df_temp.columns.get_loc('ATR')]

        return float("{0:.6f}".format(atr))

    def _enrich_eod(self, identifier, eod_data, df, tfs_params, capital):
        """Enriches the eod dataset that was received from IB

        :param eod_data: the dataset that has to enriched

        :return: an enriched dataset.
        """

        eod_data['ticker'] = identifier
        eod_data['atr'] = self._calculate_atr(tfs_params['atr_horizon'],
                                              df)
        eod_data['55DayHigh'] = self._calc_nday_high(
            tfs_params['entry_breakout_periods'], df)
        eod_data['55DayLow'] = self._calc_nday_low(
            tfs_params['entry_breakout_periods'], df)
        eod_data['20DayHigh'] = self._calc_nday_high(
            tfs_params['exit_breakout_periods'], df)
        eod_data['20DayLow'] = self._calc_nday_low(
            tfs_params['exit_breakout_periods'], df)
        eod_data['open'] = self._calc_today_open(df)
        eod_data['high'] = self._calc_today_high(df)
        eod_data['low'] = self._calc_today_low(df)
        eod_data['close'] = self._calc_today_close(df)

        unit = Unit(account_size=capital, atr=eod_data['atr'],
                    account_risk=tfs_params['account_risk'],
                    unit_stop=tfs_params['unit_stop'],
                    first_unit_stop=tfs_params['first_unit_stop'],
                    nr_equities=tfs_params['nr_equities'],
                    nr_units=tfs_params['nr_units'],
                    ticker=identifier, price=self._calc_today_close(df))

        eod_data['pos_size (1st)'] = \
            unit.calc_position_size_risk_perc(first_unit=True)
        eod_data['pos_size (other)'] = \
            unit.calc_position_size_risk_perc(first_unit=False)

        return eod_data

    def _calc_nday_high(self, period, df):
        """Calculate N day high excluding today prices"""
        nday_high = df[-period:].high.max()
        return float("{0:.4f}".format(nday_high))

    def _calc_nday_low(self, period, df):
        """Calculate N day low excluding today prices"""
        nday_low = df[-period:].low.min()
        return float("{0:.4f}".format(nday_low))

    def _calc_today_open(self, df):
        """Calculate today's open price."""
        today_open = df[-1:].open.min()
        return float("{0:.4f}".format(today_open))

    def _calc_today_high(self, df):
        """Calculate today's high price."""
        today_high = df[-1:].high.min()
        return float("{0:.4f}".format(today_high))

    def _calc_today_low(self, df):
        """Calculate today's low price."""
        today_low = df[-1:].low.min()
        return float("{0:.4f}".format(today_low))

    def _calc_today_close(self, df):
        """Calculate today's close price."""
        today_close = df[-1:].close.min()
        return float("{0:.4f}".format(today_close))

    def _set_price_formatting(self, security_type):
        """Set formatting of prices.
        """
        if security_type == "CASH":
            formatting = "{0:.4f}"
        else:
            formatting = "{0:.2f}"

        return formatting
