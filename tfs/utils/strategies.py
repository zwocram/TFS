import numpy as np
import pandas as pd
import math
import time
import decimal
import numpy as np
import pandas as pd
import pdb

from ibapi.contract import Contract


class Strategy(object):
    pass


class Unit(object):

    def __init__(self, capital, atr,
                 account_risk=0.02, unit_stop=2,
                 first_unit_stop=1, nr_equities=6,
                 nr_units=4):

        self.capital = capital
        self.atr = atr
        self.account_risk = account_risk
        self.unit_stop = unit_stop
        self.first_unit_stop = first_unit_stop
        self.nr_equities = nr_equities
        self.nr_units = nr_units

    def calc_position_size(self, price):
        self.pos_size = (self.capital / (self.nr_equities * self.nr_units)) \
            / price

        return math.floor(self.pos_size)

    def calc_stop_level(self, atr, entry_price, position_type,
                        first_unit=False):
        self.stop_level = None
        if first_unit:
            if position_type == "long":
                self.stop_level = entry_price - atr
            elif position_type == "short":
                self.stop_level = entry_price + atr
        else:
            if position_type == "long":
                self.stop_level = entry_price - 2 * atr
            elif position_type == "short":
                self.stop_level = entry_price + 2 * atr

        return float("{0:.2f}".format(self.stop_level))


class TFS(Strategy):
    def eod_data(self, instrument_list, ib=None, config=None,
                 account_size=0):
        app = ib

        atr_horizon = int(config['tfs']['atr_horizon'])
        entry_breakout_periods = int(config['tfs']['entry_breakout_periods'])
        exit_breakout_periods = int(config['tfs']['exit_breakout_periods'])
        account_risk = decimal.Decimal(config['tfs']['account_risk'])
        unit_stop = int(config['tfs']['unit_stop'])
        first_unit_stop = int(config['tfs']['first_unit_stop'])
        nr_equities = int(config['tfs']['nr_equities'])
        nr_units = int(config['tfs']['nr_units'])

        eod_df = pd.DataFrame()

        for p in instrument_list:
            ticker = p[0].upper()
            exchange = p[1].split(',')[1].lstrip()
            sec_type = p[1].split(',')[2].lstrip()

            ibcontract = Contract()
            ibcontract.secType = sec_type
            ibcontract.symbol = ticker
            ibcontract.exchange = exchange
            ibcontract.currency = 'USD'
            print('processing ', ticker)

            resolved_ibcontract = app.resolve_ib_contract(ibcontract)

            historic_data = app.get_IB_historical_data(resolved_ibcontract)

            time.sleep(5)
            if historic_data is not None:
                eod_data = {}
                df = pd.DataFrame(historic_data,
                                  columns=['date', 'open', 'high',
                                           'low', 'close', 'volume'])
                eod_data['date'] = df.iloc[-1, df.columns.get_loc('date')]

                df = df.set_index('date')

                eod_data['ticker'] = ticker
                eod_data['atr'] = self._calculate_atr(atr_horizon, df)
                eod_data['55DayHigh'] = self._calc_nday_high(
                    entry_breakout_periods, df)
                eod_data['55DayLow'] = self._calc_nday_low(
                    entry_breakout_periods, df)
                eod_data['20DayHigh'] = self._calc_nday_high(
                    exit_breakout_periods, df)
                eod_data['20DayLow'] = self._calc_nday_low(
                    exit_breakout_periods, df)
                eod_data['open'] = self._calc_today_open(df)
                eod_data['high'] = self._calc_today_high(df)
                eod_data['low'] = self._calc_today_low(df)
                eod_data['close'] = self._calc_today_close(df)

                capital = account_size
                unit = Unit(capital, eod_data['atr'],
                            account_risk=account_risk, unit_stop=unit_stop,
                            first_unit_stop=first_unit_stop,
                            nr_equities=nr_equities, nr_units=nr_units)

                nday_high_price = eod_data['55DayHigh']
                nday_low_price = eod_data['55DayLow']
                eod_data['position_size'] = \
                    unit.calc_position_size(self._calc_today_close(df))
                eod_data['stp_long'] = \
                    unit.calc_stop_level(eod_data['atr'],
                                         nday_high_price, "long",
                                         first_unit=False)
                eod_data['stp_short'] = \
                    unit.calc_stop_level(eod_data['atr'],
                                         nday_low_price, "short",
                                         first_unit=False)

                df_temp = pd.DataFrame(eod_data, index=[ticker])
                eod_df = eod_df.append(df_temp)
                # print(eod_data, position_size, stop_level)
                # print(df)

            app.init_error()

        return eod_df

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

        return float("{0:.4f}".format(atr))

    def _calc_nday_high(self, period, df):
        """Calculate N day high excluding today prices"""
        nday_high = df[:-1][-period:].high.max()
        return float("{0:.2f}".format(nday_high))

    def _calc_nday_low(self, period, df):
        """Calculate N day low excluding today prices"""
        nday_low = df[:-1][-period:].low.min()
        return float("{0:.2f}".format(nday_low))

    def _calc_today_open(self, df):
        """Calculate today's open price."""
        today_open = df[-1:].open.min()
        return float("{0:.2f}".format(today_open))

    def _calc_today_high(self, df):
        """Calculate today's high price."""
        today_high = df[-1:].high.min()
        return float("{0:.2f}".format(today_high))

    def _calc_today_low(self, df):
        """Calculate today's low price."""
        today_low = df[-1:].low.min()
        return float("{0:.2f}".format(today_low))

    def _calc_today_close(self, df):
        """Calculate today's close price."""
        today_close = df[-1:].close.min()
        return float("{0:.2f}".format(today_close))
