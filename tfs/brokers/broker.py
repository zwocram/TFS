from ib_insync import *
import quandl
from abc import ABC, abstractmethod

import pandas as pd
import numpy as np
import datetime

import pdb


class NoValidStartDate(Exception):
    pass


class DateUtils(object):
    """
    """

    def prev_months(self, ref_date, months=1):
        if months > 12:
            return

        current_month = ref_date.month
        if current_month <= months:
            start_month = months + 2
            start_year = ref_date.year - 1
        else:
            start_month = current_month - months
            start_year = ref_date.year

        hist_month = datetime.date(
            start_year,
            start_month,
            1
        )
        return hist_month


class Broker(ABC):

    @abstractmethod
    def get_historical_data(self):
        pass

    def calc_returns(self, df):
        """
        calculate returns for columns in DataFrame
        """

        for c in df.columns:
            df[c] = df[c] / df[c].shift(-1) - 1

        return df

    def get_close_prices(self, time_series, to_keep, names):
        """
        Strip all columns from historical data, except
        the to_keep column

        :param time_series: list of dataframes with hist_data
        :param to_keep: the column to keep
        """

        def _delete_rest(df, to_keep):
            """
            Do some filtering on passed dataframe
            and delete all columns except to_keep
            """

            if 0 in df.values:
                df = df.replace(0, np.nan)
            df = df.fillna(method='ffill')
            df.columns = df.columns.str.lower()
            if 'settle' in df.columns:
                df = df.rename(columns={'settle': 'close'})

            to_keep = [to_keep]
            to_delete = [c for c in df.columns if c not in to_keep]

            df = df.drop(columns=to_delete)

            return df

        series = [_delete_rest(df, to_keep) for df in time_series]
        series = pd.concat(series, axis=1)
        series = series.fillna(method='ffill')
        series.columns = names

        return series


class IBBroker(Broker):
    """my own little IB implementation"""

    def __init__(self):
        self.ib = IB()

    def _connect(self):
        self.ib.connect("127.0.0.1", 4011, clientId=1)

    def _disconnect(self):
        self.ib.disconnect()

    def _det_contract(self, contract_info):
        """determines what kind of contract has to be requested"""

        secType = contract_info[1].split(',')[2].replace(' ', '')
        symbol = contract_info[0].upper()
        exchange = contract_info[1].split(',')[1].replace(' ', '')
        currency = contract_info[1].split(',')[3].replace(' ', '')
        curr_pair = contract_info[1].split(',')[0].replace(' ', '')

        if secType == "CASH":  # forex
            return Forex(curr_pair)
        elif secType == "STK":  # Stock/etf
            return Stock(
                symbol,
                exchange,
                currency
            )
        elif secType == "CONTFUT":
            return ContFuture(
                symbol=symbol, exchange=exchange,
                currency=currency
            )

    def get_historical_data(self, section, *args):

        self._connect()
        contract_defs = [self._det_contract(s) for s in section.items()]

        names = [s[0].upper() for s in section.items()]
        contract_details = [self.ib.reqContractDetails(c)
                            for c in contract_defs]
        contracts = [d[0].contract for d in contract_details]

        time_series = [self.ib.reqHistoricalData(
            contract,
            endDateTime='',
            durationStr='240 D',
            barSizeSetting='1 day',
            whatToShow='MIDPOINT' if contract.secType == 'CASH' else 'TRADES',
            useRTH=True) for contract in contracts]

        self._disconnect()

        try:
            series = [util.df(bars).set_index('date')
                      for bars in time_series]
            #series_df = pd.concat(series, axis=1)
            #series_df.columns = names
        except Exception as e:
            print("Error converting IB time series to dataframes."
                  " Check the logs, there might be a permission error for a"
                  " particular instrument.")

        return series


class QuandlBroker(Broker):

    api_key = "xyfvxwSv8i1F5gyerDt_"
    curr_collection = pd.DataFrame()

    def _get_hist_time_series(self,
                              instr_id,
                              start_date,
                              end_date,
                              transform="rdiff"):

        quandl.ApiConfig.api_key = self.api_key
        date_format = "%Y-%m-%d"
        data = quandl.get(
            instr_id,
            start_date=start_date.strftime(date_format),
            end_date=end_date.strftime(date_format),
            transform=transform
        )

        return data

    def _convert_series(self, currency, result,
                        start_date, end_date):
        """
        """
        curr_id = currency.split('/')[1]
        if curr_id in self.curr_collection.columns:
            result = result.div(
                self.curr_collection.loc[:, curr_id])
        else:
            conversion_rate = self._get_hist_time_series(
                currency, start_date, end_date, transform="")
            self.curr_collection.insert(0, curr_id,
                                        conversion_rate)
            result = result.divide(conversion_rate)

        return result.sub(result.shift(1)).divide(result)

    def _get_data(self, instrument_ids=[],
                  column_names=[],
                  currencies=None,
                  start_date=None,
                  end_date=None,
                  fx_conv=False,
                  rdiff=False):

        partial_data = []
        for code in instrument_ids:
            if fx_conv:
                result = self._get_hist_time_series(
                    code, start_date, end_date, transform="")
                currency = currencies[instrument_ids.index(code)]
                if len(set(currencies)) > 0:
                    if currency != "EUR":
                        result = self._convert_series(
                            currency, result, start_date, end_date)
            else:
                result = self._get_hist_time_series(
                    code, start_date, end_date,
                    transform="rdiff" if rdiff else "")

            partial_data.append(result)

        all_data = pd.concat(partial_data, axis=1)
        if len(column_names) > 0:
            all_data.columns = column_names

        return all_data

    def get_historical_data(self, section, fx_conv=False, rdiff=False):

        q_tickers = [s[0].upper() for s in section.items()]
        col_names = [s[1].split(',')[0].strip() for s in section.items()]
        currencies = [s[1].split(',')[1].strip() for s in section.items()]

        end_date = datetime.datetime.now().date()
        start_date = end_date + datetime.timedelta(days=-240)

        series = [self._get_hist_time_series(ticker,
                                             start_date,
                                             end_date,
                                             transform="")
                  for ticker in q_tickers]

        """
        all_data = self._get_data(
            q_t, col_names, currencies,
            start_date, end_date, fx_conv=fx_conv, rdiff=rdiff)
        """

        return series
