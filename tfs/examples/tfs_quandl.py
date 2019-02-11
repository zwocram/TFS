import quandl
import datetime
import pandas as pd
import configparser
import random
import numpy as np
from optparse import OptionParser
from scipy.optimize import LinearConstraint
from scipy.optimize import Bounds
import pdb
import logging
import sys

from scipy.optimize import SR1
from scipy.optimize import Bounds
from scipy.optimize import LinearConstraint
from scipy.optimize import minimize


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


class QuandlData(object):

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
        if 'Settle' in data.columns:
            return data['Settle']
        elif 'Close' in data.columns:
            return data['Close']

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

    def get_data(self, instrument_ids=[],
                 column_names=[],
                 currencies=None,
                 start_date=None,
                 end_date=None,
                 fx_conv=False):

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
                    code, start_date, end_date, transform="rdiff")

            partial_data.append(result)

        all_data = pd.concat(partial_data, axis=1)
        if len(column_names) > 0:
            all_data.columns = column_names

        return all_data


class CorrelationUtils(object):
    """
    https://stats.stackexchange.com/questions/110426/least-correlated-subset-of-random-variables-from-a-correlation-matrix
    https://stackoverflow.com/questions/33811240/python-randomly-fill-2d-array-with-set-number-of-1s
    """

    corr_matrix = None

    def least_correlated_sub_matrix_by_approx(self, corr_matrix, max_dimension):
        while corr_matrix.columns.size > max_dimension:
            max_column_id = (corr_matrix ** 2).sum(axis=1).idxmax()
            corr_matrix = corr_matrix.drop(max_column_id, 1)
            corr_matrix = corr_matrix.drop(max_column_id, 0)
        return corr_matrix

    def least_correlated_sub_matrix_by_simu(self, corr_matrix,
                                            max_dimension, nr_trials=50000):
        dim_matrix = corr_matrix.columns.size
        results = []
        for s in range(nr_trials):
            if s % (nr_trials / 5) == 0:
                print(s)
            vector_value = [0] * dim_matrix
            for pos in random.sample(range(dim_matrix), max_dimension):
                vector_value[pos] = 1
            mul_1 = np.matmul(np.asarray(vector_value),
                              corr_matrix.as_matrix())
            mul_2 = np.matmul(mul_1, np.asarray(vector_value).T)
            results.append((mul_2, vector_value))

        # process results
        values = [r[0] for r in results]
        min_value = min(values)
        min_value_index = values.index(min(values))
        min_value_vector = results[min_value_index][1]
        uncorr_indices = [i for i, x in enumerate(min_value_vector) if x == 1]
        submatrix = corr_matrix.iloc[uncorr_indices, uncorr_indices]

        print("minimum found: ", min_value)
        print("corresponding min vector: ", min_value_vector)

        return submatrix

    def _corr_quad(self, x):
        tmp = np.matmul(x, self.corr_matrix)

        return np.matmul(tmp, x)

    def least_correlated_sub_matrix_by_optimization(
            self, corr_matrix,
            max_dimension):

        self.corr_matrix = corr_matrix
        nr_vars = corr_matrix.columns.size

        A_mat = np.array([[1] * nr_vars])
        b_vec = np.array([max_dimension])

        linear_constraint = LinearConstraint(A_mat, b_vec, b_vec)
        bounds = Bounds([0] * nr_vars, [1] * nr_vars)

        x0 = np.array([1] * nr_vars)

        res = minimize(self._corr_quad, x0, method='trust-constr',
                       jac="2-point", hess=SR1(),
                       constraints=[linear_constraint],
                       options={'verbose': 1}, bounds=bounds)

        x_res = res['x']
        x_res_sorted = np.sort(x_res)
        x_comp = (x_res >= x_res_sorted[nr_vars - max_dimension])
        x_hits = np.where(x_comp == True)
        index_hits = x_hits[0].tolist()

        return corr_matrix.iloc[index_hits, index_hits]


def start():
    # parse options
    parser = OptionParser()
    parser.add_option("-g", "--correlation_group", dest="corr_group",
                      help="Group in settings file that contains the "
                      " instruments.")
    parser.add_option("-d", "--dimension", dest="corr_dimension",
                      help="Sets the dimension of the correlation "
                      " submatrix.")
    parser.add_option("-f", "--fx_conv", action="store_true", default=False,
                      dest="fx_conv", help="Indicates whether to convert"
                      " time series to native currency.")
    (options, args) = parser.parse_args()
    group = options.corr_group
    fx_conv = options.fx_conv
    max_correlated_items = int(options.corr_dimension)
    if group is None or max_correlated_items is None:
        print("Provide a settings file group and/or the dimension "
              "of the subcorrelation matrix.")

    all_data = pd.DataFrame()

    # read config file
    config = configparser.ConfigParser()
    config.read('config/settings.cfg')

    section = [i[1] for i in config.items() if i[0] == group][0]
    q_codes = [s[0].upper() for s in section.items()]
    col_names = [s[1].split(',')[0].strip() for s in section.items()]
    currencies = [s[1].split(',')[1].strip() for s in section.items()]

    date_utils = DateUtils()
    start_date = date_utils.prev_months(
        datetime.datetime.now().date(), months=6)
    if start_date is None:
        raise NoValidStartDate("choose 12 or less months")

    end_date = datetime.date(
        datetime.datetime.now().year,
        datetime.datetime.now().month,
        1)
    qd = QuandlData()
    all_data = qd.get_data(q_codes, col_names, currencies,
                           start_date, end_date, fx_conv=fx_conv)
    pdb.set_trace()

    corr_utils = CorrelationUtils()
    sub_corr = corr_utils.least_correlated_sub_matrix_by_approx(
        all_data.corr().abs(), max_dimension=max_correlated_items)
    sub_corr.columns = [''] * sub_corr.columns.size
    print(sub_corr.round(decimals=4))

    sub_corr_test = corr_utils.least_correlated_sub_matrix_by_simu(
        all_data.corr().abs(),
        max_dimension=max_correlated_items,
        nr_trials=100000)
    sub_corr_test.columns = [''] * sub_corr_test.columns.size
    print(sub_corr_test.round(decimals=4))

    sub_corr_opt = corr_utils.least_correlated_sub_matrix_by_optimization(
        all_data.corr().abs(), max_dimension=max_correlated_items)
    sub_corr_opt.columns = [''] * sub_corr_opt.columns.size
    print(sub_corr_opt.round(decimals=4))


if __name__ == "__main__":
    try:
        start()
    except Exception as e:
        print("error during startup:  ", e, sys.exc_info()[0])
