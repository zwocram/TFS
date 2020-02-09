import datetime
import pandas as pd
import numpy as np
import random
import itertools

from scipy.optimize import SR1
from scipy.optimize import Bounds
from scipy.optimize import LinearConstraint
from scipy.optimize import minimize

import pdb


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
                                            max_dimension, nr_trials=50000,
                                            corr_type="least"):
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
        min_value = min(values) if corr_type == "least" else max(values)
        min_value_index = values.index(min_value)
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

    def _create_start_end_index_markets(self, market_count):
        """
        Create start and end indices for identifying market
        members in e.g. correlation matrices.

        Based on a list of available asset classes and the
        number of markets in those asset classes we create
        indices with which we can identify those markets and
        their members in a correlation matrix.

        Parameters
        ----------
        market_count: list
            each member in the list is a tuple.
                first tuple member: asset class name
                second tuple member: #markets in asset class

        Returns
        -------
        market_index: list
            each member of the list is a 2d tuple
                1st element: start index of asset class range
                2nd element: end index of asset class range

        """

        cum_markets_count = list(
            itertools.accumulate([m[1] for m in market_count]))
        dum_1 = [x-1 for x in cum_markets_count]
        cum_markets_count.insert(0, 0)
        dum_2 = cum_markets_count.pop()
        market_index = list(zip(cum_markets_count, dum_1))

        return market_index

    def least_correlated_sub_matrix_by_optimization_grouped(
            self, corr_matrix,
            max_dimension, markets_count):

        self.corr_matrix = corr_matrix
        nr_vars = corr_matrix.columns.size

        A_mat = np.zeros((len(max_dimension), nr_vars))
        market_index = self._create_start_end_index_markets(markets_count)
        for each in enumerate(market_index):
            A_mat[each[0], each[1][0]:each[1][1]+1] = 1

        b_vec = np.array([m[1] for m in max_dimension])

        linear_constraint = LinearConstraint(A_mat, b_vec, b_vec)
        bounds = Bounds([0] * nr_vars, [1] * nr_vars)

        x0 = np.array([1] * nr_vars)

        res = minimize(self._corr_quad, x0, method='trust-constr',
                       jac="2-point", hess=SR1(),
                       constraints=[linear_constraint],
                       options={'verbose': 1}, bounds=bounds)

        hits = []
        x_res = res['x']
        for m_idx in enumerate(market_index):
            counter = m_idx[0]
            x_res_sub = x_res[m_idx[1][0]:m_idx[1][1]+1]
            x_res_sorted = np.sort(x_res_sub)
            x_comp = (x_res_sub >= x_res_sorted[markets_count[counter][1] -
                                                max_dimension[counter][1]])
            x_hits = np.where(x_comp == True)
            index_hits = [h+m_idx[1][0] for h in x_hits[0].tolist()]
            print(index_hits)
            print(corr_matrix.iloc[index_hits, index_hits])

            hits += index_hits

        return corr_matrix.iloc[hits, hits]
