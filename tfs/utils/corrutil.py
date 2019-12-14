import datetime
import pandas as pd
import numpy as np
import random
import copy
from itertools import accumulate

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

    def least_correlated_sub_matrix_by_optimization_multidim(
            self, corr_matrix,
            section_sizes,
            max_dimension):

        self.corr_matrix = corr_matrix
        nr_vars = corr_matrix.columns.size

        A_mat_orig = np.zeros((len(section_sizes), nr_vars))
        section_items_acc = accumulate(section_sizes)
        diff_section_items = [sum(x)
                              for x in zip(section_items_acc,
                                           [-1 * s for s in section_sizes])]
        ranges = zip(diff_section_items, section_sizes)
        for ind, range in enumerate(ranges):
            A_mat_orig[ind, range[0]:range[0] + range[1]] = 1

        A_mat = A_mat_orig
        b_vec = np.array(max_dimension)

        linear_constraint = LinearConstraint(A_mat, b_vec, b_vec)
        bounds = Bounds([0] * nr_vars, [1] * nr_vars)

        x0 = np.array([1] * nr_vars)

        res = minimize(self._corr_quad, x0, method='trust-constr',
                       jac="2-point", hess=SR1(),
                       constraints=[linear_constraint],
                       options={'verbose': 1}, bounds=bounds)

        x_res = res['x']
        ranges = zip(diff_section_items, section_sizes)
        index_hits = None
        for ind, range in enumerate(ranges):

            x_res_split = x_res[range[0]:range[0] + range[1]]
            nr_vars_split = section_sizes[ind]
            max_dimension_split = max_dimension[ind]
            x_res_sorted = copy.deepcopy(x_res)
            x_res_sorted[range[0]:range[0] + range[1]] = sorted(
                x_res_sorted[range[0]:range[0] + range[1]])
            x_comp = (x_res >= x_res_sorted[range[0] +
                                            nr_vars_split -
                                            max_dimension_split])
            x_hits = np.where(x_comp == True)
            x_hits = x_hits[0][np.where((x_hits[0] >= range[0]) &
                                        (x_hits[0] < range[0] + range[1]))]

            if index_hits:
                index_hits.extend(x_hits.tolist())
            else:
                index_hits = x_hits.tolist()

        return corr_matrix.iloc[index_hits, index_hits]
