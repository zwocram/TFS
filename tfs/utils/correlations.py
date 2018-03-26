import pdb


class Correlations(object):
    def clean_correlation_matrix(self, df, threshold=0.6):
        """Remove correlations that are less than threshold.

        ::
        ::
        """
        to_delete = []
        df_size = df.shape
        size_range = range(0, df_size[0], 1)
        for col in size_range:
            if col not in to_delete:
                for row in range(col + 1, df_size[0], 1):
                    if df.iloc[row, col] > threshold:
                        if row not in to_delete:
                            to_delete.append(row)

        df = df.drop(df.index[to_delete])
        df = df.drop(df.columns[to_delete], axis=1)

        return df

    def fill_upper_triangle(self, df):
        """fills the upper triangle of a matrix.
        """

        df_size = df.shape
        size_range = range(0, df_size[0], 1)
        for col in size_range:
            for row in range(col + 1, df_size[0], 1):
                df.iloc[col, row] = df.iloc[row, col]

        return df

    def calc_returns(self, df, column_name):
        """Calculates the return of a given time series.

        :param df:  dataframe which contains the column whose returns
                    have to be calculated
        :param column_name: the column for which the returns have to be
                            calculated
        """

        df_temp = df
        df_temp['temp'] = df_temp[column_name].shift(1)
        df_temp[column_name] = (df_temp[column_name] - df_temp['temp']) / \
            df_temp[column_name]
        df_temp = df_temp.drop(columns=['temp'])

        return df_temp
