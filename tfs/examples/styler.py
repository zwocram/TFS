import pandas as pd
import numpy as np
import pdb


def highlight_max(s):
    '''
    highlight the maximum in a Series yellow.
    '''
    is_max = s == s.max()
    return ['background-color: yellow' if v else '' for v in is_max]


np.random.seed(24)
df = pd.DataFrame({'A': np.linspace(1, 10, 10)})
df = pd.concat([df, pd.DataFrame(np.random.randn(10, 4), columns=list('BCDE'))],
               axis=1)
df.iloc[0, 2] = np.nan
df.style.apply(highlight_max)
print(df)


data = pd.DataFrame(np.random.randn(5, 3), columns=list('ABC'))


def highlight_cols(x):
    # copy df to new - original data are not changed
    df = x.copy()
    # select all values to default value - red color
    df.loc[:, :] = 'background-color: red'
    # overwrite values grey color
    df[['B', 'C']] = 'background-color: grey'
    # return color df
    return df


data.style.apply(highlight_cols, axis=None)
print(data)


def print_format_table():
    """
    prints table of formatted text format options
    """
    for style in range(8):
        for fg in range(30, 38):
            s1 = ''
            for bg in range(40, 48):
                format = ';'.join([str(style), str(fg), str(bg)])
                print(type(format))
                s1 += '\x1b[%sm %s \x1b[0m' % (format, format)
            print(s1)
        print('\n')


print_format_table()
