import itertools
import math
import networkx as nx
import numpy as np

# new comment
import configparser
import pdb
import sys
import logging
import decimal
from optparse import OptionParser
import pandas as pd
# from datetime import datetime, time
import datetime
import time
from utils import futures

from utils.strategies import TFS, Unit
from utils.correlations import Correlations
from db.database import Database

from ib import ib
from ibapi.contract import Contract

MAX_DAYS_HISTORY = '150 D'
NR_LEAST_CORRELATED_ITEMS = 3

if __name__ == '__main__':

    # parse arguments
    parser = OptionParser()
    parser.add_option("-i", "--import_correlations", dest="import_corr",
                      help="File that contains correlations")
    parser.add_option("-g", "--correlation_group", dest="corr_group",
                      help="Group in settings file that contains the "
                      " instruments.")
    (options, args) = parser.parse_args()
    import_corr = options.import_corr
    corr_group = options.corr_group

    corr_util = Correlations()

    if import_corr is not None:
        df_corr = pd.read_csv(import_corr, index_col=['Unnamed: 0'],
                              na_values=[" ", "  ", " ", ""])
        df_corr = corr_util.fill_upper_triangle(df_corr)
        df_corr = df_corr.abs()
        # sys.exit()
        # df_corr = df.drop(columns=['Unnamed: 0'])
        nr_columns = df_corr.shape[1]
        # df_corr = df_corr.corr().abs()
    else:
        try:
            app = ib.IB("127.0.0.1", 4011, 10)
        except AttributeError as exp:
            print("Could not connect to the TWS API application.")
            sys.exit()

        # set up logging
        logging.basicConfig(format='%(levelname)s %(asctime)s: %(message)s',
                            filename='logs/log.log',
                            filemode='w', level=logging.INFO)
        logging.warning('login created')

        # read config file
        config = configparser.ConfigParser()
        config.read('config/settings.cfg')

        if corr_group is not None:
            instrument_list = config.items(corr_group)
        else:
            instrument_list = config.items('correlation_set')

        corr_df = pd.DataFrame()
        first_instrument = False
        nr_instruments = len(instrument_list)

        for p in instrument_list:
            print(p)
            identifier = p[0].upper()

            exchange = p[1].split(',')[1].lstrip()
            sec_type = p[1].split(',')[2].lstrip()
            currency = p[1].split(',')[3].lstrip().upper()
            ticker = p[1].split(',')[4].lstrip().upper()

            ibcontract = Contract()
            ibcontract.secType = sec_type
            ibcontract.symbol = ticker
            ibcontract.exchange = exchange
            ibcontract.currency = currency
            print('processing', identifier)

            resolved_ibcontract = app.resolve_ib_contract(ibcontract)
            historic_data = app.get_IB_historical_data(
                resolved_ibcontract,
                duration=MAX_DAYS_HISTORY)

            if historic_data is not None:
                df = pd.DataFrame(historic_data,
                                  columns=['date', 'open', 'high',
                                           'low', identifier, 'volume'])
                df = df.set_index('date')
                df = df.drop(columns=['open', 'high', 'low', 'volume'])
                df = corr_util.calc_returns(df, identifier)
                if first_instrument is False:
                    first_instrument = True
                    corr_df = df
                else:
                    corr_df = corr_df.join(df)
                # print(corr_df)

        df_corr = corr_df.corr().abs()

    df_corr_clean = corr_util.clean_correlation_matrix(df_corr)
    nr_columns = df_corr_clean.shape[1]
    print('The cleaned correlation matrix:\n')
    print(df_corr_clean)
    print('The correlation matrix:\n')
    print(df_corr)
    print('The actual time series:\n')
    print(corr_df)
    input('Above you see the cleaned and raw correlation matrix of'
          ' the requested instruments. Press enter to continue or'
          ' CTRL+C to quit.')

    # Create new graph
    G = nx.Graph()

    # Each node represents a dimension
    node_range = range(0, nr_columns, 1)
    G.add_nodes_from(node_range)

    print('creating nodes and their dependencies...')
    for n in node_range:
        sub_range = node_range[n + 1:]
        item_list = []
        for s in sub_range:
            # print(n, s, df_corr.iloc[n, s])
            node_connection = (n, s, df_corr_clean.iloc[n, s])
            item_list.append(node_connection)
        # print(item_list)
        G.add_weighted_edges_from(item_list)

    nodes = set(G.nodes())
    t1 = datetime.datetime.now()
    print('calculating all combinations')

    if NR_LEAST_CORRELATED_ITEMS > 0:
        nr_least_correlated_items = NR_LEAST_CORRELATED_ITEMS
    else:
        nr_least_correlated_items = math.floor(nr_columns / 3.)
        if nr_least_correlated_items == 0:
            nr_least_correlated_items += 1

    combs = set(itertools.combinations(nodes, nr_least_correlated_items))
    t2 = datetime.datetime.now()
    print('Found {0} combinations in {1} seconds.'.format(len(combs),
                                                          (t2 - t1).seconds))
    sumList = []

    print('analyzing all combinations...')
    t3 = datetime.datetime.now()
    for comb in combs:
        S = G.subgraph(list(comb))
        sum = 0
        for edge in S.edges(data=True):
            sum += edge[2]['weight']
        sumList.append((sum, comb))
    t4 = datetime.datetime.now()
    print('Analyzed all combinations in {0}'.format((t4 - t3).seconds))

    sorted = sorted(sumList, key=lambda tup: tup[0])

    print('the least correlated items:\n')
    best_node = list(sorted[0][1])
    print(df_corr_clean.iloc[best_node, best_node])

    for i in range(1, 10):
        totalWeight = sorted[i][0]
        nodes = list(sorted[i][1])
        nodes.sort()
        out = str(i) + ": " + str(totalWeight) + "," + str(nodes)
        print(out)

    S = G.subgraph(range(0, nr_least_correlated_items, 1))
    sum = 0
    for edge in S.edges(data=True):
        sum += edge[2]['weight']

    sys.exit()
