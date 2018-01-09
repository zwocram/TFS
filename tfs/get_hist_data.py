import configparser
import pdb
from datetime import datetime
from utils import futures

from ib import ib
from ibapi.contract import Contract

if __name__ == '__main__':
    ##
    # Check that the port is the same as on the Gateway
    # ipaddress is 127.0.0.1 if one same machine, clientid is arbitrary

    # read config file
    config = configparser.ConfigParser()
    config.read('config/settings.cfg')

    futures_list = {}
    for f in config.items('futures_list'):
        futures_list[f[0].upper()] = f[1]

    print(futures_list)
    futures_utils = futures.FuturesUtils()
    expiration_dates = futures_utils.resolve_expiration_month_codes()
    expiration_month_codes = expiration_dates[0]
    expiration_months = expiration_dates[1]

    app = ib.IB("127.0.0.1", 4011, 10)

    current_time = app.get_time()

    for future in futures_list:
        future_meta_data = futures_list[future].split(',')
        description_future = future_meta_data[0]
        exchange_future = future_meta_data[1].lstrip()
        security_type = future_meta_data[2].lstrip()
        for month in expiration_months:
            ibcontract = Contract()
            ibcontract.secType = security_type
            ibcontract.symbol = future.upper()
            ibcontract.exchange = exchange_future

            ibcontract.lastTradeDateOrContractMonth = month

            resolved_ibcontract = app.resolve_ib_contract(ibcontract)
            print(resolved_ibcontract)

            historic_data = app.get_IB_historical_data(resolved_ibcontract)

            print(historic_data)
