# https://github.com/erdewit/ib_insync
from ib_insync import *
import pdb
import copy
import configparser
import pandas as pd
import logging


def start():

    ib = IB()
    ib.connect("127.0.0.1", 4011, clientId=1)

    logger = logging.getLogger()
    logger.info("starting")

    # read config file
    config = configparser.ConfigParser()
    config.read('config/settings.cfg')

    section = [i[1] for i in config.items() if i[0] == 'futures_list'][0]

    contract_defs = [
        Contract(exchange=s[1].split(',')[1].replace(' ', ''),
                 secType=s[1].split(',')[2].replace(' ', ''),
                 symbol=s[0].upper()) for s in section.items()]
    names = [s[0].upper() for s in section.items()]

    # for each future 5 series are returned
    # so contract_details is a list of this series of 5
    contract_details = [ib.reqContractDetails(C)
                        for C in contract_defs]

    # start requesting market data
    ib.client.reqMarketDataType(3)
    data = [[ib.reqMktData(c.contract, '', True, False, []) for c in cd]
            for cd in contract_details]

    ib.sleep(5)
    ib.disconnect()
    pdb.set_trace()

    print(data)


if __name__ == "__main__":
    start()
