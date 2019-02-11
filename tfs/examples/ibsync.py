# https://github.com/erdewit/ib_insync
from ib_insync import *
import pdb
import copy
import configparser
import pandas as pd
import logging


def create_contract(config_section):
    contract_defs = [
        Contract(exchange=s[1].split(',')[1].replace(' ', ''),
                 secType='CONT' + s[1].split(',')[2].replace(' ', ''),
                 symbol=s[0].upper()) for s in section.items()]


ib = IB()
ib.connect("127.0.0.1", 4011, clientId=1)

logger = logging.getLogger()
logger.info("starting")

pdb.set_trace()
# read config file
config = configparser.ConfigParser()
config.read('config/settings.cfg')


section = [i[1] for i in config.items() if i[0] == 'futures_list'][0]
logger.info("analyse forex section")

logger.info("analyse futures section")
contract_defs = [
    Contract(exchange=s[1].split(',')[1].replace(' ', ''),
             secType='CONT' + s[1].split(',')[2].replace(' ', ''),
             symbol=s[0].upper()) for s in section.items()]
names = [s[0].upper() for s in section.items()]
what_to_show = 'TRADES'

forex_section = [i[1] for i in config.items() if i[0] == 'currencies_eur'][0]
forex_contracts = [
    Forex(s[1].split(',')[0].replace(' ', '')) for s in forex_section.items()]
names_forex = [s[0].upper() for s in forex_section.items()]
what_to_show = 'MIDPOINT'

#contract_defs += forex_contracts
#names += names_forex

logger.info("validating contracts")
contract_details = [ib.reqContractDetails(c) for c in contract_defs]
contracts = [d[0].contract for d in contract_details]
time_series_fut = [ib.reqHistoricalData(
    contract,
    endDateTime='',
    durationStr='120 D',
    barSizeSetting='1 day',
    whatToShow='MIDPOINT' if contract.secType == 'CASH' else 'TRADES',
    useRTH=True) for contract in contracts]

series = [util.df(bars).set_index('date')['close'] for bars in time_series_fut]
series_conc = pd.concat(series, axis=1)
series_conc.columns = names
print(series_conc)

"""
pdb.set_trace()

C = Contract(exchange="GLOBEX", secType="FUT", symbol="ES")
Details = ib.reqContractDetails(C)
# expiry in format YYYYMMDD, so fine for sorting
Contracts = [d.contract for d in Details]
print ("ES futures (e-mini)")
for F in Contracts:
    print (F.localSymbol, F.lastTradeDateOrContractMonth,  F.conId)
data_fut = [ib.reqHistoricalData(
    contract,
    endDateTime='',
    durationStr='1 Y',
    barSizeSetting='1 day',
    whatToShow='TRADES',
    useRTH=True) for contract in Contracts]

contFut = Contract(secType='CONTFUT', exchange='GLOBEX',
                   symbol='ES')
[qualContFut] = ib.qualifyContracts(copy.copy(contFut))
[frontFut] = ib.qualifyContracts(Future(conId=qualContFut.conId))

try:
    bars = ib.reqHistoricalData(
        contFut,
        endDateTime='',
        durationStr='1 Y',
        barSizeSetting='1 day',
        whatToShow='TRADES',
        useRTH=True)
except Exception as e:
    print("in excep")
    print(type(e))
    print(e.args)
    print(e)

pdb.set_trace()
contDf, frontDf = [util.df(bars) for bars in data]
df = contDf.merge(frontDf, left_index=True, right_index=True)
print(df)

print(df[['date', 'open', 'high', 'low', 'close']])
"""
