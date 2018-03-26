import pdb

from ibapi.contract import Contract

from utils.strategies import TFS, Unit
from db.database import Database


class Driver(object):
    """Class that drives 'The Process'.
    """

    def get_account_data(self, ib):
        """Retrieves some data about the IB account.

        :param ib: The IB application
        :return: buying power and account size

        """
        account_info = ib.get_account_summary(9001)
        buying_power = float([a[3] for a in account_info
                              if a[2] == 'BuyingPower'][0])
        account_size = float([a[3] for a in account_info
                              if a[2] == 'NetLiquidation'][0])

        return buying_power, account_size

    def get_forex_market_data(self, ib, forex_list):
        """Get daily forex market data.

        :param ib: the ib application
        :param forex_list: list of forex items to be processed.

        :return: returns dictionary with forex data.
        """

        close_prices = []
        for f in forex_list:
            f_metadata = self._get_instrument_metadata(f)

            contract = self._get_ib_contract(ib,
                                             f_metadata["sec_type"],
                                             f_metadata["ticker"],
                                             f_metadata["exchange"],
                                             f_metadata["currency"])

            historic_data = ib.get_IB_historical_data(contract,
                                                      duration="1 D")

            close_set = f_metadata["identifier"], \
                float("{0:.4f}".format(historic_data[0][4]))
            close_prices.append(close_set)

        return close_prices

    def _get_ib_contract(self, ib, security_type, symbol, exchange_name,
                         currency):
        """Gets the contract from interactive brokers.

        :param ib: the ib application
        :param security_type: type of instrument to search
        :param symbol: the symbol or ticker of the instrument
        :param exchange_name: the name of the exchange to search
        :param currency: currency in which instrument is denominated

        :return: the IB contract
        """

        ibcontract = Contract()
        ibcontract.secType = security_type
        ibcontract.symbol = symbol
        ibcontract.exchange = exchange_name
        ibcontract.currency = currency

        return ib.resolve_ib_contract(ibcontract)

    def _get_instrument_metadata(self, instrument):
        """Retrieves the metadata for each instrument in the
        instrument list.

        :param instrument: instrument as read from the config file

        :return: dictionary of instrument properties
        """

        instrument_props = {}
        instrument_props["identifier"] = instrument[0].upper()
        instrument_props["exchange"] = instrument[1].split(',')[1].lstrip()
        instrument_props["sec_type"] = instrument[1].split(',')[2].lstrip()
        instrument_props["currency"] = \
            instrument[1].split(', ')[3].lstrip().upper()
        instrument_props["ticker"] = \
            instrument[1].split(',')[4].lstrip().upper()

        return instrument_props
