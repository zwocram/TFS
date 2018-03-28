import pdb
import time
import decimal

from ibapi.contract import Contract

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

    def get_historical_data(self, ib, instrument, duration="60 D",
                            sleep_time=5):
        """Get daily forex market data.

        :param ib: the ib application
        :param forex_list: list of forex items to be processed.

        :return: returns dictionary with forex data.
        """

        f_metadata = self._get_instrument_metadata(instrument)

        contract = self._get_ib_contract(ib,
                                         f_metadata["sec_type"],
                                         f_metadata["ticker"],
                                         f_metadata["exchange"],
                                         f_metadata["currency"])

        historic_data = ib.get_IB_historical_data(contract,
                                                  duration=duration)

        #close_set = f_metadata["identifier"], \
        #    float("{0:.4f}".format(historic_data[0][4]))
        time.sleep(sleep_time)
        ib.init_error()

        return historic_data

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

    def get_tfs_settings(self, tfs_settings=None):
        """Gets the settings for the trend following strategy
        from the settings.cfg file.

        :param tfs_settings: the 'tfs' section of the settings file

        :return: returns a dictionary with the tfs settings.
        """

        tfs_settings_dict = {}
        tfs_settings_dict['atr_horizon'] = int(tfs_settings['atr_horizon'])

        tfs_settings_dict['entry_breakout_periods'] = int(tfs_settings['entry_breakout_periods'])
        tfs_settings_dict['exit_breakout_periods'] = int(tfs_settings['exit_breakout_periods'])
        tfs_settings_dict['account_risk'] = decimal.Decimal(tfs_settings['account_risk'])
        tfs_settings_dict['unit_stop'] = int(tfs_settings['unit_stop'])
        tfs_settings_dict['first_unit_stop'] = int(tfs_settings['first_unit_stop'])
        tfs_settings_dict['nr_equities'] = int(tfs_settings['nr_equities'])
        tfs_settings_dict['nr_units'] = int(tfs_settings['nr_units'])

        return tfs_settings_dict
