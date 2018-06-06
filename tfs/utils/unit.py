import math


class Unit(object):

    def __init__(self, account_size=0, atr=0,
                 account_risk=0.02, unit_stop=2,
                 first_unit_stop=1, nr_equities=6,
                 nr_units=4, ticker=None,
                 price=0, pos_type=None,
                 first_unit=False, point_value=1):

        self.first_unit = first_unit
        self.point_value = point_value
        self.price = price
        self.ticker = ticker
        self.account_size = account_size
        self.atr = atr
        self.account_risk = account_risk
        self.unit_stop = unit_stop
        self.first_unit_stop = first_unit_stop
        self.nr_equities = nr_equities
        self.nr_units = nr_units
        if pos_type is None:
            self.stop_level = 0
        else:
            self.stop_level = self._calc_stop_level(atr, price,
                                                    pos_type, first_unit)

    def _calc_position_size_equal_units(self, price):
        """Calculate position size based on equal unit sizing.
        """
        pos_size = math.floor((self.account_size /
                               (self.nr_equities * self.nr_units))
                              / price)

        return pos_size

    def calc_position_size_risk_perc(self, first_unit=False,
                                     long_short="long"):
        """Calculate position size based on risk percentage.
        """

        pos_sign_corr = 1
        if long_short == "short":
            pos_sign_corr = -1

        if first_unit:
            stop = self.first_unit_stop
        else:
            stop = self.unit_stop

        pos_size = math.floor((self.account_size * float(self.account_risk)) /
                              (stop * self.atr * self.point_value))

        return pos_sign_corr * pos_size

    def _calc_stop_level(self, atr, entry_price, position_type,
                         first_unit=False):
        stop_level = None
        if first_unit:
            if position_type == "long":
                stop_level = entry_price - atr
            elif position_type == "short":
                stop_level = entry_price + atr
        else:
            if position_type == "long":
                stop_level = entry_price - 2 * atr
            elif position_type == "short":
                stop_level = entry_price + 2 * atr

        return float("{0:.2f}".format(stop_level))
