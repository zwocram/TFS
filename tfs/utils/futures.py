from datetime import datetime
import pdb

FORWARD_LOOKING_PERIOD = 6


class FuturesUtils(object):

    _date_today = datetime.today()
    _current_month = _date_today.month
    _current_year = _date_today.year

    _months_in_year = 12
    _futures_epxiration_data = []

    exp_months = {
        1: 'F',
        2: 'G',
        3: 'H',
        4: 'J',
        5: 'K',
        6: 'M',
        7: 'N',
        8: 'Q',
        9: 'U',
        10: 'V',
        11: 'X',
        12: 'Z'}

    def __init__(self, futures_list):
        self.futures_list = futures_list

    def resolve_expiration_month_codes(self):

        fut_exp_codes = {}
        fut_exp_months = {}

        sum_months = self._current_month + (FORWARD_LOOKING_PERIOD - 1)
        for future_symbol in self.futures_list.keys():
            fut_code = future_symbol
            if sum_months > self._months_in_year:
                diff_months = sum_months - self._months_in_year
                sub_one = {month: fut_code
                           + code
                           + (self._current_year + 1).__str__()[-2:]
                           for month, code in self.exp_months.items()
                           if month <= diff_months}
                sub_two = {month: fut_code
                           + code
                           + self._current_year.__str__()[-2:]
                           for month, code in self.exp_months.items()
                           if month >= self._current_month}
                futures_subset = sub_one.copy()
                futures_subset.update(sub_two)
            else:
                futures_subset = {month: fut_code
                                  + code
                                  + self._current_year.__str__()[-2:]
                                  for month, code in self.exp_months.items()
                                  if self._current_month
                                  + FORWARD_LOOKING_PERIOD
                                  > month
                                  >= self._current_month}
            future_codes = futures_subset.values()
            future_code_string = None
            for fc in future_codes:
                year = '20' + fc[-2:]
                month_from_code = list(self.exp_months.keys())[
                    list(self.exp_months.values()).index(fc[-3])]
                if month_from_code < 10:
                    month_from_code = '0' + month_from_code.__str__()
                else:
                    month_from_code = month_from_code.__str__()
                year_month_combo = year + month_from_code
                if future_code_string is None:
                    future_code_string = fc
                    future_month_string = year_month_combo
                else:
                    future_code_string += ', ' + fc
                    future_month_string += ', ' + year_month_combo

            fut_exp_codes[fut_code] = future_code_string
            fut_exp_months[fut_code] = future_month_string

        self._futures_epxiration_data.append(fut_exp_codes)
        self._futures_epxiration_data.append(fut_exp_months)

        return self._futures_epxiration_data
