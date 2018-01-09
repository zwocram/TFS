from datetime import datetime

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

    def resolve_expiration_month_codes(self):

        expiration_dates = []

        sum_months = self._current_month + (FORWARD_LOOKING_PERIOD - 1)
        if sum_months > self._months_in_year:
            diff_months = sum_months - self._months_in_year
            sub_one = {month: code
                       + (self._current_year + 1).__str__()[-2:]
                       for month, code in self.exp_months.items()
                       if month <= diff_months}
            sub_two = {month: code
                       + self._current_year.__str__()[-2:]
                       for month, code in self.exp_months.items()
                       if month >= self._current_month}
            futures_subset = sub_one.copy()
            futures_subset.update(sub_two)
        else:
            futures_subset = {month: code
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
            new_expiration_period = future_code_string, future_month_string

        expiration_dates.append(new_expiration_period)

        return expiration_dates
