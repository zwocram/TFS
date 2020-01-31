from yahoo_fin.stock_info import *

import datetime as dt
from datetime import date


etf_data = get_data('XLF', start_date='12/01/2019')
print(etf_data)

tday = date.today()
delta_days = dt.timedelta(days=180)

print("today: ", tday)
print("history: ", tday - delta_days)
