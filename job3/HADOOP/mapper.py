#!/usr/bin/env python3
import sys
from collections import defaultdict
# ticker;open;close;low;high;volume;date;exchange;name;sector;industry
stock_data = defaultdict(lambda: {'closing_prices': [], 'company_name': ''})

for line in sys.stdin:
    line = line.strip()
    # skip the header
    if line.startswith("ticker"):
        continue
    fields = line.split(';')
    if len(fields) == 11:
        ticker, open, close, low, high, volume, date, exchange, name, sector, industry = fields
        year = date[:4]
        year = int(year)
        if year >= 2000:
            stock_data[(year, ticker)]['closing_prices'].append((date, close))
            stock_data[(year, ticker)]['company_name'] = name

# Sorting and calculating percentage change
for key in stock_data:
    # Sort closing prices by date
    stock_data[key]['closing_prices'].sort(key=lambda x: x[0])
    closing_prices = [price for date, price in stock_data[key]['closing_prices']]

    if len(closing_prices) > 2:
        initial_price = float(closing_prices[0])
        final_price = float(closing_prices[-1])
        percent_change = ((final_price - initial_price) / initial_price) * 100
        print(f"{key[1]}\t{key[0]}\t{percent_change:.1f}\t{stock_data[key]['company_name']}")

