#!/usr/bin/env python3
import sys
# ticker;open;close;low;high;volume;date;exchange;name;sector;industry 
for line in sys.stdin:
    line = line.strip()
    if line.startswith("ticker"):
        continue
    fields = line.split(';')
    if len(fields) == 11:
        ticker, open, close, low, high, volume, date, exchange, name, sector, industry = fields
        year = date[:4]
        # Pass to the reducer only the fields needed for job1
        key = f"{ticker};{name};{year}"
        value = f"{close};{low};{high};{volume};{date}"
        print(f"{key}\t{value}")