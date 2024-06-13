#!/usr/bin/env python3
import sys
# ticker;open;close;low;high;volume;date;exchange;name;sector;industry
for line in sys.stdin:
    line = line.strip()
    # skip the header
    if line.startswith("ticker"):
        continue
    fields = line.split(';')
    # Skip malformed lines
    if len(fields) == 11:
        ticker, _, close, _, _, volume, date, _, name, sector, industry = fields
        year = date[:4]
        if sector and industry:
            key = (sector, industry, year)
            value = (ticker, close, volume, date)
            print(f"{key}\t{value}")
    
