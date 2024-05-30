#!/usr/bin/env python3
import sys

# ticker;open;close;low;high;volume;date;exchange;name;sector;industry

for line in sys.stdin:
    line = line.strip()
    parts = line.split(';')
    # skip the header
    if line.startswith("ticker"):
        continue
    # Skip malformed lines
    if len(parts) != 11:
        continue
    ticker, _, close, _, _, volume, date, _, name, sector, industry = parts
    year = date[:4]
    if sector and industry:
        key = (sector, industry, year)
        value = (ticker, close, volume, date)
        print(f"{key}\t{value}")

'''16:18:18 - 16:28:15'''
    
