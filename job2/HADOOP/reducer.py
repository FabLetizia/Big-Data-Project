#!/usr/bin/env python3
import sys
import ast
from collections import defaultdict

def calculate_percentage_change(start, end):
    return ((end - start) / start) * 100 if start != 0 else 0

def main():
    data = defaultdict(list)
    
    for line in sys.stdin:
        key, value = line.strip().split('\t')
        key = ast.literal_eval(key)
        value = ast.literal_eval(value)
        data[key].append(value)
    
    result = []

    for (sector, industry, year), records in data.items():
        # Relating to a sector, industry and year,
        # sort the records by ticker (x[0]) and date (x[3])
        records.sort(key=lambda x: (x[0], x[3]))
        industry_tickers_first_close = {}
        industry_tickers_last_close = {}
        max_increment = -float('inf')
        max_increment_ticker = None
        total_ticker_volume = 0
        max_ticker_volume = (None, -float('inf'))
        ticker_records = defaultdict(list)
        
        # Here scroll through all the records relating to a sector, an industry and a year
        for record in records:
            ticker, close, volume, date = record
            close = float(close)
            volume = int(volume)
            # dict in which we associate each ticker (of that industry, sector and year) with its records
            ticker_records[ticker].append((date, close, volume))

        for ticker, records in ticker_records.items():
            # Sort the records of each ticker by date
            records.sort(key=lambda x: x[0])
            first_close = records[0][1]
            last_close = records[-1][1]
            increment = calculate_percentage_change(first_close, last_close)
            total_ticker_volume += volume
            
            # (b) the ticker of the industry which had the greatest percentage increase 
            # in the year (with indication of the increase)
            if increment > max_increment:
                max_increment = increment
                max_increment_ticker = (ticker, increment)
            
            # (c) the ticker of the industry that had the highest volume of transactions
            # in the year (with indication of the volume).
            if total_ticker_volume > max_ticker_volume[1]:
                max_ticker_volume = (ticker, total_ticker_volume)

    # (a) the percentage change in the industry price during the year,
            if ticker not in industry_tickers_first_close:
                industry_tickers_first_close[ticker] = first_close
            industry_tickers_last_close[ticker] = last_close
        
        industry_first_total = sum(industry_tickers_first_close.values())
        industry_last_total = sum(industry_tickers_last_close.values())
        industry_change = calculate_percentage_change(industry_first_total, industry_last_total)

        result.append((sector, industry, year, industry_change, max_increment_ticker, max_ticker_volume))
    
    ''' Sorting of the final output: in the report the industries must be grouped by 
    sector and sorted by decreasing order of percentage change.
    First sort by sector and if they have the same sector, sort by industry, 
    if they have the same industry, sort by industry_change. For idustry_change in descending order '''
    # Grouping of results by sector and industry
    grouped_results = defaultdict(list)
    for record in result:
        sector, industry, year, industry_change, max_increment_ticker, max_ticker_volume = record
        grouped_results[(sector, industry)].append(record)

    # Results grouped by sector and industry and sorted by decreasing percentage change
    sorted_results = sorted(grouped_results.items(), key=lambda x: x[1][0][3], reverse=True)

    for (sector, industry), industry_results in sorted_results:
        for record in industry_results:
            sector, industry, year, industry_change, max_increment_ticker, max_ticker_volume = record
            print(f"{sector}\t{industry}\t{year}\t{industry_change:.2f}%\t{max_increment_ticker}\t{max_ticker_volume}")

if __name__ == "__main__":
    main()

''' 19:59:17 - 20:09:59'''
