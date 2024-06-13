#!/usr/bin/env python3
import sys
from collections import defaultdict

def main():
    trend_data = defaultdict(list)
    ticker_year_data = defaultdict(dict)

    # Read input and organize data
    for line in sys.stdin:
        ticker, year, percent_change, name = line.strip().split('\t')
        year = int(year)
        percent_change = float(percent_change)
        trend_data[ticker].append((year, percent_change, name))
        ticker_year_data[ticker][year] = percent_change

    # Dictionary to group tickers with the same trends over the same years
    trend_patterns = defaultdict(list)

    # Process data for each ticker once
    for ticker, yearly_data in ticker_year_data.items():
        sorted_years = sorted(yearly_data.keys())

        # Look for patterns of 3 consecutive years
        for i in range(len(sorted_years) - 2):
            year_one, year_two, year_three = sorted_years[i:i + 3]
            if int(year_three) - int(year_one) == 2:  # consecutive years
                # Create a unique key for the 3-year trend pattern
                trend_key = (year_one, year_two, year_three, yearly_data[year_one], yearly_data[year_two], yearly_data[year_three])
                trend_patterns[trend_key].append(ticker)
    # Print the header
    print("[ticker1,ticker2]\t[trend1, trend2, trend3]\t[year1, year2, year3]")
    
    # Print the data
    for (year_one, year_two, year_three, trend_one, trend_two, trend_three), tickers in trend_patterns.items():
        if len(tickers) > 1:
            print(f"{tickers} [{trend_one}, {trend_two}, {trend_three}] [{year_one}, {year_two}, {year_three}]")

if __name__ == "__main__":
    main()

''' 
tempo esecuzione:
LOCALE:
50%:  52 sec
100%: 58 sec
150%: 89 sec
200%: 116 sec

AWS:
 50%: 43 sec
 100%: 50 sec
 150%: 69 sec
 200%: 88 sec
 '''

