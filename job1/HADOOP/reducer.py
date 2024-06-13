#!/usr/bin/env python3
import sys
from collections import defaultdict

# Function to calculate the percentage change in the price over the year
def calculate_percentage_change(start, end):
    return ((end - start) / start) * 100 if start != 0 else 0

# Function to parse the input provided by the mapper
def parse_input_line(line):
    key, value = line.strip().split('\t')
    ticker, name, year = key.split(';')
    
    close, low, high, volume, date = value.split(';')
    close = float(close)
    low = float(low)
    high = float(high)
    volume = int(volume)
    # as key,value
    return (ticker, name, year), (close, low, high, volume, date)


def main():
    stock_data = defaultdict(list)
    for line in sys.stdin:
        key, value = parse_input_line(line)
        stock_data[key].append(value)
    
    result_data = []
    
    # Calculate what is required in job1 by analyzing the records relating to the same ticker, same name and same year
    for (ticker, name, year), daily_values in stock_data.items():
        daily_values.sort(key=lambda x: x[4])  # Sort by date
        first_close = daily_values[0][0]
        last_close = daily_values[-1][0]
        min_price = min(day[1] for day in daily_values)
        max_price = max(day[2] for day in daily_values)
        avg_volume = sum(day[3] for day in daily_values) / len(daily_values)
        
        percentage_change = round(calculate_percentage_change(first_close, last_close), 2)
        
        result_data.append((ticker, name, year, percentage_change, min_price, max_price, avg_volume))
    
    # Print the header
    print("ticker\tname\tyear\tpercentage_change\tmin_price\tmax_price\tavg_volume")
    
    # Print the data
    for ticker, name, year, percentage_change, min_price, max_price, avg_volume in sorted(result_data, key=lambda x: (x[0], x[2])):
        print(f"{ticker}\t{name}\t{year}\t{percentage_change:.2f}\t{min_price}\t{max_price}\t{avg_volume:.2f}")

if __name__ == "__main__":
    main()


''' 
tempo esecuzione:
LOCALE:
50%: 78 sec
100%: 172 sec
150%: 243 sec
200%: 298 sec

AWS:
 50%: 45 sec
 100%: 85 sec
 150%: 86 sec
 200%: 119 sec'''
