#!/usr/bin/env python3

import argparse
from pyspark.sql import SparkSession
from collections import defaultdict

def calculate_percentage_change(start, end):
    return ((end - start) / start) * 100 if start != 0 else 0

def parse_line(line):
    line = line.strip()
    fields = line.split(';')
    if line.startswith("ticker") or len(fields) != 11:
        return None
    ticker, _, close, _, _, volume, date, _, _, sector, industry = fields
    year = date[:4]
    if sector and industry:
        key = (sector, industry, year)
        value = (ticker, float(close), int(volume), date)
        return (key, value)
    return None

''' This function takes the values associated with a key (industry, sector, year) and for 
that specific year calculates the percentage increase of the industry, the ticker of the 
industry with the greatest percentage increase and the ticker of the industry with the 
greatest volume'''
def aggregate_records(records):
    ticker_records = defaultdict(list)
    industry_tickers_first_close = {}
    industry_tickers_last_close = {}
    total_ticker_volume = defaultdict(int)

    for record in records:
        ticker, close, volume, date = record
        ticker_records[ticker].append((date, close, volume))

    max_increment = -float('inf')
    max_increment_ticker = None
    max_ticker_volume = (None, -float('inf'))

    for ticker, recs in ticker_records.items():
        recs.sort(key=lambda x: x[0])
        first_close = recs[0][1]
        last_close = recs[-1][1]
        increment = calculate_percentage_change(first_close, last_close)
        for rec in recs:
            total_ticker_volume[ticker] += rec[2]

        if increment > max_increment:
            max_increment = increment
            max_increment_ticker = (ticker, increment)

        if total_ticker_volume[ticker] > max_ticker_volume[1]:
            max_ticker_volume = (ticker, total_ticker_volume[ticker])

        if ticker not in industry_tickers_first_close:
            industry_tickers_first_close[ticker] = first_close
        industry_tickers_last_close[ticker] = last_close

    industry_first_total = sum(industry_tickers_first_close.values())
    industry_last_total = sum(industry_tickers_last_close.values())
    industry_change = calculate_percentage_change(industry_first_total, industry_last_total)

    return (industry_change, max_increment_ticker, max_ticker_volume)


parser = argparse.ArgumentParser()
# input and output paths passed from the command line
parser.add_argument("--input_path", type=str, help="Input dataset path")
parser.add_argument("--output_path", type=str, help="Output folder path")
 
args = parser.parse_args()
dataset_filepath, output_filepath = args.input_path, args.output_path

spark = SparkSession \
    .builder \
    .appName("Stock Analysis") \
    .getOrCreate()

lines = spark.sparkContext.textFile(dataset_filepath).cache()
    
parsed_lines = lines.map(parse_line).filter(lambda x: x is not None)
    
grouped = parsed_lines.groupByKey().mapValues(list)
    
aggregated = grouped.mapValues(aggregate_records)
    
result = aggregated.collect()
    
grouped_results = defaultdict(list)
for record in result:
    key, values = record
    sector, industry, year = key
    industry_change, max_increment_ticker, max_ticker_volume = values
    grouped_results[(sector, industry)].append((sector, industry, year, industry_change, max_increment_ticker, max_ticker_volume))

sorted_results = sorted(grouped_results.items(), key=lambda x: x[1][0][3], reverse=True)

for (sector, industry), industry_results in sorted_results:
    industry_results.sort(key=lambda x: (x[0], x[3]))

# Final storing
sorted_results_rdd = spark.sparkContext.parallelize([
    f"{sector}\t{industry}\t{year}\t{industry_change:.2f}%\t({max_increment_ticker[0]},{max_increment_ticker[1]:.2f})%\t({max_ticker_volume[0]},{max_ticker_volume[1]})"
    for (sector, industry), industry_results in sorted_results
    for sector, industry, year, industry_change, max_increment_ticker, max_ticker_volume in industry_results
])
sorted_results_rdd.coalesce(1).saveAsTextFile(output_filepath)
spark.stop()

'''
tempo esecuzione:
LOCALE:
50%:  43 sec
100%: 60 sec
150%: 87 sec
200%: 102 sec

AWS:
 50%: 42 sec
 100%: 55 sec
 150%: 68 sec
 200%: 80 sec
'''




