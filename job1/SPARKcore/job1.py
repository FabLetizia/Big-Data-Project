#!/usr/bin/env python3

from datetime import datetime
import argparse
from pyspark.sql import SparkSession
import csv
from io import StringIO

def parse_line(line):
    # Extract the fields
    fields = next(csv.reader(StringIO(line), delimiter=';'))
    # Skip the header (first line)
    if fields[0] == "ticker":
        return None
    ticker = fields[0]
    date = datetime.strptime(fields[6], '%Y-%m-%d')
    year = date.year
    close = float(fields[2])
    low = float(fields[3])
    high = float(fields[4])
    volume = float(fields[5])
    name = fields[8]
    # (key, values)
    return ((ticker, year), (date, close, low, high, volume, name))
 
''' This function takes the values ​​associated with a key (ticker, year), sorts them by date,
then calculates yearly statistics such as percentage change in price, lowest price, highest price,
and average volume. Returns a tuple with these statistics.'''
def calculate_stats(values):
    sorted_values = sorted(values, key=lambda x: x[0])
    _, close_prices, low_prices, high_prices, volumes, name = zip(*sorted_values)
    first_close = close_prices[0]
    last_close = close_prices[-1]
    # All rounded to 2 decimal places
    percentual_variation = round(((last_close - first_close) / first_close) * 100, 2)
    max_high = round(max(high_prices),2)
    min_low = round(min(low_prices),2)
    mean_volume = round(sum(volumes) / len(volumes),2)
    return (name[0], percentual_variation, min_low, max_high, mean_volume)


parser = argparse.ArgumentParser()
# input and output paths passed from the command line
parser.add_argument("--input_path", type=str, help="Input dataset path")
parser.add_argument("--output_path", type=str, help="Output folder path")
 
args = parser.parse_args()
dataset_filepath, output_filepath = args.input_path, args.output_path
 
spark = SparkSession \
    .builder \
    .appName("Stock Annual Trend") \
    .getOrCreate()
 
lines = spark.sparkContext.textFile(dataset_filepath).cache()
data = lines.map(parse_line).filter(lambda x: x is not None).groupByKey()

stats_stock_year = data.mapValues(calculate_stats)
output = stats_stock_year.map(lambda x: (x[0], x[1]))

output_sorted = output.sortByKey()

# Reduce the number of partitions to 1 before saving the output
output_sorted.coalesce(1).saveAsTextFile(output_filepath)

spark.stop()

''' 
tempo esecuzione:
LOCALE:
50%: 41 sec
100%: 85 sec
150%: 112 sec
200%: 141 sec

AWS:
 50%: 34 sec
 100%: 36 sec
 150%: 133 sec
 200%: 136 sec
 '''
