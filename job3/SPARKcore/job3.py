#!/usr/bin/env python3
import argparse
from pyspark import SparkContext, SparkConf

# Function to parse each line of the input
def parse_line(line):
    fields = line.strip().split(';')
    if line.startswith("ticker") or len(fields) != 11:
        return None
    ticker, _, close, _, _, _, date, _, name, _, _ = fields
    year = date[:4]
    try:
        year = int(year)
        close = float(close)
        if year >= 2000:
            return (ticker, (year, close, name))
    except ValueError:
        pass
    return None

# Function to calculate percentage change for each year
def calculate_percentage_change(data):
    data = sorted(data, key=lambda x: x[0])  # Sort by date
    if len(data) > 1:
        start_price = float(data[0][1])
        end_price = float(data[-1][1])
        percent_change = ((end_price - start_price) / start_price) * 100
        year = data[0][0]
        name = data[0][2]
        return (year, percent_change, name)
    return None

# Prepare data for pattern analysis
def prepare_pattern_data(data):
    ticker, year_percentage = data
    year, percentage_change, name = year_percentage
    return (ticker, (year, percentage_change, name))

# Function to find patterns of 3 consecutive years
def find_patterns(data):
    ticker, year_data = data
    year_data = sorted(year_data, key=lambda x: x[0])
    patterns = []
    for i in range(len(year_data) - 2):
        year1, change1, name1 = year_data[i]
        year2, change2, name2 = year_data[i + 1]
        year3, change3, name3 = year_data[i + 2]
        if year3 - year1 == 2:  # Check for consecutive years
            patterns.append(((year1, year2, year3), (change1, change2, change3), name1))
    return patterns

# Group by pattern and find tickers with the same pattern
def group_patterns(pattern):
    years, changes, name = pattern
    return (years, changes), [name]

# Prepare final output format
def format_output(data):
    (years, changes), tickers = data
    return f"{tickers} {list(changes)} {list(years)}"

# Configure Spark
conf = SparkConf().setAppName("StockAnalysis")
sc = SparkContext(conf=conf)

# Argument parser for input and output paths
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input dataset path")
parser.add_argument("--output_path", type=str, help="Output folder path")
args = parser.parse_args()

input_path = args.input_path
output_path = args.output_path

# Read input data
lines = sc.textFile(input_path)

# Remove header and filter invalid lines
header = lines.first()
rows = lines.filter(lambda line: line != header).map(parse_line).filter(lambda x: x is not None)

# Group by ticker and year, and collect closing prices
grouped_data = rows.groupByKey().mapValues(list)

# Calculate percentage changes
percentage_changes = grouped_data.mapValues(calculate_percentage_change).filter(lambda x: x[1] is not None)

# Prepare data for pattern analysis
pattern_data = percentage_changes.map(prepare_pattern_data).groupByKey().mapValues(list)

# Find patterns
patterns = pattern_data.flatMap(find_patterns).filter(lambda x: len(x) > 0)

pattern_groups = patterns.map(group_patterns).reduceByKey(lambda a, b: a + b).filter(lambda x: len(x[1]) > 1)

# Save results to the output path
pattern_groups.map(format_output).saveAsTextFile(output_path)

sc.stop()


''' 
tempo esecuzione:
LOCALE:
50%: 39 sec
100%: 63 sec
150%: 68 sec
200%: 100 sec

AWS:
 50%: 37 sec
 100%: 52 sec
 150%: 52 sec
 200%: 71 sec
 '''
