from pyspark.sql import SparkSession
# Import of predefined functions
from pyspark.sql.functions import year, unix_timestamp, col, min, max, avg, first, last, round

# Spark session
spark = SparkSession.builder \
    .appName("Stock Analysis") \
    .getOrCreate()

# Read data from hdfs
stock_data = spark.read.csv("/user/fabio/input/historical_stocks_data.csv", header=True, inferSchema=True, sep=';')

# Creates the temporary table that allows you to query the stock_data dataframe
stock_data.createOrReplaceTempView("stock_data")

# Query (very similar to HIVE)
query = """
WITH stock_yearly_stats AS (
    SELECT
        ticker,
        name,
        YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP(date, 'yyyy-MM-dd'))) AS year,
        FIRST_VALUE(close) OVER (PARTITION BY ticker, YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP(date, 'yyyy-MM-dd'))) ORDER BY date) AS first_close,
        LAST_VALUE(close) OVER (PARTITION BY ticker, YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP(date, 'yyyy-MM-dd'))) ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_close,
        MIN(low) OVER (PARTITION BY ticker, YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP(date, 'yyyy-MM-dd')))) AS min_price,
        MAX(high) OVER (PARTITION BY ticker, YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP(date, 'yyyy-MM-dd')))) AS max_price,
        AVG(volume) OVER (PARTITION BY ticker, YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP(date, 'yyyy-MM-dd')))) AS avg_volume
    FROM stock_data
)
SELECT
    ticker,
    name,
    year,
    ROUND(((last_close - first_close) / first_close) * 100, 2) AS percent_change,
    min_price,
    max_price,
    ROUND(avg_volume, 2) AS avg_volume
FROM stock_yearly_stats
GROUP BY ticker, name, year, first_close, last_close, min_price, max_price, avg_volume
ORDER BY ticker, year
"""

result = spark.sql(query)

# Store output on hdfs
result.write.option("delimiter", "\t").csv("user/fabio/output/job1/spark_sql")

spark.stop()

# start: 16:17:21    end: 16:19:15 