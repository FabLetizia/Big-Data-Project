from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Stock Analysis") \
    .getOrCreate()

# Read data from hdfs
stock_data = spark.read.csv('/input/historical_stocks_data.csv', header=True, sep=';', inferSchema=True)

# Creates the temporary table that allows you to query the stock_data dataframe
stock_data.createOrReplaceTempView("stock_data")

# Table with addition of the year field
spark.sql("""
CREATE OR REPLACE TEMP VIEW stock_data_year AS
SELECT
    `ticker`,
    `open`,
    `close`,
    `low`,
    `high`,
    `volume`,
    `date` AS stock_date,
    `exchange`,
    `name`,
    `sector`,
    `industry`,
    YEAR(TO_DATE(stock_date, 'yyyy-MM-dd')) AS `year`
FROM
    stock_data
""")

# Table to store intermediate results
spark.sql("""
CREATE OR REPLACE TEMP VIEW stock_changes AS
SELECT
    `ticker`,
    `year`,
    MIN(CAST(`close` AS DOUBLE)) AS `start_price`,
    MAX(CAST(`close` AS DOUBLE)) AS `end_price`,
    `name`
FROM stock_data_year
WHERE CAST(`year` AS INT) >= 2000
GROUP BY `ticker`, `year`, `name`
""")

# Calculation of percentage changes
spark.sql("""
CREATE OR REPLACE TEMP VIEW stock_percentage_changes AS
SELECT
    `ticker`,
    `year`,
    ((`end_price` - `start_price`) / `start_price`) * 100 AS `percentage_change`,
    `name`
FROM stock_changes
WHERE `start_price` IS NOT NULL AND `end_price` IS NOT NULL
""")

# Find 3 consecutive year patterns
spark.sql("""
CREATE OR REPLACE TEMP VIEW stock_trends AS
SELECT
    s1.`ticker`,
    s1.`year` AS `year1`,
    s2.`year` AS `year2`,
    s3.`year` AS `year3`,
    s1.`percentage_change` AS `change1`,
    s2.`percentage_change` AS `change2`,
    s3.`percentage_change` AS `change3`,
    s1.`name`
FROM
    stock_percentage_changes s1
JOIN
    stock_percentage_changes s2 ON s1.`ticker` = s2.`ticker` AND s1.`year` + 1 = s2.`year`
JOIN
    stock_percentage_changes s3 ON s1.`ticker` = s3.`ticker` AND s1.`year` + 2 = s3.`year`
ORDER BY
    s1.`ticker`, s1.`year`
""")

# Select tickers with the same 3-year trend patterns
spark.sql("""
CREATE OR REPLACE TEMP VIEW output AS
SELECT
    COLLECT_SET(s1.`ticker`) AS `tickers`,
    CONCAT('[', s1.`change1`, ', ', s2.`change2`, ', ', s3.`change3`, ']') AS `changes`,
    CONCAT('[', s1.`year1`, ', ', s2.`year2`, ', ', s3.`year3`, ']') AS `years`
FROM
    stock_trends s1
JOIN
    stock_trends s2 ON s1.`ticker` = s2.`ticker` AND s1.`year1` + 1 = s2.`year2`
JOIN
    stock_trends s3 ON s1.`ticker` = s3.`ticker` AND s1.`year1` + 2 = s3.`year3`
GROUP BY
    s1.`year1`, s2.`year2`, s3.`year3`, s1.`change1`, s2.`change2`, s3.`change3`
HAVING SIZE(COLLECT_SET(s1.`ticker`)) > 1
""")

# Output
output = spark.sql("SELECT * FROM output")

# Store output on hdfs
output.write.option("delimiter", "\t").save("/output/job3/spark_sql")

# Drop the temporary views
spark.catalog.dropTempView("stock_data")
spark.catalog.dropTempView("stock_data_year")
spark.catalog.dropTempView("stock_changes")
spark.catalog.dropTempView("stock_percentage_changes")
spark.catalog.dropTempView("stock_trends")
spark.catalog.dropTempView("stock_trends_patterns")

spark.stop()

''' 
tempo esecuzione:
LOCALE:
50%: 51 sec
100%: 104 sec
150%: 143 sec
200%: 220 sec

AWS:
 50%: 16 sec
 100%: 37 sec
 150%: 45 sec
 200%: 55 sec
 '''