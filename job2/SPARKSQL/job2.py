from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Stock Data Analysis") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "4") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Read data from hdfs
stock_data = spark.read.csv('/user/historical_stocks_data.csv', header=True, sep=';', inferSchema=True)

# Creates the temporary table that allows you to query the stock_data dataframe
stock_data.createOrReplaceTempView("stock_data")

# Table with addition of the year field
spark.sql("""
CREATE OR REPLACE TEMP VIEW stock_data_year AS
SELECT
    ticker,
    open,
    close,
    low,
    high,
    volume,
    `date` AS stock_date,
    `exchange`,
    name,
    sector,
    industry,
    YEAR(TO_DATE(stock_date, 'yyyy-MM-dd')) AS year
FROM
    stock_data
""")

# Summing up first and last close prices for each industry in each year
spark.sql("""
CREATE OR REPLACE TEMP VIEW industry_first_last_close AS
SELECT
    sector,
    industry,
    year,
    SUM(open) AS industry_first_close,
    SUM(close) AS industry_last_close
FROM
    stock_data_year
GROUP BY
    sector, industry, year
""")

# Calculating the industry annual percentage change
spark.sql("""
CREATE OR REPLACE TEMP VIEW industry_annual_change AS
SELECT
    sector,
    industry,
    year,
    ROUND(((industry_last_close - industry_first_close) / industry_first_close) * 100, 2) AS industry_percent_change
FROM
    industry_first_last_close
""")

# Table containing the ticker, year, first closing price, and last closing price for each stock in each year
spark.sql("""
CREATE OR REPLACE TEMP VIEW stock_first_last_close AS
SELECT
    ticker,
    year,
    FIRST_VALUE(close) OVER (PARTITION BY ticker, year ORDER BY stock_date) AS first_close,
    LAST_VALUE(close) OVER (PARTITION BY ticker, year ORDER BY stock_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_close
FROM
    stock_data_year
""")

# Table in which the following are calculated for each ticker: the annual percentage change, the annual minimum, the annual maximum and the annual volume.
spark.sql("""
CREATE OR REPLACE TEMP VIEW stock_statistics AS
SELECT
    sdy.ticker,
    sdy.name,
    sdy.year,
    sdy.sector,
    sdy.industry,
    ROUND(((sflc.last_close - sflc.first_close) / sflc.first_close) * 100, 2) AS percent_change,
    ROUND(MIN(sdy.low), 2) AS min_low,
    ROUND(MAX(sdy.high), 2) AS max_high,
    ROUND(SUM(sdy.volume), 2) AS total_volume
FROM
    stock_data_year sdy
JOIN
    stock_first_last_close sflc
ON
    sdy.ticker = sflc.ticker AND sdy.year = sflc.year
GROUP BY
    sdy.ticker, sdy.name, sdy.year, sdy.sector, sdy.industry, sflc.first_close, sflc.last_close
""")

# Table to calculate the ticker with the highest percentage increase for each industry
spark.sql("""
CREATE OR REPLACE TEMP VIEW industry_max_increase AS
SELECT
    sector,
    industry,
    year,
    FIRST(ticker) AS ticker,
    FIRST(name) AS name,
    MAX(percent_change) AS highest_percent_change
FROM
    stock_statistics
GROUP BY
    sector, industry, year
""")

# Table to calculate the ticker with the highest volume for each industry
spark.sql("""
CREATE OR REPLACE TEMP VIEW industry_max_volume AS
SELECT
    sector,
    industry,
    year,
    FIRST(ticker) AS ticker,
    FIRST(name) AS name,
    MAX(total_volume) AS highest_volume
FROM
    stock_statistics
GROUP BY
    sector, industry, year
""")

# Final table with all statistics
spark.sql("""
CREATE OR REPLACE TEMP VIEW output AS
SELECT
    iac.sector,
    iac.industry,
    iac.year,
    iac.industry_percent_change,
    imh.name AS highest_increase_stock,
    imh.highest_percent_change,
    imv.name AS highest_volume_stock,
    imv.highest_volume
FROM
    industry_annual_change iac
JOIN
    industry_max_increase imh
ON
    iac.sector = imh.sector AND iac.industry = imh.industry AND iac.year = imh.year
JOIN
    industry_max_volume imv
ON
    iac.sector = imv.sector AND iac.industry = imv.industry AND iac.year = imv.year
ORDER BY
    iac.sector, iac.industry, iac.year DESC
""")

output = spark.sql("SELECT * FROM output")

# Store output on hdfs
output.write.option("delimiter", "\t").text("/user/spark_sql/output.txt")

spark.catalog.dropTempView("stock_data")
spark.catalog.dropTempView("stock_data_year")
spark.catalog.dropTempView("stock_first_last_close")
spark.catalog.dropTempView("stock_statistics")
spark.catalog.dropTempView("industry_first_last_close")
spark.catalog.dropTempView("industry_annual_change")
spark.catalog.dropTempView("industry_max_increase")
spark.catalog.dropTempView("industry_max_volume")
spark.catalog.dropTempView("output")

'''
tempo esecuzione:
LOCALE:
50%: 312 sec
100%: 500 sec
150%: 698 sec
200%: 821 sec

AWS:
 50%: 35 sec
 100%: 47 sec
 150%: 50 sec
 200%: 118 sec
'''