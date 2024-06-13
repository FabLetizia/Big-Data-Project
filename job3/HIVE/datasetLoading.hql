-- Create a Hive table for the input data
CREATE TABLE IF NOT EXISTS stock_data (
    `ticker` STRING,
    `open` DOUBLE,
    `close` DOUBLE,
    `low` DOUBLE,
    `high` DOUBLE,
    `volume` INT,
    `date` STRING,
    `exchange` STRING,
    `name` STRING,
    `sector` STRING,
    `industry` STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE;


-- Load data into Hive table from hdfs
LOAD DATA INPATH '/input/historical_stocks_data.csv' INTO TABLE stock_data;