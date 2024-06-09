-- Table with addition of the year field
CREATE TABLE stock_data_year AS
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
    YEAR(TO_DATE(`date`)) AS year
FROM
    stock_data;

-- Table containing the ticker, year, first closing price, and last closing price for each stock in each year
CREATE TABLE stock_first_last_close AS
SELECT
    ticker,
    year,
    FIRST_VALUE(close) OVER (PARTITION BY ticker, year ORDER BY stock_date) AS first_close,
    LAST_VALUE(close) OVER (PARTITION BY ticker, year ORDER BY stock_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_close
FROM
    stock_data_year;

-- Table in which the following are calculated for each ticker: the annual percentage change, the annual minimum, the annual maximum and the annual volume.
CREATE TABLE stock_statistics AS
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
    sdy.ticker, sdy.name, sdy.year, sdy.sector, sdy.industry, sflc.first_close, sflc.last_close;

-- Summing up first and last close prices for each industry in each year
CREATE TABLE industry_first_last_close AS
SELECT
    sector,
    industry,
    year,
    SUM(open) AS industry_first_close,
    SUM(close) AS industry_last_close
FROM
    stock_data_year
GROUP BY
    sector, industry, year;

-- Calculating the industry annual percentage change
CREATE TABLE industry_annual_change AS
SELECT
    sector,
    industry,
    year,
    ROUND(((industry_last_close - industry_first_close) / industry_first_close) * 100, 2) AS industry_percent_change
FROM
    industry_first_last_close;

-- Table to calculate the ticker with the highest percentage increase for each industry
CREATE TABLE industry_max_increase AS
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
    sector, industry, year;

-- Table to calculate the ticker with the highest volume for each industry
CREATE TABLE industry_max_volume AS
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
    sector, industry, year;

-- Final table with all statistics
CREATE TABLE output AS
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
    iac.sector, iac.industry, iac.year DESC;

-- for the output
INSERT OVERWRITE DIRECTORY './output'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT * FROM output;

DROP TABLE stock_data;
DROP TABLE stock_data_year;
DROP TABLE stock_first_last_close;
DROP TABLE stock_statistics;
DROP TABLE industry_first_last_close;
DROP TABLE industry_annual_change;
DROP TABLE industry_max_increase;
DROP TABLE industry_max_volume;
DROP TABLE output;


/*  
tempo esecuzione:
LOCALE:
50%: 450 sec
100%: 899 sec
150%: 1060 sec
200%: 1573 sec

AWS:
 50%: 163 sec
 100%: 355 sec
 150%: 471 sec
 200%: 667 sec
  */