-- Set number of reducer
SET mapreduce.job.reduces=2;

-- This part of the code creates a CTE (Common Table Expression) called stock_yearly_stats, 
-- which calculates yearly statistics for each stock
WITH stock_yearly_stats AS (
    SELECT
        ticker,
        `name`,
        YEAR(TO_DATE(`date`)) AS year,        
        FIRST_VALUE(close) OVER (PARTITION BY ticker, YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP(`date`, 'yyyy-MM-dd'))) ORDER BY `date`) AS first_close,
        LAST_VALUE(close) OVER (PARTITION BY ticker, YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP(`date`, 'yyyy-MM-dd'))) ORDER BY `date` ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_close,
        MIN(low) OVER (PARTITION BY ticker, YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP(`date`, 'yyyy-MM-dd')))) AS min_price,
        MAX(high) OVER (PARTITION BY ticker, YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP(`date`, 'yyyy-MM-dd')))) AS max_price,
        AVG(volume) OVER (PARTITION BY ticker, YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP(`date`, 'yyyy-MM-dd')))) AS avg_volume
    FROM stock_data
)

-- Final table with all statistics 
-- (including the calculation of the percentage change in the closing price)
CREATE TABLE output AS
SELECT
    ticker,
    `name`,
    `year`,
    ROUND(((last_close - first_close) / first_close) * 100, 2) AS percentage_change,
    min_price,
    max_price,
    ROUND(avg_volume, 2) AS avg_volume
FROM stock_yearly_stats
GROUP BY ticker, `name`, `year`, first_close, last_close, min_price, max_price, avg_volume
ORDER BY ticker, `year`;

-- for the output
INSERT OVERWRITE DIRECTORY './output'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT * FROM output;

/* 
tempo esecuzione: 
LOCALE:
50%: 298 sec
100%: 730 sec
150%: 1002 sec
200%: 1641 sec

AWS:
 50%: 160 sec
 100%: 340 sec
 150%: 327 sec
 200%: 462 sec 
 */


