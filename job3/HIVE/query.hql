-- Table with addition of the year field
CREATE TABLE stock_data_year AS
SELECT
    `ticker`,
    `open`,
    `close`,
    `low`,
    `high`,
    `volume`,
    `date`,
    `exchange`,
    `name`,
    `sector`,
    `industry`,
    YEAR(TO_DATE(`date`, 'yyyy-MM-dd')) AS `year`
FROM stock_data
WHERE CAST(YEAR(TO_DATE(`date`, 'yyyy-MM-dd')) AS INT) >= 2000;

-- Table to store intermediate results
CREATE TABLE stock_changes AS
SELECT
    `ticker`,
    `year`,
    MIN(CAST(`close` AS DOUBLE)) AS `start_price`,
    MAX(CAST(`close` AS DOUBLE)) AS `end_price`,
    `name`
FROM stock_data_year
GROUP BY `ticker`, `year`, `name`;

-- Calculation of percentage changes
CREATE TABLE stock_percentage_changes AS
SELECT
    `ticker`,
    `year`,
    ((`end_price` - `start_price`) / `start_price`) * 100 AS `percentage_change`,
    `name`
FROM stock_changes
WHERE `start_price` IS NOT NULL AND `end_price` IS NOT NULL;

-- Find 3 consecutive year patterns
CREATE TABLE stock_trends AS
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
    `stock_percentage_changes` s1
JOIN
    `stock_percentage_changes` s2 ON s1.`ticker` = s2.`ticker` AND s1.`year` + 1 = s2.`year`
JOIN
    `stock_percentage_changes` s3 ON s1.`ticker` = s3.`ticker` AND s1.`year` + 2 = s3.`year`
ORDER BY
    s1.`ticker`, s1.`year`;

-- Select tickers with the same 3-year trend patterns
CREATE TABLE output AS
SELECT
    COLLECT_SET(s1.`ticker`) AS `tickers`,
    CONCAT('[', s1.`percentage_change`, ', ', s2.`percentage_change`, ', ', s3.`percentage_change`, ']') AS `changes`,
    CONCAT('[', s1.`year`, ', ', s2.`year`, ', ', s3.`year`, ']') AS `years`
FROM
    stock_percentage_changes s1
JOIN
    stock_percentage_changes s2 ON s1.`ticker` = s2.`ticker` AND s1.`year` + 1 = s2.`year`
JOIN
    stock_percentage_changes s3 ON s1.`ticker` = s3.`ticker` AND s1.`year` + 2 = s3.`year`
GROUP BY
    s1.`year`, s2.`year`, s3.`year`, s1.`percentage_change`, s2.`percentage_change`, s3.`percentage_change`
HAVING SIZE(COLLECT_SET(s1.`ticker`)) > 1;

-- for the output
INSERT OVERWRITE DIRECTORY './output'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT * FROM output;

-- Clean up temporary tables
DROP TABLE IF EXISTS stock_data_year;
DROP TABLE IF EXISTS stock_changes;
DROP TABLE IF EXISTS stock_percentage_changes;
DROP TABLE IF EXISTS stock_trends;



/*
tempo esecuzione:
LOCALE:
50%:  420 sec
100%: 668 sec
150%: 891 sec
200%: 1340 sec

AWS:
 50%: 120 sec
 100%: 133 sec
 150%: 142 sec
 200%: 171 sec
*/


