
-- ******************************************************************************************************************************
WITH
  traces_in AS (  
                            SELECT *  
                            FROM  `bigquery-public-data.crypto_ethereum.traces`
                            WHERE   (DATE(block_timestamp) <= DATE_ADD('2015-07-30', INTERVAL 50 DAY ) )
            ),
                        
-- ******************************************************************************************************************************



ether_emitted_by_date AS (
  SELECT
    DATE(block_timestamp) AS date,
    SUM(value) AS value
  FROM
    traces_in
  WHERE TRUE
    AND ( trace_type IN ('genesis','reward') )
  GROUP BY
    DATE(block_timestamp) )

SELECT
  date,
  SUM(value) OVER (ORDER BY date) / power(10, 18) AS supply
FROM
  ether_emitted_by_date
