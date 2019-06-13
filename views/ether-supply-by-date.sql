with ether_emitted_by_date AS (
  SELECT
    DATE(block_timestamp) AS date,
    SUM(value) AS value
  FROM
    `bigquery-public-data.ethereum_blockchain.traces`
  WHERE TRUE
    AND ( trace_type IN ('genesis','reward') )
    AND ( DATE(block_timestamp) >= DATE_ADD(CURRENT_DATE(), INTERVAL -7 DAY ) )
  GROUP BY
    DATE(block_timestamp) )

SELECT
  date,
  SUM(value) OVER (ORDER BY date) / power(10, 18) AS supply
FROM
  ether_emitted_by_date
