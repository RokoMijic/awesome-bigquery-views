SELECT
  miner,
  DATE(timestamp) AS date,
  COUNT(miner) AS total_block_reward
FROM
  `bigquery-public-data.crypto_ethereum.blocks` 
WHERE TRUE
  AND (DATE(timestamp) >= DATE_ADD(CURRENT_DATE(), INTERVAL -7 DAY ) )
GROUP BY
  miner,
  date
HAVING
  COUNT(miner) > 1
ORDER BY
  date
