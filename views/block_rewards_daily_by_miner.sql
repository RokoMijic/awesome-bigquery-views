
-- *********************************************************************************************

WITH blocks_in AS (
                            SELECT * 
                            FROM  `bigquery-public-data.crypto_ethereum.blocks` 
                            WHERE   (DATE(timestamp      ) <= DATE_ADD('2015-07-30', INTERVAL 50 DAY ) )
                  )
               
-- *********************************************************************************************


SELECT
  miner,
  DATE(timestamp) AS date,
  COUNT(miner) AS total_block_reward
FROM
  blocks_in
GROUP BY
  miner,
  date
HAVING
  COUNT(miner) > 1
ORDER BY
  date
