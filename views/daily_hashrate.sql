-- ***** DAILY HASHRATE in GHASH/SEC ****

-- ******************************************************************************************************************************
WITH
  blocks_in AS (
                            SELECT * 
                            FROM  `bigquery-public-data.crypto_ethereum.blocks` 
                            WHERE   (DATE(timestamp      ) <= DATE_ADD('2015-07-30', INTERVAL 7 DAY ) )
            ),
-- ******************************************************************************************************************************


  block_rows AS (
                        SELECT
                          *,
                          ROW_NUMBER() OVER (ORDER BY timestamp) AS rn
                        FROM
                          blocks_in
                 ),
    
  delta_time AS (
                        SELECT
                          mp.timestamp AS block_time,
                          mp.difficulty AS difficulty,
                          TIMESTAMP_DIFF(mp.timestamp, mc.timestamp, SECOND) AS delta_block_time
                        FROM
                          block_rows mc
                        JOIN
                          block_rows mp
                        ON
                          mc.rn = mp.rn - 1 
               ),
               
  hashrate_book AS (
                        SELECT
                          TIMESTAMP_TRUNC(block_time, DAY) AS block_day,
                          AVG(delta_block_time) AS daily_avg_block_time,
                          AVG(difficulty) AS daily_avg_difficulty
                        FROM
                          delta_time
                        GROUP BY
                          TIMESTAMP_TRUNC(block_time, DAY) 
                   )
SELECT
  DATE(block_day),
  (daily_avg_difficulty/daily_avg_block_time)*POWER(10,-9) AS hashrate
FROM
  hashrate_book
ORDER BY
  block_day ASC
