
-- ***** BALANCE OF EVERY ETHEREUM ADDRESS ON EVERY DAY ****

-- ******************************************************************************************************************************
WITH

                    -- remove where clauses below to unlimit the time period (will use A LOT more data) 

  traces_in AS (  
                            SELECT *  
                            FROM  `bigquery-public-data.crypto_ethereum.traces`
                            WHERE   (DATE(block_timestamp) <= DATE_ADD('2015-07-30', INTERVAL 7 DAY ) )
            ),
                        
  blocks_in AS (
                            SELECT * 
                            FROM  `bigquery-public-data.crypto_ethereum.blocks` 
                            WHERE   (DATE(timestamp      ) <= DATE_ADD('2015-07-30', INTERVAL 7 DAY ) )
            ),

  transactions_in AS (
                            SELECT * 
                            FROM  `bigquery-public-data.crypto_ethereum.transactions` 
                            WHERE   (DATE(block_timestamp) <= DATE_ADD('2015-07-30', INTERVAL 7 DAY ) )
            ),
            
-- ******************************************************************************************************************************


  double_entry_book AS (
                                SELECT
                                  to_address AS address,
                                  value AS value,
                                  block_timestamp
                                  -- debits
                                FROM
                                  traces_in
                                WHERE TRUE 
                                  AND to_address IS NOT NULL
                                  AND status = 1
                                  AND (call_type NOT IN ('delegatecall','callcode','staticcall') OR call_type IS NULL)
                                UNION ALL
                                  -- credits
                                SELECT
                                  from_address AS address,
                                  -value AS value,
                                  block_timestamp
                                FROM
                                  traces_in
                                WHERE TRUE
                                  AND from_address IS NOT NULL
                                  AND status = 1
                                  AND (call_type NOT IN ('delegatecall', 'callcode','staticcall') OR call_type IS NULL)
                                UNION ALL
                                  -- transaction fees debits
                                SELECT
                                  miner AS address,
                                  SUM(CAST(receipt_gas_used AS numeric) * CAST(gas_price AS numeric)) AS value,
                                  timestamp AS block_timestamp
                                FROM
                                  transactions_in AS transactions
                                JOIN
                                  blocks_in AS blocks
                                ON
                                  blocks.number = transactions.block_number
                                  AND blocks.timestamp = transactions.block_timestamp
                                GROUP BY
                                  blocks.miner,
                                  block_timestamp
                                UNION ALL
                                  -- transaction fees credits
                                SELECT
                                  from_address AS address,
                                  -(CAST(receipt_gas_used AS numeric) * CAST(gas_price AS numeric)) AS value,
                                  block_timestamp
                                FROM
                                  transactions_in
                               )
    

SELECT
  address,
  SUM(value) * power(10, -18) AS balance
FROM
  double_entry_book
GROUP BY
  address
ORDER BY
  balance DESC








