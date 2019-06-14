WITH
  double_entry_book AS (
                                SELECT
                                  to_address AS address,
                                  value AS value,
                                  block_timestamp
                                  -- debits
                                FROM
                                  `bigquery-public-data.crypto_ethereum.traces`
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
                                  `bigquery-public-data.crypto_ethereum.traces`
                                WHERE TRUE
                                  AND from_address IS NOT NULL
                                  AND status = 1
                                  AND (call_type NOT IN ('delegatecall', 'callcode','staticcall')
                                    OR call_type IS NULL)
                                UNION ALL
                                  -- transaction fees debits
                                SELECT
                                  miner AS address,
                                  SUM(CAST(receipt_gas_used AS numeric) * CAST(gas_price AS numeric)) AS value,
                                  timestamp AS block_timestamp
                                FROM
                                  `bigquery-public-data.crypto_ethereum.transactions` AS transactions
                                JOIN
                                  `bigquery-public-data.crypto_ethereum.blocks` AS blocks
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
                                  `bigquery-public-data.crypto_ethereum.transactions` 
                               ),
    
  double_entry_book_by_date AS (
                                SELECT
                                  DATE(block_timestamp) AS date,
                                  address,
                                  SUM(value * 0.00000001) AS value
                                FROM
                                  double_entry_book
                                GROUP BY
                                  address,
                                  date 
                              ),
  daily_balances_with_gaps AS (
                                SELECT
                                  address,
                                  date,
                                  SUM(value) OVER (PARTITION BY address ORDER BY date) AS balance,
                                  LEAD(date, 1, CURRENT_DATE()) OVER (PARTITION BY address ORDER BY date) AS next_date
                                FROM
                                  double_entry_book_by_date 
                             ),
  calendar AS (
                                SELECT
                                  date
                                FROM
                                  UNNEST(GENERATE_DATE_ARRAY('2015-07-30', CURRENT_DATE())) AS date 
              ),
              
  daily_balances AS (
                                SELECT
                                  address,
                                  calendar.date,
                                  balance
                                FROM
                                  daily_balances_with_gaps
                                JOIN
                                  calendar
                                ON
                                  daily_balances_with_gaps.date <= calendar.date
                                  AND calendar.date < daily_balances_with_gaps.next_date
                                WHERE
                                  balance > 1 
                    ),
                    
  address_counts AS (
                                SELECT
                                  date,
                                  COUNT(*) AS address_count
                                FROM
                                  daily_balances
                                GROUP BY
                                  date 
                    ),
                    
  daily_balances_sampled AS (
                                SELECT
                                  address,
                                  daily_balances.date,
                                  balance
                                FROM
                                  daily_balances
                                JOIN
                                  address_counts
                                ON
                                  daily_balances.date = address_counts.date
                                WHERE
                                  MOD(ABS(farm_fingerprint(address)), 100000000)/100000000 <= safe_divide(10000, address_count) 
                            ),
                            
  ranked_daily_balances AS (
                                SELECT
                                  date,
                                  balance,
                                  ROW_NUMBER() OVER (PARTITION BY date ORDER BY balance DESC) AS rank
                                FROM
                                  daily_balances_sampled 
                             )
    
    
SELECT
  date,
  1 - 2 * SUM((balance * (rank - 1) + balance / 2)) / COUNT(*) / SUM(balance) AS gini
FROM
  ranked_daily_balances
GROUP BY
  date
HAVING
  SUM(balance) > 0
ORDER BY
  date ASC;
