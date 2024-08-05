{{
    config(
        tags=["test_hourly"]
    )
}}

WITH block_txs_real_time_task_counts AS (
    SELECT 
        count(*) AS success_count
    FROM 
        TABLE(solana.information_schema.task_history(
            scheduled_time_range_start=>dateadd('hour',-1,current_timestamp()),
            task_name=>'BULK_GET_BLOCK_TXS_REAL_TIME'))
    WHERE
        state = 'SUCCEEDED'
),
block_rewards_real_time_task_counts AS (
    SELECT 
        count(*) AS success_count
    FROM 
        TABLE(solana.information_schema.task_history(
            scheduled_time_range_start=>dateadd('hour',-1,current_timestamp()),
            task_name=>'BULK_GET_BLOCK_REWARDS_REAL_TIME'))
    WHERE
        state = 'SUCCEEDED'
),
blocks_real_time_task_counts AS (
    SELECT 
        count(*) AS success_count
    FROM 
        TABLE(solana.information_schema.task_history(
            scheduled_time_range_start=>dateadd('hour',-1,current_timestamp()),
            task_name=>'BULK_GET_BLOCKS_REAL_TIME'))
    WHERE
        state = 'SUCCEEDED'
)
SELECT
    'block_txs_real_time' AS pipeline,
    success_count
FROM
    block_txs_real_time_task_counts
WHERE
    success_count < 7
UNION ALL
SELECT
    'blocks_real_time' AS pipeline,
    success_count
FROM
    blocks_real_time_task_counts
WHERE
    success_count < 10
UNION ALL
SELECT
    'block_rewards_real_time' AS pipeline,
    success_count
FROM
    block_rewards_real_time_task_counts
WHERE
    success_count < 3