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
)
SELECT
    'block_txs_real_time' AS pipeline,
    success_count
FROM
    block_txs_real_time_task_counts
WHERE
    success_count < 7