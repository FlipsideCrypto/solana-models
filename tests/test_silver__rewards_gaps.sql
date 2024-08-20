{{
    config(
        tags=["test_hourly"]
    )
}}

WITH missing AS (
    SELECT 
        block_id
    FROM 
        solana.silver.blocks
    WHERE 
        block_timestamp < current_date - INTERVAL '12 HOUR'
    EXCEPT
    SELECT 
        block_id
    FROM 
        {{ ref('streamline__complete_block_rewards') }}
),
first_block_of_epoch AS (
    SELECT 
        b.block_id
    FROM 
        solana.silver.blocks b
    JOIN 
        solana.silver.epoch e
        ON b.block_id BETWEEN e.start_block AND e.end_block
    QUALIFY
        row_number() OVER (PARTITION BY e.epoch ORDER BY b.block_id) = 1
)
SELECT 
    m.*,
    f.block_id IS NOT NULL AS is_epoch_first_block
FROM 
    missing m
LEFT JOIN 
    first_block_of_epoch f
    USING(block_id)
