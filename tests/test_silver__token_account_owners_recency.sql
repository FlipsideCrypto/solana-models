{{ config(
    tags = ["test_daily"]
) }}

WITH most_recent_block AS (

    SELECT
        MAX(start_block_id) AS recent_block_id
    FROM
        solana.silver.token_account_owners
)
SELECT
    A.recent_block_id,
    b.block_id,
    b.block_timestamp
FROM
    most_recent_block A
    LEFT JOIN solana.silver.blocks b
    ON A.recent_block_id = b.block_id
WHERE
    b.block_timestamp <= (
        CURRENT_TIMESTAMP - INTERVAL '12 HOUR'
    )
