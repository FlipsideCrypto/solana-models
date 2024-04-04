{{ config(
    tags = ["test_daily"]
) }}

WITH most_recent_block AS (

    SELECT
        MAX(start_block_id) AS recent_block_id
    FROM
        {{ ref('silver__token_account_owners') }}
)
SELECT
    A.recent_block_id,
    b.block_id,
    b.block_timestamp
FROM
    most_recent_block A
    LEFT JOIN {{ ref('silver__blocks') }} b
    ON A.recent_block_id = b.block_id
WHERE
    b.block_timestamp <= (
        SYSDATE() - INTERVAL '12 HOUR'
    )
