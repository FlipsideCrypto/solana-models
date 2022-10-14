WITH tmp AS (
    SELECT
        block_id,
        previous_block_id,
        block_timestamp
    FROM
        {{ ref('silver__blocks') }}
),
missing AS (
    SELECT
        t1.previous_block_id AS missing_block_id,
        t1.block_timestamp
    FROM
        tmp t1
        LEFT OUTER JOIN tmp t2
        ON t1.previous_block_id = t2.block_id
    WHERE
        t2.block_id IS NULL
        AND t1.previous_block_id IS NOT NULL
),
gaps AS (
    SELECT
        (
            SELECT
                MAX(block_id)
            FROM
                solana.silver.blocks
            WHERE
                block_id > 0
                AND block_id < missing_block_id
        ) AS gap_start_block_id,
        missing_block_id AS gap_end_block_id,
        gap_end_block_id - gap_start_block_id AS diff
    FROM
        missing
    WHERE
        block_timestamp :: DATE < CURRENT_DATE
)
SELECT
    *
FROM
    gaps
WHERE
    gap_end_block_id <> 1690556;-- this block is not available
