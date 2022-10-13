WITH tmp AS (

    SELECT
        block_id,
        previous_block_id
    FROM
        {{ ref('silver__blocks') }}
    WHERE 
        _inserted_date < current_date 
    AND 
        block_id >= 130000000
),
missing AS (
    SELECT
        t1.previous_block_id AS missing_block_id
    FROM
        tmp t1
        LEFT OUTER JOIN tmp t2
        ON t1.previous_block_id = t2.block_id
    WHERE
        t2.block_id IS NULL
        AND t1.previous_block_id IS NOT NULL
)
SELECT
    *
FROM
    missing
