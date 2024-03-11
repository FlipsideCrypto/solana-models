{{ config(
    materialized = 'view',
    tags = ['streamline'],
) }}

WITH pre_final AS (

    SELECT
    --     SEQ8()+99360012 AS block_id
    -- FROM
    --     TABLE(GENERATOR(rowcount => 80000000))
    -- WHERE
    --     block_id >= 99360012
    --     AND block_id <= 163728008
        block_id
    FROM
        solana_dev.silver.backfill_rewards_blocks_240308
    EXCEPT
    SELECT
        block_id
    FROM
        {{ ref('streamline__complete_block_rewards') }}
    WHERE 
        _partition_id > 41169
)
SELECT
    block_id,
    (
        SELECT
            coalesce(MAX(_partition_id) + 1,1)
        FROM
            {{ ref('streamline__complete_block_rewards') }}
    ) AS batch_id
FROM
    pre_final
ORDER BY block_id
LIMIT 1