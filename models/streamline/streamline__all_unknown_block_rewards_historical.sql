{{ config(
    materialized = 'view',
    tags = ['streamline'],
    enabled = false,
) }}

WITH pre_final AS (

    SELECT
        SEQ8()+99360012 AS block_id
    FROM
        TABLE(GENERATOR(rowcount => 80000000))
    WHERE
        block_id >= 99360012
        AND block_id <= 163728008
    EXCEPT
    SELECT
        block_id
    FROM
        {{ ref('streamline__complete_block_rewards') }}
    WHERE 
        _partition_id > 38754
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
