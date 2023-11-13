{{ config(
    materialized = 'view',
    tags = ['streamline'],
) }}

WITH pre_final AS (

    SELECT
        SEQ8()+148693779 AS block_id
    FROM
        TABLE(GENERATOR(rowcount => 80000000))
    WHERE
        block_id >= 148693779
        AND block_id <= 226117675
    EXCEPT
    SELECT
        block_id
    FROM
        {{ ref('streamline__complete_block_rewards') }}
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
