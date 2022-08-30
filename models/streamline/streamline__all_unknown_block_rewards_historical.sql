{{ config(
    materialized = 'view'
) }}

WITH pre_final AS (

    SELECT
        SEQ8()+98680445 AS block_id
    FROM
        TABLE(GENERATOR(rowcount => 60000000))
    WHERE
        block_id > 98680445
        AND block_id <= 148378013
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
