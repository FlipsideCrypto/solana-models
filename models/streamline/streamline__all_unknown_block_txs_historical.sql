{{ config(
    materialized = 'view'
) }}

WITH pre_final AS (

    SELECT
        SEQ8() AS block_id
    FROM
        TABLE(GENERATOR(rowcount => 1000000000))
    WHERE
        block_id <= 98680445
    EXCEPT
    SELECT
        block_id
    FROM
        {{ ref('streamline__complete_block_txs') }}
)
SELECT
    block_id,
    (
        SELECT
            coalesce(MAX(_partition_id) + 1,1)
        FROM
            {{ ref('streamline__complete_block_txs') }}
    ) AS batch_id
FROM
    pre_final
