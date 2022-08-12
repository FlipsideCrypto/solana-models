{{ config(
    materialized = 'view'
) }}

SELECT
    SEQ8() AS block_id
FROM
    TABLE(GENERATOR(rowcount => 1000000000))
WHERE
    block_id <= (
        SELECT
            MIN(block_id)
        FROM
            {{ source(
                'solana_external',
                'blocks_api'
            ) }}
        WHERE
            _inserted_date >= CURRENT_DATE
    )
