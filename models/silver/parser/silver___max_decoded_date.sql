{{ config(
    materialized = 'view'
) }}

SELECT
    program_id,
    MAX(block_timestamp) AS block_timestamp
FROM
    {{ source(
        'bronze',
        'decoded_instructions'
    ) }}
GROUP BY
    block_timestamp
