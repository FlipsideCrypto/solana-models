{{ config(
    materialized = 'view'
) }}

SELECT
    *,
    '2000-01-01' as inserted_timestamp,
    '2000-01-01' AS modified_timestamp
FROM
    {{ source(
        'solana_silver',
        'swaps_intermediate_jupiterv4'
    ) }}
