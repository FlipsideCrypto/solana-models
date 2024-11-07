{{ config(
    materialized = 'view'
) }}

SELECT
    *
FROM
    {{ source(
        'solana_silver',
        'swaps_intermediate_jupiterv4'
    ) }}
