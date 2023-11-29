{{ config(
    materialized = 'view'
) }}

SELECT
    *
FROM
    {{ ref('silver__swaps_intermediate_jupiterv6') }}
