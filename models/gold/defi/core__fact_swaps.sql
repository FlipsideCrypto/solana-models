{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'SWAPS' }}}
) }}

SELECT
    *
FROM
    {{ ref('defi__fact_swaps') }}
