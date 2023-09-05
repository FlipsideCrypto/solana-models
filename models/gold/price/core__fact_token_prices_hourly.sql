{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'PRICES' }}}
) }}

SELECT
    *
FROM
    {{ ref('price__fact_token_prices_hourly') }}
