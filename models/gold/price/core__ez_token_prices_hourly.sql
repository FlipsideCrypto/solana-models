{{ config(
    materialized = 'view'
) }}

SELECT
    *
FROM
    {{ ref('price__ez_token_prices_hourly') }}
