{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'PRICES' }}}
) }}

SELECT
    'coinmarketcap' AS provider,
    recorded_hour,
    id :: STRING AS id,
    symbol,
    CLOSE,
    imputed
FROM
    {{ ref('silver__token_prices_coin_market_cap_hourly') }}
UNION
SELECT
    'coingecko' AS provider,
    recorded_hour,
    id,
    symbol,
    CLOSE,
    imputed
FROM
    {{ ref('silver__token_prices_coin_market_cap_hourly') }}
