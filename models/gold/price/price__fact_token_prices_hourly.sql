{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'PRICES' }}},
    tags = ['scheduled_non_core']
) }}

SELECT
    'coinmarketcap' AS provider,
    recorded_hour,
    id :: STRING AS id,
    upper(symbol) as symbol,
    CLOSE,
    imputed
FROM
    {{ ref('silver__token_prices_coin_market_cap_hourly') }}
UNION
SELECT
    'coingecko' AS provider,
    recorded_hour,
    id,
    upper(symbol) as symbol,
    CLOSE,
    imputed
FROM
    {{ ref('silver__token_prices_coin_gecko_hourly') }}
