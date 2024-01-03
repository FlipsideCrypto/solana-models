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
    imputed,
    COALESCE (
        token_prices_coin_market_cap_hourly_id,
        {{ dbt_utils.generate_surrogate_key(
            ['recorded_hour', 'id']
        ) }}
    ) AS fact_token_prices_hourly_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__token_prices_coin_market_cap_hourly') }}
UNION
SELECT
    'coingecko' AS provider,
    recorded_hour,
    id,
    upper(symbol) as symbol,
    CLOSE,
    imputed,
    COALESCE (
        token_prices_coin_gecko_hourly_id,
        {{ dbt_utils.generate_surrogate_key(
            ['recorded_hour', 'id']
        ) }}
    ) AS fact_token_prices_hourly_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__token_prices_coin_gecko_hourly') }}
