{{ config(
    materialized = 'view'
) }}

WITH base AS (

    SELECT
        DISTINCT recorded_hour
    FROM
        {{ ref('silver__token_prices_coin_gecko_hourly') }}
    UNION
    SELECT
        DISTINCT recorded_hour
    FROM
        {{ ref('silver__token_prices_coin_market_cap_hourly') }}
)
SELECT
    b.recorded_hour,
    token_address,
    token_name,
    A.symbol,
    decimals,
    cg.close close_coin_gecko,
    cmc.close close_coin_market_cap,
    cg.imputed imputed_coin_gecko,
    cmc.imputed imputed_coin_market_cap
FROM
    {{ ref('silver__token_metadata') }} A
    CROSS JOIN base b
    LEFT JOIN {{ ref('silver__token_prices_coin_gecko_hourly') }}
    cg
    ON A.coin_gecko_id = cg.id
    AND b.recorded_hour = cg.recorded_hour
    LEFT JOIN {{ ref('silver__token_prices_coin_market_cap_hourly') }}
    cmc
    ON A.coin_gecko_id = cmc.id
    AND b.recorded_hour = cmc.recorded_hour
WHERE
    COALESCE(
        cg.imputed,
        cmc.imputed
    ) IS NOT NULL
