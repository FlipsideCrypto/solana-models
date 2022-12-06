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
    CASE
        WHEN cg.imputed = FALSE THEN cg.close
        WHEN cmc.imputed = FALSE THEN cmc.close
        WHEN cg.imputed = TRUE THEN cg.close
        WHEN cmc.imputed = TRUE THEN cmc.close
    END AS CLOSE,
    CASE
        WHEN cg.imputed = FALSE THEN cg.imputed
        WHEN cmc.imputed = FALSE THEN cmc.imputed
        WHEN cg.imputed = TRUE THEN cg.imputed
        WHEN cmc.imputed = TRUE THEN cmc.imputed
    END AS is_imputed
FROM
    {{ ref('silver__token_metadata') }} A
    CROSS JOIN base b
    LEFT JOIN {{ ref('silver__token_prices_coin_gecko_hourly') }}
    cg
    ON A.coin_gecko_id = cg.id
    AND b.recorded_hour = cg.recorded_hour
    LEFT JOIN {{ ref('silver__token_prices_coin_market_cap_hourly') }}
    cmc
    ON A.coin_market_cap_id = cmc.id
    AND b.recorded_hour = cmc.recorded_hour
WHERE
    COALESCE(
        cg.imputed,
        cmc.imputed
    ) IS NOT NULL
