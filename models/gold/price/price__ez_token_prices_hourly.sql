{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'PRICES' }}},
    tags = ['scheduled_non_core']
) }}

SELECT
    b.date_hour AS recorded_hour,
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
    END AS is_imputed,
    COALESCE (
        token_prices_coin_gecko_hourly_id,
        token_prices_coin_market_cap_hourly_id,
        {{ dbt_utils.generate_surrogate_key(
            ['b.date_hour','token_address']
        ) }}
    ) AS ez_token_prices_hourly_id,
    GREATEST(COALESCE(cg.inserted_timestamp, '2000-01-01'), COALESCE(cmc.inserted_timestamp, '2000-01-01')) AS inserted_timestamp,
    GREATEST(COALESCE(cg.modified_timestamp, '2000-01-01'), COALESCE(cmc.modified_timestamp, '2000-01-01')) AS modified_timestamp
FROM
    {{ ref('silver__token_metadata') }} A
    CROSS JOIN {{ ref('silver__date_hours') }}
    b
    LEFT JOIN {{ ref('silver__token_prices_coin_gecko_hourly') }}
    cg
    ON A.coin_gecko_id = cg.id
    AND b.date_hour = cg.recorded_hour
    LEFT JOIN {{ ref('silver__token_prices_coin_market_cap_hourly') }}
    cmc
    ON A.coin_market_cap_id = cmc.id
    AND b.date_hour = cmc.recorded_hour
WHERE
    COALESCE(
        cg.imputed,
        cmc.imputed
    ) IS NOT NULL
