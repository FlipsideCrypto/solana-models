{{ config(
    materialized = 'view',
    tags = ['scheduled_non_core']
) }}

SELECT
    token_address,
    token_name,
    symbol,
    decimals,
    coin_gecko_id,
    coin_market_cap_id,
    tags,
    logo,
    twitter,
    website,
    description,
    _INSERTED_TIMESTAMP,
    COALESCE (
        labels_id,
        {{ dbt_utils.generate_surrogate_key(
            ['token_address']
        ) }}
    ) AS dim_tokens_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__token_metadata') }}
