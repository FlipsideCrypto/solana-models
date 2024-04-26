{{ config (
    materialized = 'view'
) }}

SELECT
    HOUR,
    asset_id,
    symbol,
    NAME,
    decimals,
    price,
    blockchain,
    is_imputed,
    is_deprecated,
    provider,
    source,
    _inserted_timestamp,
    inserted_timestamp,
    modified_timestamp,
    complete_native_prices_id,
    _invocation_id
FROM
    {{ source(
        'crosschain_silver',
        'complete_native_prices'
    ) }}
WHERE
    blockchain = 'solana'
    AND symbol = 'SOL'
