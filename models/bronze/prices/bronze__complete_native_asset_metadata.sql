{{ config (
    materialized = 'view'
) }}

SELECT
    asset_id,
    symbol,
    NAME,
    decimals,
    blockchain,
    is_deprecated,
    provider,
    source,
    _inserted_timestamp,
    inserted_timestamp,
    modified_timestamp,
    complete_native_asset_metadata_id,
    _invocation_id
FROM
    {{ source(
        'silver_crosschain',
        'complete_native_asset_metadata'
    ) }}
WHERE
    blockchain = 'solana'
    AND symbol = 'SOL'
