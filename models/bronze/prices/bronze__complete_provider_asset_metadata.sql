{{ config (
    materialized = 'view'
) }}

SELECT
    asset_id,
    token_address,
    NAME,
    symbol,
    platform,
    platform_id,
    provider,
    source,
    _inserted_timestamp,
    inserted_timestamp,
    modified_timestamp,
    complete_provider_asset_metadata_id,
    _invocation_id
FROM
    {{ source(
        'crosschain_silver',
        'complete_provider_asset_metadata'
    ) }}
WHERE
    platform IN (
        'solana',
        'Solana'
    )
-- platforms specific to Solana