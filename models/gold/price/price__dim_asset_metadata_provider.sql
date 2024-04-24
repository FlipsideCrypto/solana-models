{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    token_address,
    asset_id,
    symbol,
    NAME,
    platform AS blockchain,
    platform_id AS blockchain_id,
    provider,
    inserted_timestamp,
    modified_timestamp,
    complete_provider_asset_metadata_id AS dim_asset_metadata_provider_id
FROM
    {{ ref('silver__complete_provider_asset_metadata') }}
