{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    token_address,
    asset_id AS id, -- id column pending deprecation
    asset_id,
    A.symbol,
    A.name,
    C.decimals, -- decimals column pending deprecation
    platform AS blockchain,
    platform_id AS blockchain_id,
    provider,
    A.inserted_timestamp,
    A.modified_timestamp,
    A.complete_provider_asset_metadata_id AS dim_asset_metadata_id
FROM
    {{ ref('silver__complete_provider_asset_metadata') }} A
LEFT JOIN {{ ref('core__dim_tokens') }} C --remove this join alongside decimal column deprecation
    ON C.token_address = A.token_address
