{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'complete_token_asset_metadata_id',
    tags = ['scheduled_non_core']
) }}

SELECT
    LOWER(
        A.token_address
    ) AS token_address,
    asset_id,
    symbol,
    NAME,
    decimals,
    blockchain,
    blockchain_name,
    blockchain_id,
    is_deprecated,
    provider,
    source,
    _inserted_timestamp,
    inserted_timestamp,
    modified_timestamp,
    complete_token_asset_metadata_id,
    _invocation_id
FROM
    {{ ref(
        'bronze__complete_token_asset_metadata'
    ) }} A

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(
                modified_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
