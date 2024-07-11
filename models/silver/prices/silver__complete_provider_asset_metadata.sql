{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'complete_provider_asset_metadata_id',
    tags = ['scheduled_non_core']
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
    {{ ref(
        'bronze__complete_provider_asset_metadata'
    ) }}

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