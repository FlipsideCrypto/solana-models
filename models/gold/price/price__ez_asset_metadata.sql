{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'PRICE' }}},
    unique_key = ['ez_asset_metadata_id'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(token_address,symbol,NAME)'),
    tags = ['scheduled_non_core']
) }}

{% if execute %}

    {% if is_incremental() %}
        {% set query %}
            SELECT MAX(modified_timestamp) AS max_modified_timestamp
            FROM {{ this }}
        {% endset %}

        {% set max_modified_timestamp = run_query(query).columns[0].values()[0] %}
    {% endif %}
{% endif %}

SELECT
    token_address,
    asset_id,
    symbol,
    NAME,
    decimals,
    blockchain,
    FALSE AS is_native,
    is_deprecated,
    is_verified,
    is_verified_modified_timestamp,
    inserted_timestamp,
    modified_timestamp,
    complete_token_asset_metadata_id AS ez_asset_metadata_id
FROM
    {{ ref('silver__complete_token_asset_metadata') }}
{% if is_incremental() %}
WHERE
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
UNION ALL
SELECT
    NULL AS token_address,
    asset_id,
    symbol,
    NAME,
    decimals,
    blockchain,
    TRUE AS is_native,
    is_deprecated,
    TRUE as is_verified,
    NULL as is_verified_modified_timestamp,
    inserted_timestamp,
    modified_timestamp,
    complete_native_asset_metadata_id AS ez_asset_metadata_id
FROM
    {{ ref('silver__complete_native_asset_metadata') }}
{% if is_incremental() %}
WHERE
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
