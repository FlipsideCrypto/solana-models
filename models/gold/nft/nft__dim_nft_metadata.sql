{{ config(
    materialized = 'incremental',
    unique_key = ['dim_nft_metadata_id'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}', '{{this.identifier}}', 'ON EQUALITY(mint, nft_name)'),
    full_refresh = false,
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' }}},
    tags = ['scheduled_non_core', 'exclude_change_tracking']
) }}

{% if execute %}
    {% if is_incremental() %}
    {% set max_modified_query %}
    SELECT
        MAX(modified_timestamp) AS modified_timestamp
    FROM
        {{ this }}
    {% endset %}
    {% set max_modified_timestamp = run_query(max_modified_query)[0][0] %}
    {% endif %}
{% endif %}

SELECT
  A.mint,
  b.nft_collection_name,
  b.collection_id,
  A.creators,
  A.authority,
  A.metadata,
  A.image_url,
  A.metadata_uri,
  A.nft_name,
  A.helius_nft_metadata_id AS dim_nft_metadata_id,
  sysdate() AS inserted_timestamp,
  sysdate() AS modified_timestamp
FROM
  {{ ref('silver__helius_nft_metadata') }} A
LEFT JOIN 
  {{ ref('silver__nft_collection_view') }} b
  ON A.nft_collection_id = b.nft_collection_id
{% if is_incremental() %}
WHERE
    a.modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
UNION ALL
SELECT
  mint,
  NULL AS nft_collection_name, -- collection data pipe is currently broken so these will be null until it's fixed
  NULL AS collection_id,
  creators,
  authority,
  metadata,
  image_url,
  metadata_uri,
  nft_name,
  helius_cnft_metadata_id AS dim_nft_metadata_id,
  sysdate() AS inserted_timestamp,
  sysdate() AS modified_timestamp
FROM
  {{ ref('silver__helius_cnft_metadata') }}
{% if is_incremental() %}
WHERE
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}