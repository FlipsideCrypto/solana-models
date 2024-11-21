{{ config(
  materialized = 'view',
  meta = { 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' }} },
  tags = ['scheduled_non_core', 'exclude_change_tracking']
) }}

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
  A.inserted_timestamp,
  A.modified_timestamp
FROM
  {{ ref('silver__helius_nft_metadata') }} A
LEFT JOIN 
  {{ ref('silver__nft_collection_view') }} b
  ON A.nft_collection_id = b.nft_collection_id
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
  inserted_timestamp,
  modified_timestamp
FROM
  {{ ref('silver__helius_cnft_metadata') }}