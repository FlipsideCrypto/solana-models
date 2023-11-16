{{ config(
  materialized = 'view',
  meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' }}},
  tags = ['scheduled_non_core']
) }}

SELECT
  a.mint,
  b.nft_collection_name,
  a.creator_address,
  a.metadata,
  a.image_url,
  a.metadata_uri,
  a.edition,
  a.nft_name,
  a.nft_metadata_id as dim_nft_metadata_id,
  a.inserted_timestamp,
  a.modified_timestamp
FROM
  {{ ref('silver__helius_nft_metadata') }} a
  left join {{ ref('silver__nft_collection') }} b
  on a.nft_collection_id = b.nft_collection_id
