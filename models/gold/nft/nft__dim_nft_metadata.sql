{{ config(
  materialized = 'view',
  meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' }}},
  tags = ['scheduled_non_core']
) }}

SELECT
  a.mint,
  b.nft_collection_name,
  b.collection_id,
  a.creator_address,
  a.metadata,
  a.image_url,
  a.metadata_uri,
  a.nft_name,
  a.helius_nft_metadata_id as dim_nft_metadata_id,
  a.inserted_timestamp,
  a.modified_timestamp,
  c.blockchain,
  c.contract_address,
  c.contract_name,
  c.created_at_timestamp,
  c.creator_name,
  c.image_url,
  c.project_name,
  c.token_id,
  c.token_metadata,
  c.token_metadata_uri,
  c.token_name
FROM
  {{ ref('silver__helius_nft_metadata') }} a
  left join {{ ref('silver__nft_collection') }} b
  on a.nft_collection_id = b.nft_collection_id
  left join {{ ref('silver__nft_metadata') }} c
  on a.mint = c.mint




