{{ config(
    materialized = 'incremental',
    unique_key = "contract_address",
    cluster_by = ['contract_address'],
    tags = ['share']
) }}

  SELECT 
    blockchain,
    contract_address,
    contract_name,
    created_at_timestamp,
    mint,
    creator_address,
    creator_name,
    image_url,
    project_name,
    token_id,
    token_metadata,
    token_metadata_uri,
    token_name
FROM {{ref('core__dim_nft_metadata')}}