{{ 
    config(
      materialized='view',
      meta={
        'database_tags':{
            'table': {
                'PURPOSE': 'NFT'
            }
        }
      }
    ) 
}}

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
FROM {{ ref('silver__nft_metadata') }}