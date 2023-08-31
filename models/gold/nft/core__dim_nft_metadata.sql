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
    *
FROM
    {{ ref('nft__dim_nft_metadata') }}