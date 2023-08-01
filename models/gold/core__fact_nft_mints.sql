{{ config(
    materialized = 'view',
    meta={
        'database_tags':{
            'table': {
                'PURPOSE': 'NFT'
            }
        }
    }
) }}

SELECT
    block_timestamp, 
    block_id, 
    initialization_tx_id as tx_id, 
    succeeded, 
    program_id,
    purchaser,  
    mint_price,
    mint_currency, 
    mint,
    FALSE as is_compressed
FROM 
    {{ ref('silver__nft_mints') }}
union all
SELECT
    block_timestamp, 
    block_id, 
    tx_id, 
    succeeded, 
    program_id,
    purchaser,  
    mint_price,
    mint_currency, 
    mint,
    TRUE as is_compressed
FROM 
    {{ ref('silver__nft_compressed_mints') }}