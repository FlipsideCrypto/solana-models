{{ config(
    materialized = 'view'
) }}

SELECT
    block_timestamp, 
    block_id, 
    tx_id, 
    succeeded, 
    program_id,
    purchaser,  
    mint_price,
    mint_currency, 
    NFT 
FROM 
    {{ ref('silver__nft_mints') }}