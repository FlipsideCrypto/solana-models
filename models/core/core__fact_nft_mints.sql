{{ config(
    materialized = 'view'
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
    mint 
FROM 
    {{ ref('silver__nft_mints') }}