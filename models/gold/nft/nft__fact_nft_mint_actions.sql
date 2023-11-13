{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' }}},
    tags = ['scheduled_non_core']
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    index,
    inner_index,
    event_type,
    mint,
    mint_amount,
    mint_authority,
    signers,
    DECIMAL,
    mint_standard_type
FROM
    {{ ref('silver__nft_mint_actions') }}

    

    
