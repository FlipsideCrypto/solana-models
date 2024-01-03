{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    cluster_by = ['block_timestamp::date'],
    tags = ['share']
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
    mint 
FROM {{ref('nft__fact_nft_mints')}}
where block_timestamp::date between '2021-12-01' and '2021-12-31'