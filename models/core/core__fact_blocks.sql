{{ config(
    materialized = 'view'
) }}

SELECT 
    block_id,
    block_timestamp, 
    network,
    chain_id,
    tx_count,
    block_height, 
    block_hash, 
    previous_block_id, 
    previous_block_hash
FROM
    {{ ref('silver__blocks') }}