{{ config(
    materialized = 'view',
    tags = ['scheduled_core']
) }}

SELECT 
    block_id,
    block_timestamp, 
    network,
    chain_id,
    block_height, 
    block_hash, 
    previous_block_id, 
    previous_block_hash
FROM
    {{ ref('silver__blocks') }}