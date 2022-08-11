{{ config(
    materialized = 'incremental',
    unique_key = "block_id",
    cluster_by = ['block_timestamp::date'],
    tags = ['share']
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
FROM {{ref('core__fact_blocks')}}
where block_timestamp::date between '2021-12-01' and '2021-12-31'