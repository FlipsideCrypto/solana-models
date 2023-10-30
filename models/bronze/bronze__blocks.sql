{{ config (
    materialized = 'view',
    enabled = false,
) }}

SELECT 
    record_id, 
    offset_id AS block_id, 
    block_id AS offset_id, 
    block_timestamp, 
    network, 
    chain_id, 
    tx_count, 
    header, 
    ingested_at,
    _inserted_timestamp
FROM 
    {{ source(
      'prod',
      'solana_blocks'
    ) }} 