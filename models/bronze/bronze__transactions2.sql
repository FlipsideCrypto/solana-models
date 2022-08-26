{{ config (
    materialized = 'view'
) }}

SELECT 
    value, 
    _partition_id, 
    metadata, 
    block_id, 
    tx_id, 
    data, 
    error
FROM 
    {{ source(
        'bronze_prod', 
        'block_txs_api'
    )}}