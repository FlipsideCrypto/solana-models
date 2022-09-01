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
    error,
    TO_TIMESTAMP_NTZ(
        SUBSTR(SPLIT_PART(metadata$filename, '/', 4), 1, 10) :: NUMBER,
        0
    ) as _inserted_timestamp
FROM 
    solana.bronze.block_txs_api