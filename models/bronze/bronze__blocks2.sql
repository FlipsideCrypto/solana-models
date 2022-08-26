{{ config (
    materialized = 'view'
) }}

SELECT 
    value, 
    _inserted_date, 
    metadata, 
    block_id, 
    data, 
    error 
FROM 
    {{ source(
        'bronze_prod', 
        'blocks_api'
    )}}