{{ config (
    materialized = 'view'
) }}

SELECT 
    value, 
    _inserted_date, 
    metadata, 
    block_id, 
    data, 
    error,
    TO_TIMESTAMP_NTZ(
        SUBSTR(SPLIT_PART(metadata$filename, '/', 4), 1, 10) :: NUMBER,
        0
    ) as _inserted_timestamp
FROM 
    {{ source(
        'bronze_streamline', 
        'blocks_api'
    )}}