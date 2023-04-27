{{ config (
    materialized = 'view'
) }}

SELECT 
    data as json_data,
    TO_TIMESTAMP_NTZ(
    SUBSTR(SPLIT_PART(filename, '/', 3), 1, 10) :: NUMBER,
    0
  ) as _inserted_timestamp
FROM 
solana_dev.bronze.temp_streamline_validators_app_api