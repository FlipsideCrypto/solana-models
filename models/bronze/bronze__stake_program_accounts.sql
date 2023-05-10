{{ config (
    materialized = 'view'
) }}

SELECT 
    $1 as json_data,
    TO_TIMESTAMP_NTZ(
    SUBSTR(SPLIT_PART(METADATA$FILENAME, '/', 3), 1, 10) :: NUMBER,
    0
  ) as _inserted_timestamp
FROM 
    {{ source(
        'bronze_streamline', 
        'stake_program_accounts'
    )}}