{{ config(
    materialized='view',
    tags = ['scheduled_non_core']
  ) 
}}

SELECT
    program_id,
    max_block_id,
    min_block_id,
    first_block_id,
    status_historical_data,
    idl_id as dim_idl_id,
    inserted_timestamp,
    modified_timestamp
FROM {{ ref('silver__idl') }}