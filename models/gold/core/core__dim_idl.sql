{{ config(
    materialized='view',
    tags = ['scheduled_non_core']
  ) 
}}

SELECT
    program_id,
    idl,
    idl_hash,
    earliest_decoded_block,
    idl_id as dim_idl_id,
    inserted_timestamp,
    modified_timestamp
FROM {{ ref('silver__idl') }}