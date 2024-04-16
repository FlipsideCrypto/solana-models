{{ config(
    materialized='view',
    tags = ['scheduled_non_core']
  ) 
}}

SELECT
    program_id,
    idl,
    idl_hash,
    is_valid,
    submitted_by,
    _inserted_timestamp,
    first_block_id,
    earliest_decoded_block,
    backfill_status,
    idl_id as dim_idl_id,
    inserted_timestamp,
    modified_timestamp
FROM {{ ref('silver__idl') }}