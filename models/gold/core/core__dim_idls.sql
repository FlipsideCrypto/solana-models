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
    date_submitted,
    first_block_id,
    earliest_decoded_block,
    backfill_status,
    idls_id as dim_idls_id,
    inserted_timestamp,
    modified_timestamp
FROM {{ ref('silver__idls') }}