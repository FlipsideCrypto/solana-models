{{ config(
    materialized='view',
    tags = ['daily']
  ) 
}}

SELECT
    account_address,
    owner,
    start_block_id,
    end_block_id,
    token_account_owners_id AS fact_token_account_owners_id,
    inserted_timestamp,
    modified_timestamp
FROM {{ ref('silver__token_account_owners') }}