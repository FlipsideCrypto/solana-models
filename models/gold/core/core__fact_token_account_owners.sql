{{ config(
    materialized='view',
    tags = ['scheduled_non_core']
  ) 
}}

SELECT
    account_address,
    owner,
    start_block_id,
    end_block_id,
    COALESCE (
        token_account_owners_id,
        {{ dbt_utils.generate_surrogate_key(
            ['account_address','start_block_id']
        ) }}
    ) AS fact_token_account_owners_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM {{ ref('silver__token_account_owners') }}