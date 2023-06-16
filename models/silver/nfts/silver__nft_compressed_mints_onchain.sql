{{ config(
    materialized = 'incremental',
    unique_key = "mint",
    incremental_strategy = 'delete+insert',
) }}

with bgum_mints as (

  select 

    block_timestamp,
    tx_id,
    instruction :accounts[1] ::string as leaf_owner,
    instruction :accounts[8] ::string as collection_mint,
    signers[0]::string as creator_address

  from solana.silver.events e
  inner join solana.silver.transactions txs
    using(tx_id, block_timestamp, succeeded)
  where succeeded
    and program_id = 'BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY'
    and array_contains('Program log: Instruction: MintToCollectionV1' ::variant, log_messages)
    -- and block_timestamp > '2023-02-07'
    -- and block_timestamp::date = '2023-04-27'
{% if is_incremental() %}
AND e._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
and tx._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)

{% else %}
    AND block_timestamp :: date >= '2023-02-07'
{% endif %}

  union all

  select 
  
    block_timestamp,
    tx_id,
    f.value :accounts[1] ::string as leaf_owner,
    f.value :accounts[8] ::string as collection_mint,
    signers[0]::string as creator_address
  
  from solana.silver.events e
  inner join lateral flatten (input => inner_instruction :instructions) f
  inner join solana.silver.transactions txs
    using(tx_id, block_timestamp, succeeded)
  where succeeded
    and program_id = '1atrmQs3eq1N2FEYWu6tyTXbCjP4uQwExpjtnhXtS8h' -- lazy_transactions
    and f.value :programId = 'BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY' -- bubblegum
    -- and block_timestamp > '2023-02-07'
    -- and block_timestamp::date = '2023-04-27'
{% if is_incremental() %}
AND e._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
and tx._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)

{% else %}
    AND block_timestamp :: date >= '2023-02-07'
{% endif %}
)

select *
from bgum_mints
