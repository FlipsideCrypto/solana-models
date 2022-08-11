{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    cluster_by = ['block_timestamp::date'],
    tags = ['share']
) }}

  SELECT 
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    event_type,
    program_id,
    signers,
    account_keys,
    instruction,
    inner_instruction,
    pre_balances,
    post_balances,
    pre_token_balances,
    post_token_balances
FROM {{ref('core__fact_staking_lp_actions')}}
where block_timestamp::date between '2021-12-01' and '2021-12-31'