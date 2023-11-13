{{ config(
  materialized = 'view',
  meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STAKING' }}},
  tags = ['scheduled_non_core']
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
FROM
    {{ ref('silver__staking_lp_actions') }}
