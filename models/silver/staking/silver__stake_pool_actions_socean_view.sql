{{ config(
  materialized = 'view'
) }}

SELECT 
    tx_id,
    block_id,
    block_timestamp,
    index,
    succeeded,
    action,
    stake_pool,
    stake_pool_withdraw_authority,
    stake_pool_deposit_authority,
    address,
    reserve_stake_address,
    amount,
    _inserted_timestamp,
    _unique_key,
    stake_pool_actions_socean_id,
    inserted_timestamp,
    modified_timestamp,
    _invocation_id
FROM
  {{ source(
    'solana_silver',
    'stake_pool_actions_socean'
  ) }}