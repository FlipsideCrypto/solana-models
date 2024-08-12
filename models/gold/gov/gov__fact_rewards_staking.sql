{{ config(
  materialized = 'view',
  post_hook = 'ALTER VIEW {{this}} SET CHANGE_TRACKING = TRUE;',
  meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'REWARDS' }} },
  tags = ['scheduled_non_core']
) }}

SELECT
  block_timestamp,
  block_id,
  stake_pubkey,
  epoch_earned,
  reward_amount_sol,
  post_balance_sol,
  rewards_staking_id AS fact_rewards_staking_id,
  modified_timestamp,
  inserted_timestamp,
  epoch_id AS dim_epoch_id
FROM
  {{ ref('silver__rewards_staking') }}
