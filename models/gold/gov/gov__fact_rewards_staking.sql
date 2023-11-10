{{ config(
  materialized = 'view',
  meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'REWARDS' }}}
) }}

SELECT
    block_timestamp,
    block_id,
    stake_pubkey,
    epoch_earned,
    reward_amount_sol,
    post_balance_sol,
    rewards_staking_id as fact_rewards_staking_id,
    modified_timestamp,
    inserted_timestamp,
    epoch_id as dim_epoch_id
from
  {{ ref('silver__rewards_staking') }}
