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
    staking_rewards_id as fact_staking_rewards_id,
    modified_timestamp,
    inserted_timestamp,
    epoch_id as dim_epoch_id
from
  {{ ref('silver__staking_rewards') }}
