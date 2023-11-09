{{ config(
  materialized = 'view',
  meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'REWARDS' }}}
) }}

SELECT
    block_timestamp,
    block_id,
    vote_pubkey,
    epoch_earned,
    reward_amount_sol,
    post_balance_sol,
    fee_rewards_id as fact_fee_rewards_id,
    modified_timestamp,
    inserted_timestamp,
    epoch_id as dim_epoch_id
from
  {{ ref('silver__fee_rewards') }}
