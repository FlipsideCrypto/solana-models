{{ config(
  materialized = 'view',
  meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'REWARDS' }}},
  tags = ['scheduled_non_core']
) }}

SELECT
    block_timestamp,
    block_id,
    vote_pubkey,
    epoch_earned,
    reward_amount_sol,
    post_balance_sol,
    rewards_rent_id as fact_rewards_rent_id,
    modified_timestamp,
    inserted_timestamp,
    epoch_id as dim_epoch_id
from
  {{ ref('silver__rewards_rent_view') }}
