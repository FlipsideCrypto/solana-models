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
    rent_rewards_id as fact_rent_rewards_id,
    modified_timestamp,
    inserted_timestamp,
    epoch_id as dim_epoch_id
from
  {{ ref('silver__rent_rewards') }}
