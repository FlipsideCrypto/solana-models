{{ config(
  materialized = 'view'
) }}

SELECT
    block_timestamp,
    block_id,
    vote_pubkey,
    epoch_earned,
    reward_amount_sol,
    post_balance_sol,
    voting_rewards_id as fact_voting_rewards_id,
    modified_timestamp,
    inserted_timestamp,
    epoch_id as dim_epoch_id
from
  {{ ref('silver__voting_rewards') }}
