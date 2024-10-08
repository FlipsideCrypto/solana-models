{{ config(
  materialized = 'view',
  meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'REWARDS' }}},
  tags = ['scheduled_non_core']
) }}

{% set switchover_block_id = 292334107 %}

SELECT
  block_timestamp,
  block_id,
  vote_pubkey,
  epoch_earned,
  reward_amount_sol,
  post_balance_sol,
  rewards_fee_id as fact_rewards_fee_id,
  modified_timestamp,
  inserted_timestamp,
  epoch_id as dim_epoch_id
FROM
  {{ ref('silver__rewards_fee_view') }}
WHERE
  block_id <= {{ switchover_block_id }}
UNION ALL
SELECT
  block_timestamp,
  block_id,
  vote_pubkey,
  epoch_earned,
  reward_amount_sol,
  post_balance_sol,
  rewards_fee_2_id as fact_rewards_fee_id,
  modified_timestamp,
  inserted_timestamp,
  epoch_id as dim_epoch_id
FROM
  {{ ref('silver__rewards_fee_2') }}
WHERE
  block_id > {{ switchover_block_id }}
