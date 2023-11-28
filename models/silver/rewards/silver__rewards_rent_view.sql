{{ config(
  materialized = 'view'
) }}

SELECT
  block_timestamp,
  block_id,
  reward_amount_sol,
  post_balance_sol,
  vote_pubkey,
  epoch_earned,
  _partition_id,
  rewards_rent_id,
  epoch_id,
  inserted_timestamp,
  modified_timestamp,
  invocation_id,
  _inserted_timestamp
FROM
  {{ source(
    'solana_silver',
    'rewards_rent'
  ) }}