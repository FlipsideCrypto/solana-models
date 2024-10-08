{{ config(
  materialized = 'view',
  meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'REWARDS' }}},
  tags = ['scheduled_non_core']
) }}

{% set switchover_block_id = 292334107 %}

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
    {{ ref('silver__rewards_staking_view') }}
WHERE
    block_id <= {{ switchover_block_id }}
UNION ALL
SELECT
    block_timestamp,
    block_id,
    stake_pubkey,
    epoch_earned,
    reward_amount_sol,
    post_balance_sol,
    rewards_staking_2_id AS fact_rewards_staking_id,
    modified_timestamp,
    inserted_timestamp,
    epoch_id AS dim_epoch_id
FROM
    {{ ref('silver__rewards_staking_2') }}
WHERE
    block_id > {{ switchover_block_id }}