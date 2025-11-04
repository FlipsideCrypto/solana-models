{{ config(
  materialized = 'view'
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    program_id,
    action,
    liquidity_provider,
    liquidity_pool_address,
    amount,
    mint,
    action_index,
    _inserted_timestamp,
    liquidity_pool_actions_saber_id,
    inserted_timestamp,
    modified_timestamp
FROM
  {{ source(
    'solana_silver',
    'liquidity_pool_actions_saber'
  ) }}