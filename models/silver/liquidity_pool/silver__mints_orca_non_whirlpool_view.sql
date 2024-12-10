{{ config(
  materialized = 'view'
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    index,
    inner_index,
    program_id,
    action,
    mint,
    amount,
    liquidity_provider,
    liquidity_pool_address,
    _inserted_timestamp
FROM
  {{ source(
    'solana_silver',
    'mints_orca_non_whirlpool'
  ) }}
