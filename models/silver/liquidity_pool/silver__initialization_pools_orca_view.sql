
{{ config(
  materialized = 'view'
) }}

SELECT
    block_timestamp,
    block_id,
    tx_id,
    liquidity_pool,
    owner,
    mint_authority,
    token_a_account,
    token_b_account,
    pool_token,
    _inserted_timestamp
FROM
  {{ source(
    'solana_silver',
    'initialization_pools_orca'
  ) }}
