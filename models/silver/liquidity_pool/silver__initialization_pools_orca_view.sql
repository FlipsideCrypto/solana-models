
{{ config(
  materialized = 'view'
) }}

SELECT
    block_timestamp,
    block_id,
    tx_id,
    instruction :accounts [4] :: STRING AS liquidity_pool,
    'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc' AS owner,
    liquidity_pool AS mint_authority,
    instruction :accounts [5] :: STRING AS token_a_account,
    instruction :accounts [6] :: STRING AS token_b_account,
    NULL AS pool_token,
    _inserted_timestamp
FROM
  {{ source(
    'solana_silver',
    'initialization_pools_orca'
  ) }}
