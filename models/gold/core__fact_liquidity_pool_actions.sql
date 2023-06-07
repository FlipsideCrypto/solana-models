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
    _inserted_timestamp
FROM
    {{ ref('silver__liquidity_pool_actions_raydium') }}
UNION
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
    _inserted_timestamp
FROM
    {{ ref('silver__liquidity_pool_actions_orca') }}
UNION
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
    _inserted_timestamp
FROM
    {{ ref('silver__liquidity_pool_actions_saber') }}
