{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STAKING' }}},
    tags = ["scheduled_non_core"],
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
    _inserted_timestamp,
    COALESCE (
        liquidity_pool_actions_raydium_id,
        {{ dbt_utils.generate_surrogate_key(
            ['BLOCK_ID','TX_ID','INDEX','INNER_INDEX']
        ) }}
    ) AS fact_liquidity_pool_actions_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
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
    _inserted_timestamp,
    COALESCE (
        liquidity_pool_actions_orca_id,
        {{ dbt_utils.generate_surrogate_key(
            ['BLOCK_ID','TX_ID','INDEX','INNER_INDEX']
        ) }}
    ) AS fact_liquidity_pool_actions_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
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
    _inserted_timestamp,
    COALESCE (
        liquidity_pool_actions_saber_id,
        {{ dbt_utils.generate_surrogate_key(
            ['BLOCK_ID','TX_ID','ACTION_INDEX']
        ) }}
    ) AS fact_liquidity_pool_actions_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__liquidity_pool_actions_saber') }}
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
    _inserted_timestamp,
    liquidity_pool_actions_meteora_id AS fact_liquidity_pool_actions_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__liquidity_pool_actions_meteora') }}
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
    _inserted_timestamp,
    liquidity_pool_actions_meteora_dlmm_id AS fact_liquidity_pool_actions_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__liquidity_pool_actions_meteora_dlmm') }}
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
    _inserted_timestamp,
    liquidity_pool_actions_meteora_multi_id AS fact_liquidity_pool_actions_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__liquidity_pool_actions_meteora_multi') }}

