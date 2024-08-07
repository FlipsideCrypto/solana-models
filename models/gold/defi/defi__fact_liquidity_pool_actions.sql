{{ config(
    materialized = 'incremental',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STAKING' }}},
    unique_key = ['fact_liquidity_pool_actions_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','action','program_id'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(tx_id, liquidity_provider, liquidity_pool_address, mint)'),
    tags = ['scheduled_non_core']
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
    liquidity_pool_actions_raydium_id AS fact_liquidity_pool_actions_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__liquidity_pool_actions_raydium') }}
{% if is_incremental() %}
WHERE modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
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
    liquidity_pool_actions_orca_id as fact_liquidity_pool_actions_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__liquidity_pool_actions_orca') }}
{% if is_incremental() %}
WHERE modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
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
{% if is_incremental() %}
WHERE modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
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
{% if is_incremental() %}
WHERE modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
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
{% if is_incremental() %}
WHERE modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
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
{% if is_incremental() %}
WHERE modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}

