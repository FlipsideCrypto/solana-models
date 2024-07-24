{{ config(
    materialized = 'incremental',
    unique_key = "liquidity_pool_actions_raydium_id",
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
    tags = ['scheduled_non_core'],
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
    Amount,
    liquidity_provider,
    liquidity_pool_address,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['TX_ID','INDEX','INNER_INDEX']) }} AS liquidity_pool_actions_raydium_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM 
    {{ ref('silver__burns_raydium') }}
{% if is_incremental() %}
WHERE _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
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
    index,
    inner_index,
    program_id,
    action,
    mint,
    amount,
    liquidity_provider,
    liquidity_pool_address,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['TX_ID','INDEX','INNER_INDEX']) }} AS liquidity_pool_actions_raydium_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM 
    {{ ref('silver__mints_raydium') }}
{% if is_incremental() %}
WHERE _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
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
    index,
    inner_index,
    program_id,
    action,
    mint,
    Amount,
    liquidity_provider,
    liquidity_pool_address,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['TX_ID','INDEX','INNER_INDEX']) }} AS liquidity_pool_actions_raydium_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM 
    {{ ref('silver__pool_transfers_raydium') }}
{% if is_incremental() %}
WHERE _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}