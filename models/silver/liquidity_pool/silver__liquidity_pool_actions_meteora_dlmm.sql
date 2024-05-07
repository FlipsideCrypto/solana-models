-- depends_on: {{ ref('silver__decoded_instructions_combined') }}
{{ config(
    materialized = 'incremental',
    unique_key = "liquidity_pool_actions_meteora_dlmm_id",
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    tags = ['scheduled_non_core'],
) }}

{% if execute %}
    {% set base_query %}
    CREATE
    OR REPLACE temporary TABLE silver.liquidity_pool_actions_meteora_dlmm__intermediate_tmp AS

    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        INDEX,
        inner_index,
        program_id,
        event_type,
        decoded_instruction,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_instructions_combined') }}
    WHERE
        program_id = 'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo'
        AND event_type IN (
            'removeLiquidityByRange',
            'removeLiquidity',
            'removeAllLiquidity',
            'addLiquidityByStrategyOneSide',
            'addLiquidityOneSide',
            'addLiquidity',
            'addLiquidityByWeight',
            'addLiquidityByStrategy'
        )
        AND succeeded

        {% if is_incremental() %}
        AND _inserted_timestamp >= (
            SELECT
                MAX(_inserted_timestamp) - INTERVAL '1 hour'
            FROM
                {{ this }}
        )
        {% else %}
            AND _inserted_timestamp :: DATE >= '2024-02-19'
        {% endif %}

    {% endset %}
    {% do run_query(base_query) %}
    {% set between_stmts = fsc_utils.dynamic_range_predicate("silver.liquidity_pool_actions_meteora_dlmm__intermediate_tmp", "block_timestamp::date") %}
{% endif %}

WITH base AS (
    SELECT
        *
    FROM
        silver.liquidity_pool_actions_meteora_dlmm__intermediate_tmp
),
base_transfers AS (
    SELECT
        block_timestamp,
        tx_id,
        INDEX,
        mint,
        amount,
        dest_token_account,
        source_token_account
    FROM
        {{ ref('silver__transfers') }}
    WHERE
        {{ between_stmts }}
),
decoded AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        INDEX,
        inner_index,
        program_id,
        event_type AS action,
        silver.udf_get_account_pubkey_by_name(
            'sender',
            decoded_instruction :accounts
        ) AS liquidity_provider,
        silver.udf_get_account_pubkey_by_name(
            'lbPair',
            decoded_instruction :accounts
        ) AS liquidity_pool_address,
        COALESCE(
            silver.udf_get_account_pubkey_by_name(
                'reserveX',
                decoded_instruction :accounts
            ),
            silver.udf_get_account_pubkey_by_name(
                'reserve',
                decoded_instruction :accounts
            )
        ) AS liquidity_a_token_vault,
        silver.udf_get_account_pubkey_by_name(
            'reserveY',
            decoded_instruction :accounts
        ) AS liquidity_b_token_vault,
        _inserted_timestamp,
    FROM
        base
),
deposits AS (
    SELECT
        d.block_timestamp,
        d.block_id,
        d.tx_id,
        d.succeeded,
        d.index,
        SPLIT_PART(
            t.index,
            '.',
            2
        ) :: INT AS inner_index,
        d.program_id,
        d.action,
        t.mint,
        t.amount,
        d.liquidity_provider,
        d.liquidity_pool_address,
        d._inserted_timestamp
    FROM
        base_transfers t
        INNER JOIN decoded d
        ON t.block_timestamp :: DATE = d.block_timestamp :: DATE
        AND t.tx_id = d.tx_id
        AND SPLIT_PART(
            t.index,
            '.',
            1
        ) = d.index
    WHERE
        d.action IN (
            'addLiquidityByStrategyOneSide',
            'addLiquidityOneSide',
            'addLiquidity',
            'addLiquidityByWeight',
            'addLiquidityByStrategy'
        )
        AND dest_token_account IN (
            liquidity_a_token_vault,
            liquidity_b_token_vault
        )
),
withdraws AS (
    SELECT
        d.block_timestamp,
        d.block_id,
        d.tx_id,
        d.succeeded,
        d.index,
        SPLIT_PART(
            t.index,
            '.',
            2
        ) :: INT AS inner_index,
        d.program_id,
        d.action,
        t.mint,
        t.amount,
        d.liquidity_provider,
        d.liquidity_pool_address,
        d._inserted_timestamp
    FROM
        base_transfers t
        INNER JOIN decoded d
        ON t.block_timestamp :: DATE = d.block_timestamp :: DATE
        AND t.tx_id = d.tx_id
        AND SPLIT_PART(
            t.index,
            '.',
            1
        ) = d.index
    WHERE
        d.action IN (
            'removeLiquidityByRange',
            'removeLiquidity',
            'removeAllLiquidity'
        )
        AND source_token_account IN (
            liquidity_a_token_vault,
            liquidity_b_token_vault
        )
),
pre_final AS (
    SELECT
        *
    FROM
        deposits
    UNION ALL
    SELECT
        *
    FROM
        withdraws
)
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    INDEX,
    inner_index,
    program_id,
    action,
    mint,
    amount,
    liquidity_provider,
    liquidity_pool_address,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_id','index','inner_index']) }} AS liquidity_pool_actions_meteora_dlmm_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    pre_final
