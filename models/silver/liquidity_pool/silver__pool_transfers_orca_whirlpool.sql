{{ config(
    materialized = 'incremental',
    unique_key = ["block_id","tx_id"],
    incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp::date >= LEAST(current_date-7,(select min(block_timestamp)::date from ' ~ generate_tmp_view_name(this) ~ '))'],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE']
) }}

WITH base_transfers AS (

    SELECT
        t.block_id,
        t.block_timestamp,
        t.tx_id,
        COALESCE(SPLIT_PART(t.index :: text, '.', 1) :: INT, INDEX :: INT) AS INDEX,
        NULLIF(SPLIT_PART(t.index :: text, '.', 2), '') :: INT AS inner_index,
        t.tx_from,
        t.tx_to,
        t.source_token_account,
        t.dest_token_account,
        t.amount,
        t.mint,
        t.succeeded,
        t._inserted_timestamp
    FROM
        {{ ref('silver__transfers') }}
        t
    WHERE
        tx_id IN (
            SELECT
                tx_id
            FROM
                {{ ref('silver__liquidity_pool_events_orca') }}
        )

{% if is_incremental() %}
and _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)

{% else %}
and block_timestamp :: date >= '2022-03-10'
{% endif %}
),
whirlpool_txfers AS (
    SELECT
        t.*,
        COALESCE(
            l1.liquidity_provider,
            l2.liquidity_provider
        ) AS liquidity_provider,
        COALESCE(
            l1.program_id,
            l2.program_id
        ) AS program_id,
        COALESCE(
            l1.action,
            l2.action
        ) AS action
    FROM
        base_transfers t
        LEFT JOIN {{ ref('silver__liquidity_pool_events_orca') }}
        l1
        ON t.tx_id = l1.tx_id
        AND t.index = l1.index
        AND l1.inner_index IS NULL
        LEFT JOIN {{ ref('silver__liquidity_pool_events_orca') }}
        l2
        ON t.tx_id = l2.tx_id
        AND t.index = l2.index
        AND l2.inner_index IS NOT NULL
        AND t.inner_index BETWEEN l2.lp_program_inner_index_start
        AND l2.lp_program_inner_index_end
    WHERE
        l1.program_id = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc'
        OR l2.program_id = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc'
),
pre_final AS (
    SELECT
        t.*,
        COALESCE(
            p1.liquidity_pool,
            p2.liquidity_pool
        ) AS liquidity_pool_address,
        CASE
            WHEN t.action = 'whirlpool_fee_withdraw' THEN 'withdraw'
            WHEN t.action = 'whirlpool_unknown'
            AND p1.tx_id IS NOT NULL THEN 'deposit'
            WHEN t.action = 'whirlpool_unknown'
            AND p2.tx_id IS NOT NULL THEN 'withdraw'
        END AS action_true
    FROM
        whirlpool_txfers t
        LEFT JOIN {{ ref('silver__initialization_pools_orca') }}
        p1
        ON (
            t.dest_token_account = p1.token_a_account
            OR t.dest_token_account = p1.token_b_account
        )
        LEFT JOIN {{ ref('silver__initialization_pools_orca') }}
        p2
        ON (
            t.source_token_account = p2.token_a_account
            OR t.source_token_account = p2.token_b_account
        )
    WHERE
        p1.tx_id IS NOT NULL
        OR p2.tx_id IS NOT NULL
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    INDEX,
    inner_index,
    program_id,
    action_true AS action,
    mint,
    amount,
    liquidity_provider,
    liquidity_pool_address,
    _inserted_timestamp
FROM
    pre_final
