{{ config(
    materialized = 'incremental',
    unique_key = ["block_id","tx_id","index","inner_index"],
    incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp::date >= LEAST(current_date-7,(select min(block_timestamp)::date from ' ~ generate_tmp_view_name(this) ~ '))'],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    tags = ['scheduled_non_core']
) }}

WITH base_raydium_pool_events AS (

    SELECT
        *
    FROM
        {{ ref('silver__liquidity_pool_events_raydium') }}

{% if is_incremental() %}
where _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)

{% else %}
    where block_timestamp :: date >= '2021-03-06'
{% endif %}
),
base_transfers AS (
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
                base_raydium_pool_events
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)

{% else %}
    AND block_timestamp :: date >= '2021-03-06'
{% endif %}
),
raydium_txfers AS (
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
        LEFT JOIN base_raydium_pool_events l1
        ON t.tx_id = l1.tx_id
        AND t.index = l1.index
        AND l1.inner_index = -1
        LEFT JOIN base_raydium_pool_events l2
        ON t.tx_id = l2.tx_id
        AND t.index = l2.index
        AND l2.inner_index <> -1
        AND t.inner_index BETWEEN l2.lp_program_inner_index_start
        AND l2.lp_program_inner_index_end
),
pre_final AS (
    SELECT
        t.*,
        COALESCE(
            p1.liquidity_pool,
            p2.liquidity_pool
        ) AS liquidity_pool_address
    FROM
        raydium_txfers t
        LEFT JOIN {{ ref('silver__initialization_pools_raydium') }}
        p1
        ON (
            t.dest_token_account = p1.token_a_account
            OR t.dest_token_account = p1.token_b_account
        )
        AND t.action = 'deposit'
        and p1.pool_token not in ('Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB','USDH1SM1ojwWUga67PGrgFWUHibbjqMvuMaDkRJTgkX')
        LEFT JOIN {{ ref('silver__initialization_pools_raydium') }}
        p2
        ON (
            t.source_token_account = p2.token_a_account
            OR t.source_token_account = p2.token_b_account
        )
        AND t.action = 'withdraw'
        and p2.pool_token not in ('Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB','USDH1SM1ojwWUga67PGrgFWUHibbjqMvuMaDkRJTgkX')
    WHERE
        p1.tx_id IS NOT NULL
        OR p2.tx_id IS NOT NULL
    qualify(row_number() over (partition by t.block_id, t.tx_id, t.index,t.inner_index order by t.index,t.inner_index)) = 1

)
SELECT
    block_id,
    block_timestamp,
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
    _inserted_timestamp
FROM
    pre_final
