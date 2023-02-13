{{ config(
    materialized = 'incremental',
    unique_key = ["block_id","tx_id"],
    incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp::date >= LEAST(current_date-7,(select min(block_timestamp)::date from ' ~ generate_tmp_view_name(this) ~ '))'],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE']
) }}

WITH base_mint_actions AS (

    SELECT
        *
    FROM
        {{ ref('silver__mint_actions') }}
    WHERE
        block_timestamp :: DATE >= '2021-02-14'
),

base_whirlpool_events AS (
    SELECT
        *
    FROM
        {{ ref('silver__liquidity_pool_events_orca') }}
    WHERE
        program_id IN (
            '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP',
            'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1'
        )
),
orca_mint_actions AS (
    SELECT
        m.*,
        COALESCE(
            e1.liquidity_provider,
            e2.liquidity_provider
        ) AS liquidity_provider
    FROM
        base_mint_actions m
        LEFT JOIN base_whirlpool_events e1
        ON m.tx_id = e1.tx_id
        AND m.index = e1.index
        AND e1.inner_index IS NULL
        LEFT JOIN base_whirlpool_events e2
        ON m.tx_id = e2.tx_id
        AND m.index = e2.index
        AND e2.inner_index IS NOT NULL
        AND m.inner_index BETWEEN e2.swap_program_inner_index_start
        AND e2.swap_program_inner_index_end
    WHERE
        m.event_type = 'mintTo'
        AND(
            e1.tx_id IS NOT NULL
            OR e2.tx_id IS NOT NULL
        )
)
SELECT
    A.block_id,
    A.block_timestamp,
    A.tx_id,
    A.succeeded,
    A.index,
    A.inner_index,
    b.owner AS program_id,
    A.event_type AS action,
    A.mint,
    COALESCE(A.mint_amount / pow(10, m.decimals), A.mint_amount) AS amount,
    A.liquidity_provider,
    b.liquidity_pool AS liquidity_pool_address,
    A._inserted_timestamp
FROM
    orca_mint_actions A
    INNER JOIN {{ ref('silver__initialization_pools_orca') }} b
    ON A.mint = b.pool_token
    LEFT JOIN {{ ref('silver__token_metadata') }} m
    ON A.mint = m.token_address
