{{ config(
    materialized = 'incremental',
    unique_key = ["block_id","tx_id","index","inner_index"],
    incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp::date >= LEAST(current_date-7,(select min(block_timestamp)::date from ' ~ generate_tmp_view_name(this) ~ '))'],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE']
) }}

WITH base_mint_actions AS (

    SELECT
        *
    FROM
        {{ ref('silver__mint_actions') }}

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
base_raydium_events AS (
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
raydium_mint_actions AS (
    SELECT
        m.*,
        COALESCE(
            e1.liquidity_provider,
            e2.liquidity_provider
        ) AS liquidity_provider
    FROM
        base_mint_actions m
        LEFT JOIN base_raydium_events e1
        ON m.tx_id = e1.tx_id
        AND m.index = e1.index
        AND e1.inner_index = -1
        LEFT JOIN base_raydium_events e2
        ON m.tx_id = e2.tx_id
        AND m.index = e2.index
        AND e2.inner_index <> -1
        AND m.inner_index BETWEEN e2.lp_program_inner_index_start
        AND e2.lp_program_inner_index_end
    WHERE
        m.event_type = 'mintTo'
        AND(
            e1.tx_id IS NOT NULL
            OR e2.tx_id IS NOT NULL
        )
),
pre_final_raydium_mints AS(
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
        A.mint_amount,
        A.liquidity_provider,
        b.liquidity_pool AS liquidity_pool_address,
        A._inserted_timestamp
    FROM
        raydium_mint_actions A
        INNER JOIN {{ ref('silver__initialization_pools_raydium') }}
        b
        ON A.mint = b.pool_token
),
-- mints in swaps aren't captured in 'liqudity_pool_events' so they are accounted for here
mints_in_swaps AS(
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
        A.mint_amount,
        A.mint_authority AS liquidity_provider,
        b.liquidity_pool AS liquidity_pool_address,
        A._inserted_timestamp
    FROM
        base_mint_actions A
        INNER JOIN {{ ref('silver__initialization_pools_raydium') }}
        b
        ON A.mint = b.pool_token
    WHERE
        A.tx_id NOT IN (
            SELECT
                tx_id
            FROM
                pre_final_raydium_mints
        )
)
SELECT
    A.block_id,
    A.block_timestamp,
    A.tx_id,
    A.succeeded,
    A.index,
    A.inner_index,
    A.program_id,
    A.action,
    A.mint,
    COALESCE(A.mint_amount / pow(10, m.decimals), A.mint_amount) AS amount,
    A.liquidity_provider,
    A.liquidity_pool_address,
    A._inserted_timestamp
FROM
    pre_final_raydium_mints A
    LEFT JOIN {{ ref('silver__token_metadata') }}
    m
    ON A.mint = m.token_address
    where liquidity_provider is not null and amount is not null
UNION
SELECT
    A.block_id,
    A.block_timestamp,
    A.tx_id,
    A.succeeded,
    A.index,
    A.inner_index,
    A.program_id,
    A.action,
    A.mint,
    COALESCE(A.mint_amount / pow(10, m.decimals), A.mint_amount) AS amount,
    A.liquidity_provider,
    A.liquidity_pool_address,
    A._inserted_timestamp
FROM
    mints_in_swaps A
    LEFT JOIN {{ ref('silver__token_metadata') }}
    m
    ON A.mint = m.token_address
    where liquidity_provider is not null and amount is not null
