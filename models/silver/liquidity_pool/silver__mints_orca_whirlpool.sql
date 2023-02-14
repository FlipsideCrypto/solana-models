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

{% if is_incremental() %}
where _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)

{% else %}
where block_timestamp :: date >= '2022-03-10'
{% endif %}
),
base_whirlpool_events AS (
    SELECT
        *
    FROM
        {{ ref('silver__liquidity_pool_events_orca') }}
    WHERE
        program_id = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)

{% else %}
    AND block_timestamp :: date >= '2022-03-10'
{% endif %}
),
orca_mint_actions AS (
    SELECT
        m.*,
        m.mint_authority AS liquidity_pool_address,
        e.liquidity_provider,
        e.program_id
    FROM
        base_mint_actions m
        INNER JOIN base_whirlpool_events e
        ON e.tx_id = m.tx_id
    WHERE
        m.event_type = 'mintTo'
        AND m.mint_amount = 1
)
SELECT
    A.block_id,
    A.block_timestamp,
    A.tx_id,
    A.succeeded,
    A.index,
    A.inner_index,
    A.program_id,
    A.event_type AS action,
    A.mint,
    A.mint_amount AS amount,
    A.liquidity_provider,
    A.liquidity_pool_address,
    A._inserted_timestamp
FROM
    orca_mint_actions A
    INNER JOIN {{ ref('silver__initialization_pools_orca') }}
    b
    ON A.liquidity_pool_address = b.liquidity_pool
