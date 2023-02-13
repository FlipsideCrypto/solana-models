{{ config(
    materialized = 'incremental',
    unique_key = ["block_id","tx_id"],
    incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp::date >= LEAST(current_date-7,(select min(block_timestamp)::date from ' ~ generate_tmp_view_name(this) ~ '))'],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE']
) }}
-- Get whirlpool LP burns by finding existing whirlpool mints
WITH base_burn_actions AS (

    SELECT
        *
    FROM
        {{ ref('silver__burn_actions') }}
    WHERE
        block_timestamp :: DATE >= '2022-03-10'
),
base_whirlpool_mints AS (
    SELECT
        *
    FROM
        {{ ref('silver__mints_orca_whirlpool') }}
)
SELECT
    b.block_id,
    b.block_timestamp,
    b.tx_id,
    b.succeeded,
    b.index,
    b.inner_index,
    m.program_id,
    b.event_type AS action,
    b.mint,
    b.burn_amount AS amount,
    b.burn_authority AS liquidity_provider,
    m.liquidity_pool_address,
    b._inserted_timestamp
FROM
    base_burn_actions b
    INNER JOIN base_whirlpool_mints m
    ON b.mint = m.mint