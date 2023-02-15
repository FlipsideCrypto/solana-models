{{ config(
    materialized = 'incremental',
    unique_key = ["block_id","tx_id","index","inner_index"],
    incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp::date >= LEAST(current_date-7,(select min(block_timestamp)::date from ' ~ generate_tmp_view_name(this) ~ '))'],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE']
) }}

WITH base_events AS(

    SELECT
        *
    FROM
        {{ ref('silver__events') }}
    WHERE
        (
            program_id IN (
                '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
                '27haf8L6oxUeXrHrgEgsexjSY5hbVUWEmvv9Nyxg8vQv',
                '5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h'
            )
            OR ARRAY_CONTAINS(
                '27haf8L6oxUeXrHrgEgsexjSY5hbVUWEmvv9Nyxg8vQv' :: variant,
                inner_instruction_program_ids
            )
            OR ARRAY_CONTAINS(
                '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8' :: variant,
                inner_instruction_program_ids
            )
            OR ARRAY_CONTAINS(
                '5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h' :: variant,
                inner_instruction_program_ids
            )
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_id > 67919748 -- first appearance of Raydium LP program id
{% endif %}
),
lp_events AS (
    SELECT
        e.*,
        signers [0] :: STRING AS liquidity_provider,
        silver.udf_get_jupv4_inner_programs(
            inner_instruction :instructions
        ) AS inner_programs
    FROM
        base_events e
),
lp_events_w_inner_program_ids AS (
    SELECT
        lp_events.*,
        i.value :program_id :: STRING AS inner_lp_program_id,
        i.value :inner_index :: INT AS lp_program_inner_index_start,
        COALESCE(LEAD(lp_program_inner_index_start) over (PARTITION BY tx_id, lp_events.index
    ORDER BY
        lp_program_inner_index_start) -1, 999999) AS lp_program_inner_index_end
    FROM
        lp_events,
        TABLE(FLATTEN(inner_programs)) i),
outer_withdraws_and_deposits AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        INDEX,
        -1 AS inner_index,
        liquidity_provider,
        program_id,
        NULL AS lp_program_inner_index_start,
        NULL AS lp_program_inner_index_end,
        instruction AS event_instructions,
        ARRAY_SIZE(
            instruction :accounts
        ) AS num_accts,
        _inserted_timestamp
    FROM
        lp_events
    WHERE
        program_id IN (
            '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
            '27haf8L6oxUeXrHrgEgsexjSY5hbVUWEmvv9Nyxg8vQv',
            '5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h'
        )
),
inner_withdraws_and_deposits AS (
    SELECT
        A.block_timestamp,
        A.block_id,
        A.tx_id,
        A.succeeded,
        A.index,
        ii.index AS inner_index,
        A.liquidity_provider,
        A.inner_lp_program_id AS program_id,
        A.lp_program_inner_index_start,
        A.lp_program_inner_index_end,
        ii.value AS event_instructions,
        ARRAY_SIZE(
            ii.value :accounts
        ) AS num_accts,
        A._inserted_timestamp
    FROM
        lp_events_w_inner_program_ids A
        LEFT JOIN TABLE(FLATTEN(inner_instruction :instructions)) ii
        ON A.lp_program_inner_index_start = ii.index
    WHERE
        A.inner_lp_program_id IN (
            '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
            '27haf8L6oxUeXrHrgEgsexjSY5hbVUWEmvv9Nyxg8vQv',
            '5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h'
        )
),
combined AS(
    SELECT
        *
    FROM
        outer_withdraws_and_deposits
    UNION
    SELECT
        *
    FROM
        inner_withdraws_and_deposits
)
SELECT
    C.*,
    CASE
        WHEN num_accts > 17 THEN 'withdraw'
        WHEN num_accts < 17 THEN 'deposit'
    END AS action
FROM
    combined C
    LEFT JOIN {{ ref('silver__initialization_pools_raydium') }} p1
    ON (
        event_instructions :accounts [5] :: STRING = p1.pool_token
    )
WHERE
    (
        C.num_accts > 17
        OR C.num_accts < 17
    )
    AND p1.tx_id IS NOT NULL
