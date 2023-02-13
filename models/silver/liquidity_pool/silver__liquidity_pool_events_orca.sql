{{ config(
    materialized = 'incremental',
    unique_key = ["block_id","tx_id"],
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
                'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc',
                '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP',
                'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1'
            )
            OR ARRAY_CONTAINS(
                '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP' :: variant,
                inner_instruction_program_ids
            )
            OR ARRAY_CONTAINS(
                'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc' :: variant,
                inner_instruction_program_ids
            )
            OR ARRAY_CONTAINS(
                'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1' :: variant,
                inner_instruction_program_ids
            )
        )
        AND block_id > 65303193 -- first appearance of Orca program id
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
        i.value :program_id :: STRING AS inner_swap_program_id,
        i.value :inner_index :: INT AS swap_program_inner_index_start,
        COALESCE(LEAD(swap_program_inner_index_start) over (PARTITION BY tx_id, lp_events.index
    ORDER BY
        swap_program_inner_index_start) -1, 999999) AS swap_program_inner_index_end
    FROM
        lp_events,
        TABLE(FLATTEN(inner_programs)) i),
outer_withdraws_and_deposits AS (
    SELECT
        -- *
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        INDEX,
        NULL AS inner_index,
        liquidity_provider,
        program_id,
        NULL AS swap_program_inner_index_start,
        NULL AS swap_program_inner_index_end,
        instruction AS temp_instructions,
        ARRAY_SIZE(
            instruction :accounts
        ) AS temp_num_accts,
        _inserted_timestamp
    FROM
        lp_events
    WHERE
        (
            program_id IN (
                'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1',
                '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP'
            )
            OR (
                program_id = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc'
                AND instruction :accounts [1] = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
            )
            OR (
                program_id = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc'
                AND ARRAY_SIZE(
                    instruction :accounts
                ) = 9
            )
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
        A.inner_swap_program_id AS program_id,
        A.swap_program_inner_index_start,
        A.swap_program_inner_index_end,
        ii.value AS temp_instructions,
        ARRAY_SIZE(
            ii.value :accounts
        ) AS temp_num_accts,
        A._inserted_timestamp
    FROM
        lp_events_w_inner_program_ids A
        LEFT JOIN TABLE(FLATTEN(inner_instruction :instructions)) ii
        ON A.swap_program_inner_index_start = ii.index
    WHERE
        (
            A.inner_swap_program_id IN (
                'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1',
                '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP'
            )
            OR (
                A.inner_swap_program_id = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc'
                AND ii.value :accounts [1] = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
                AND ARRAY_SIZE(
                    ii.value :accounts
                ) > 10
            ) -- OR (
            --     program_id = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc'
            --     AND ARRAY_SIZE(
            --         instruction :accounts
            --     ) = 9
            -- )
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
),
lp_events_with_swaps_removed AS (
    SELECT
        C.*
    FROM
        combined C
        LEFT JOIN solana_dev.silver.initialization_pools_orca p1
        ON (
            temp_instructions :accounts [6] :: STRING = p1.token_a_account
            OR temp_instructions :accounts [6] :: STRING = p1.token_b_account
        )
    WHERE
        C.program_id = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc'
        OR (
            C.program_id IN (
                'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1',
                '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP'
            )
            AND temp_num_accts = 9
        )
        OR(
            C.program_id IN (
                'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1',
                '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP'
            )
            AND temp_num_accts = 10
            AND p1.tx_id IS NOT NULL
        )
        OR(
            C.program_id IN (
                'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1',
                '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP'
            )
            AND temp_num_accts = 11
            AND p1.tx_id IS NOT NULL
        )
)
SELECT
    A.*,
    CASE
        WHEN program_id = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc'
        AND temp_num_accts = 9 THEN 'whirlpool_fee_withdraw'
        WHEN program_id = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc'
        AND temp_num_accts > 9 THEN 'whirlpool_unknown'
        WHEN temp_num_accts = 9
        AND A.program_id IN (
            'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1',
            '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP'
        ) THEN 'deposit'
        WHEN temp_num_accts = 11
        AND A.program_id IN (
            'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1',
            '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP'
        ) THEN 'withdraw'
        WHEN temp_num_accts = 10
        AND A.program_id IN (
            'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1',
            '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP'
        )
        AND p2.pool_token IS NOT NULL THEN 'deposit'
        WHEN temp_num_accts = 10
        AND A.program_id IN (
            'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1',
            '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP'
        )
        AND p1.pool_token IS NOT NULL THEN 'withdraw'
    END AS action
FROM
    lp_events_with_swaps_removed A
    LEFT JOIN solana_dev.silver.initialization_pools_orca p1
    ON (
        A.temp_instructions :accounts [3] :: STRING = p1.pool_token
    )
    LEFT JOIN solana_dev.silver.initialization_pools_orca p2
    ON (
        A.temp_instructions :accounts [7] :: STRING = p2.pool_token
    )
