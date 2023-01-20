{{ config(
    materialized = 'incremental',
    unique_key = ["block_id","tx_id","action_index"],
    merge_predicates = ["DBT_INTERNAL_DEST.block_timestamp::date >= LEAST(current_date-7,(select min(block_timestamp)::date from {{ this }}__dbt_tmp))"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
) }}

WITH base_events AS(

    SELECT
        *
    FROM
        {{ ref('silver__events') }}
    WHERE
        (
            program_id IN (
                -- Orca
                'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc',
                '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP',
                'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1',
                --program ids for acct mapping
                'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL',
                'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
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

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2021-02-14'
{% endif %}
),
dex_lp_txs AS (
    SELECT
        e.*,
        signers [0] :: STRING AS liquidity_provider,
        silver.udf_get_jupv4_inner_programs(
            inner_instruction :instructions
        ) AS inner_programs
    FROM
        base_events e
),
temp_inner_program_ids AS (
    SELECT
        dex_lp_txs.*,
        i.value :program_id :: STRING AS inner_swap_program_id,
        i.value :inner_index :: INT AS swap_program_inner_index_start,
        COALESCE(LEAD(swap_program_inner_index_start) over (PARTITION BY tx_id, dex_lp_txs.index
    ORDER BY
        swap_program_inner_index_start) -1, 999999) AS swap_program_inner_index_end
    FROM
        dex_lp_txs,
        TABLE(FLATTEN(inner_programs)) i),
        base_transfers AS (
            SELECT
                tr.*
            FROM
                {{ ref('silver__transfers') }}
                tr
                INNER JOIN (
                    SELECT
                        DISTINCT tx_id
                    FROM
                        dex_lp_txs
                ) d
                ON d.tx_id = tr.tx_id

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% else %}
WHERE
    block_timestamp :: DATE >= '2021-02-14'
{% endif %}
),
nft_lp_mint_address AS (
    SELECT
        e.tx_id,
        e.index,
        ii.value :parsed :info :mint :: text AS mint,
        ROW_NUMBER() over (
            PARTITION BY tx_id
            ORDER BY
                e.index
        ) AS temp_rn
    FROM
        base_events e
        LEFT JOIN TABLE(FLATTEN(inner_instruction :instructions)) ii
    WHERE
        program_id = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc'
        AND (
            ii.value :parsed :type = 'burnChecked'
        )
        OR (
            ii.value :parsed :type = 'initializeMint'
        )
),
lp_transfers_temp AS(
    SELECT
        A.block_id,
        A.block_timestamp,
        A.tx_id,
        COALESCE(SPLIT_PART(INDEX :: text, '.', 1) :: INT, INDEX :: INT) AS INDEX,
        NULLIF(SPLIT_PART(INDEX :: text, '.', 2), '') :: INT AS inner_index,
        A.program_id,
        A.tx_from,
        A.tx_to,
        A.amount,
        A.mint,
        A.succeeded,
        A._inserted_timestamp
    FROM
        base_transfers AS A
    WHERE
        A.tx_id IN (
            SELECT
                tx_id
            FROM
                dex_lp_txs
        )
),
lp_action_w_inner_program_id AS(
    SELECT
        s.*,
        t.inner_swap_program_id,
        t.swap_program_inner_index_start,
        t.swap_program_inner_index_end
    FROM
        lp_transfers_temp s
        LEFT OUTER JOIN temp_inner_program_ids t
        ON t.tx_id = s.tx_id
        AND t.index = s.index
        AND s.inner_index BETWEEN t.swap_program_inner_index_start
        AND t.swap_program_inner_index_end
    WHERE
        inner_swap_program_id IS NULL
        OR inner_swap_program_id IN (
            '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP',
            'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc',
            'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1'
        )
),
multisig_account_mapping AS(
    SELECT
        tx_id,
        instruction :parsed :info :account :: STRING AS associated_account,
        instruction :parsed :info :multisigOwner :: STRING AS owner
    FROM
        base_events
    WHERE
        program_id = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
        AND event_type = 'closeAccount'
    UNION
    SELECT
        tx_id,
        instruction :parsed :info :delegate :: STRING AS associated_account,
        instruction :parsed :info :multisigOwner :: STRING AS owner
    FROM
        base_events
    WHERE
        program_id = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
        AND event_type = 'approve'
),
-- LP transfers where Orca programs are within Instructions
lp_transfers_with_amounts_1 AS(
    SELECT
        s.block_id,
        s.block_timestamp,
        s.tx_id,
        s.index,
        s.inner_index,
        s.tx_from,
        s.tx_to,
        s.amount,
        s.mint,
        s.succeeded,
        s._inserted_timestamp,
        e.liquidity_provider,
        s.inner_swap_program_id,
        s.swap_program_inner_index_start,
        e.signers,
        e.program_id,
        e.instruction :accounts [0] :: STRING AS liquidity_pool_address,
        CASE
            WHEN e.program_id IN (
                '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP',
                'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1'
            ) THEN ii.value :parsed :info :mint :: STRING
            ELSE NULL
        END AS lp_mint_address,
        CASE
            WHEN e.program_id IN (
                'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1',
                '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP'
            ) THEN ii.value :parsed :info :amount :: INT
            WHEN e.program_id = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc' THEN 1
            ELSE NULL
        END AS lp_amount,
        CASE
            WHEN e.instruction :accounts [1] :: STRING = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
            AND e.program_id = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc' THEN 'lp_action'
            WHEN ARRAY_SIZE(
                e.instruction :accounts
            ) IN (
                9,
                10
            )
            AND e.program_id IN (
                'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1',
                '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP'
            )
            AND ii.value :parsed :type :: STRING = 'mintTo' THEN 'deposit'
            WHEN ARRAY_SIZE(
                e.instruction :accounts
            ) IN (
                10,
                11
            )
            AND e.program_id IN (
                'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1',
                '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP'
            )
            AND ii.value :parsed :type :: STRING = 'burn' THEN 'withdraw'
            ELSE NULL
        END AS action,
        DENSE_RANK() over (
            PARTITION BY e.tx_id
            ORDER BY
                e.index
        ) AS temp_rn
    FROM
        lp_action_w_inner_program_id s
        INNER JOIN dex_lp_txs e
        ON s.tx_id = e.tx_id
        AND s.index = e.index
        LEFT JOIN TABLE(FLATTEN(inner_instruction :instructions)) ii -- LEFT OUTER JOIN liquidity_provider_map_temp sm
    WHERE
        (
            (
                e.program_id IN (
                    'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1',
                    '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP'
                )
                AND ii.value :parsed :type :: STRING IN(
                    'burn',
                    'mintTo'
                )
            )
            OR (
                e.program_id = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc'
                AND e.instruction :accounts [1] = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
                AND s.inner_index = ii.index
                AND action IS NOT NULL
            )
        )
),
-- LP transfers where Orca programs are within Inner_Instructions
lp_transfers_with_amounts_2 AS(
    SELECT
        s.block_id,
        s.block_timestamp,
        s.tx_id,
        s.index,
        s.inner_index,
        s.tx_from,
        s.tx_to,
        s.amount,
        s.mint,
        s.succeeded,
        s._inserted_timestamp,
        e.liquidity_provider,
        s.inner_swap_program_id,
        s.swap_program_inner_index_start,
        -- s.swap_program_inner_index_end,
        -- e.inner_instruction:instructions:accounts as acct_test_all,
        -- e.inner_instruction:instructions[0]:accounts as acct_test_0,
        -- e.inner_instruction:instructions[1]:accounts as acct_test_1,
        --     e.inner_instruction:instructions[2]:accounts as acct_test_2,
        e.signers,
        coalesce(s.inner_swap_program_id,e.program_id) as program_id,
    e.inner_instruction:instructions[s.swap_program_inner_index_start]:accounts[0] :: STRING AS liquidity_pool_address,
        CASE
            WHEN s.inner_swap_program_id IN (
                '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP',
                'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1'
            ) THEN ii.value :parsed :info :mint :: STRING
            ELSE NULL
        END AS lp_mint_address,
        CASE
            WHEN s.inner_swap_program_id IN (
                'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1',
                '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP'
            ) THEN ii.value :parsed :info :amount :: INT
            WHEN s.inner_swap_program_id = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc' THEN 1
            ELSE NULL
        END AS lp_amount,

        CASE
            WHEN e.inner_instruction:instructions[s.swap_program_inner_index_start]:accounts[1] :: STRING = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
            AND s.inner_swap_program_id = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc' THEN 'lp_action'
    
            WHEN ARRAY_SIZE(
                e.inner_instruction:instructions[s.swap_program_inner_index_start]:accounts
            ) in (9,10)
            AND s.inner_swap_program_id in ('DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1','9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP')
            and ii.value :parsed :type :: STRING = 'mintTo' THEN 'deposit'
    
            WHEN ARRAY_SIZE(
                e.inner_instruction:instructions[s.swap_program_inner_index_start]:accounts
            ) in (10,11)
            AND s.inner_swap_program_id in ('DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1','9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP')
            and ii.value :parsed :type :: STRING = 'burn' THEN 'withdraw'

            ELSE NULL
        END AS action,
        DENSE_RANK() over (
            PARTITION BY e.tx_id
            ORDER BY
                e.index
        ) AS temp_rn
    
--     ,
--     'x' as start_inner_flat,
--     ii.*,
--     'z' as start_event_flat,
--     e.*
    FROM
        lp_action_w_inner_program_id s
        INNER JOIN dex_lp_txs e
        ON s.tx_id = e.tx_id
        AND s.index = e.index
        LEFT JOIN TABLE(FLATTEN(inner_instruction :instructions)) ii -- LEFT OUTER JOIN liquidity_provider_map_temp sm
    WHERE
    (
        (s.inner_swap_program_id in ('DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1','9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP')
     and lp_mint_address <> mint
     and 
        ii.value :parsed :type :: STRING IN(
            'burn',
            'mintTo'
        ))
        OR (
            s.inner_swap_program_id = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc'
            AND e.inner_instruction:instructions[s.swap_program_inner_index_start]:accounts[1] = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
            AND s.inner_index = ii.index
            and ARRAY_SIZE(
                e.inner_instruction:instructions[s.swap_program_inner_index_start]:accounts
            ) >10
        )
)
),
all_lp_transfers_with_amounts AS(
    SELECT
        *
    FROM
        lp_transfers_with_amounts_1
    UNION
    SELECT
        *
    FROM
        lp_transfers_with_amounts_2
),
-- deposits and swaps have the same instruction structure, so filter out swaps with this CTE
cnt_distinct_to_and_from AS (
    SELECT
        COUNT(
            DISTINCT tx_to
        ) AS cnt_tx_to,
        COUNT(
            DISTINCT tx_from
        ) AS cnt_tx_from,
        tx_id,
        INDEX,
        swap_program_inner_index_start
    FROM
        all_lp_transfers_with_amounts
    GROUP BY
        tx_id,
        INDEX,
        swap_program_inner_index_start
),
lp_actions_filtered AS(
    SELECT
        l.block_id,
        l.block_timestamp,
        l.tx_id,
        l.index,
        l.inner_index,
        l.tx_from,
        l.tx_to,
        l.amount,
        l.mint,
        l.action,
        l.succeeded,
        l._inserted_timestamp,
        l.liquidity_provider,
        l.liquidity_pool_address,
        COALESCE(
            l.lp_mint_address,
            n.mint
        ) AS lp_mint_address,
        l.lp_amount,
        l.signers,
        l.program_id
    FROM
        all_lp_transfers_with_amounts l
        LEFT JOIN nft_lp_mint_address n
        ON l.tx_id = n.tx_id
        AND l.temp_rn = n.temp_rn
        INNER JOIN cnt_distinct_to_and_from C
        ON l.tx_id = C.tx_id
        AND l.index = C.index
        AND (
            l.swap_program_inner_index_start = C.swap_program_inner_index_start
            OR l.swap_program_inner_index_start IS NULL
            AND C.swap_program_inner_index_start IS NULL
        )
    WHERE
        -- action = 'withdraw'
        -- or (action = 'deposit' and (c.cnt_tx_from = 1 or c.cnt_tx_to = 1))
        (
            C.cnt_tx_from = 1
            OR C.cnt_tx_to = 1
        )
        OR program_id = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc'
),
temp_final AS(
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        succeeded,
        program_id,
        CASE
            -- WHEN action = 'lp_action'
            -- AND tx_to = liquidity_provider THEN 'withdraw'
            -- WHEN action = 'lp_action'
            -- AND tx_from = liquidity_provider THEN 'deposit'
            -- ELSE action
            WHEN action = 'lp_action'
            AND tx_to = liquidity_pool_address THEN 'deposit'
            WHEN action = 'lp_action'
            AND tx_from = liquidity_pool_address THEN 'withdraw'
            ELSE action
        END AS action,
        liquidity_provider,
        liquidity_pool_address,
        amount,
        mint,
        INDEX,
        inner_index,
        _inserted_timestamp
    FROM
        lp_actions_filtered
    UNION
        -- get the lp mint/amount as separate records
    SELECT
        l.block_id,
        l.block_timestamp,
        l.tx_id,
        l.succeeded,
        l.program_id,
        CASE
            WHEN l.action = 'deposit' THEN 'mint_LP_tokens'
            WHEN l.action = 'withdraw' THEN 'burn_LP_tokens'
            WHEN l.action = 'lp_action'
            AND l.tx_to = l.liquidity_pool_address THEN 'mint_LP_tokens'
            WHEN l.action = 'lp_action'
            AND l.tx_from = l.liquidity_pool_address THEN 'burn_LP_tokens'
            ELSE NULL
        END AS action,
        l.liquidity_provider,
        l.liquidity_pool_address,
        COALESCE(l.lp_amount / pow(10, m.decimals), l.lp_amount) AS amount,
        l.lp_mint_address AS mint,
        l.index,
        NULL AS inner_index,
        l._inserted_timestamp
    FROM
        lp_actions_filtered l
        LEFT OUTER JOIN solana_dev.silver.token_metadata m
        ON l.lp_mint_address = m.token_address
    WHERE
        l.lp_mint_address IS NOT NULL -- and l.action IS NOT NULL
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    program_id,
    action,
    liquidity_provider,
    liquidity_pool_address,
    amount,
    mint,
    ROW_NUMBER() over (
        PARTITION BY tx_id
        ORDER BY
            INDEX,
            inner_index
    ) AS action_index,
    _inserted_timestamp
FROM
    temp_final
WHERE
    COALESCE(
        amount,
        0
    ) > 0
