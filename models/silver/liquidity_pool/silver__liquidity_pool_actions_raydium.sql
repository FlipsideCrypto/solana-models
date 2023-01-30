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
                '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
                '27haf8L6oxUeXrHrgEgsexjSY5hbVUWEmvv9Nyxg8vQv',
                '5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h',
                --program ids for acct mapping
                'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL',
                'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
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
        AND block_id > 67919748 -- first appearance of Raydium LP program id

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2021-03-06'
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
    block_timestamp :: DATE >= '2021-03-06'
{% endif %}
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
        s.program_id <> '11111111111111111111111111111111'
        or inner_swap_program_id IS NULL
        OR inner_swap_program_id IN (
                '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
                '27haf8L6oxUeXrHrgEgsexjSY5hbVUWEmvv9Nyxg8vQv',
                '5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h'
        )
),
raydium_account_mapping AS(
    SELECT
        tx_id,
        ii.value :parsed :info :account :: STRING AS associated_account,
        COALESCE(
            ii.value :parsed :info :source :: STRING,
            ii.value :parsed :info :owner :: STRING
        ) AS owner
    FROM
        dex_lp_txs AS d
        LEFT OUTER JOIN TABLE(FLATTEN(inner_instruction :instructions)) ii
    WHERE
        d.program_id = '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8'
        AND associated_account IS NOT NULL
),
account_mappings AS (
    SELECT
        *
    FROM
        raydium_account_mapping
    UNION
    SELECT
        e.tx_id,
        e.instruction :parsed :info :account :: STRING AS associated_account,
        COALESCE(
            e.instruction :parsed :info :source :: STRING,
            e.instruction :parsed :info :owner :: STRING
        ) AS owner
    FROM
        base_events e
        INNER JOIN (
            SELECT
                DISTINCT tx_id
            FROM
                dex_lp_txs
        ) d
        ON d.tx_id = e.tx_id
    WHERE
        (
            (
                e.program_id = 'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL'
                AND e.event_type = 'create'
            )
            OR (
                e.program_id = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
                AND e.event_type = 'closeAccount'
            )
        )
),
lp_actions_w_amounts_1 AS(
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
        e.program_id,
        e.instruction :accounts [2] :: STRING AS temp_authority,
        e.instruction :accounts [1] :: STRING AS liquidity_pool_address,
        e.instruction :accounts [5] :: STRING AS lp_mint_address,
        ii.value :parsed :info :amount :: INT AS lp_amount,
        CASE
            WHEN e.program_id IN (
                '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
                '27haf8L6oxUeXrHrgEgsexjSY5hbVUWEmvv9Nyxg8vQv'
            )
            AND ARRAY_SIZE(
                e.instruction :accounts
            ) > 17 THEN 'withdraw'
            WHEN e.program_id IN (
                '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
                '27haf8L6oxUeXrHrgEgsexjSY5hbVUWEmvv9Nyxg8vQv'
            )
            AND ARRAY_SIZE(
                e.instruction :accounts
            ) < 15 THEN 'deposit'
            WHEN e.program_id = '5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h'
            AND ARRAY_SIZE(
                e.instruction :accounts
            ) = 21 THEN 'withdraw'
            WHEN e.program_id = '5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h'
            AND ARRAY_SIZE(
                e.instruction :accounts
            ) IN (
                14,
                15
            ) THEN 'deposit'
            WHEN e.program_id = '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8'
            AND ARRAY_SIZE(
                e.instruction :accounts
            ) = 16 THEN 'withdrawpnl'
            ELSE NULL
        END AS action
    FROM
        lp_action_w_inner_program_id s
        INNER JOIN dex_lp_txs e
        ON s.tx_id = e.tx_id
        AND s.index = e.index
        LEFT JOIN TABLE(FLATTEN(inner_instruction :instructions)) ii
    WHERE
        (
            ii.value :parsed :type :: STRING IN(
                'burn',
                'mintTo'
            )
            AND (
                (
                    --deposits/add liquidity
                    e.program_id = '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8'
                    AND (
                        e.instruction :accounts [2] :: STRING = '5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1'
                        AND ARRAY_SIZE(
                            e.instruction :accounts
                        ) IN (
                            12,
                            13,
                            14
                        )
                    )
                )
                OR (
                    -- withdraws/remove liquidity
                    (
                        e.program_id = '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8'
                        AND e.instruction :accounts [0] :: STRING = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
                        AND e.instruction :accounts [2] :: STRING = '5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1'
                        AND e.instruction :accounts [10] :: STRING IN (
                            '9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin',
                            'srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX'
                        )
                        AND ARRAY_SIZE(
                            e.instruction :accounts
                        ) >= 19
                    )
                )
                OR (
                    e.program_id = '27haf8L6oxUeXrHrgEgsexjSY5hbVUWEmvv9Nyxg8vQv'
                )
                OR (
                    e.program_id = '5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h'
                    AND ARRAY_SIZE(
                        e.instruction :accounts
                    ) IN (
                        14,
                        15,
                        21
                    )
                )
            )
        )
        OR (
            e.program_id = '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8'
            AND ARRAY_SIZE(
                e.instruction :accounts
            ) = 16
            AND e.instruction :accounts [1] <> 'SysvarRent111111111111111111111111111111111'
        )
),
lp_actions_w_amounts_2 AS(
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
        COALESCE(
            s.inner_swap_program_id,
            e.program_id
        ) AS program_id,
        s.inner_swap_program_id,
        s.swap_program_inner_index_start,
        s.swap_program_inner_index_end,
        e.inner_instruction :instructions [0] :accounts [2] :: STRING AS temp_authority,
        e.inner_instruction :instructions [0] :accounts [1] :: STRING AS liquidity_pool_address,
        e.inner_instruction :instructions [0] :accounts [5] :: STRING AS lp_mint_address,
        ii.value :parsed :info :amount :: INT AS lp_amount,
        CASE
            WHEN s.inner_swap_program_id IN (
                '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
                '27haf8L6oxUeXrHrgEgsexjSY5hbVUWEmvv9Nyxg8vQv'
            )
            AND ARRAY_SIZE(
                e.inner_instruction :instructions [0] :accounts
            ) > 17 THEN 'withdraw'
            WHEN s.inner_swap_program_id IN (
                '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
                '27haf8L6oxUeXrHrgEgsexjSY5hbVUWEmvv9Nyxg8vQv'
            )
            AND ARRAY_SIZE(
                e.inner_instruction :instructions [0] :accounts
            ) < 15 THEN 'deposit'
            WHEN e.program_id = '5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h'
            AND ARRAY_SIZE(
                e.inner_instruction :instructions [0] :accounts
            ) = 21 THEN 'withdraw'
            WHEN e.program_id = '5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h'
            AND ARRAY_SIZE(
                e.inner_instruction :instructions [0] :accounts
            ) IN (
                14,
                15
            ) THEN 'deposit'
            ELSE NULL
        END AS action
    FROM
        lp_action_w_inner_program_id s
        INNER JOIN dex_lp_txs e
        ON s.tx_id = e.tx_id
        AND s.index = e.index
        LEFT JOIN TABLE(FLATTEN(inner_instruction :instructions)) ii
    WHERE
        (
            ii.value :parsed :type :: STRING IN(
                'burn',
                'mintTo'
            )
            AND lp_mint_address <> mint
            AND (
                (
                    s.inner_swap_program_id = '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8'
                    AND (
                        e.inner_instruction :instructions [0] :accounts [2] :: STRING = '5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1'
                        AND ARRAY_SIZE(
                            e.inner_instruction :instructions [0] :accounts
                        ) IN (
                            12,
                            13,
                            14
                        )
                    )
                )
                OR (
                    (
                        s.inner_swap_program_id = '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8'
                        AND e.inner_instruction :instructions [0] :accounts [0] :: STRING = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
                        AND e.inner_instruction :instructions [0] :accounts [2] :: STRING = '5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1'
                        AND e.inner_instruction :instructions [0] :accounts [10] :: STRING IN (
                            '9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin',
                            'srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX'
                        )
                        AND ARRAY_SIZE(
                            e.inner_instruction :instructions [0] :accounts
                        ) >= 19
                        AND ii.index BETWEEN swap_program_inner_index_start
                        AND swap_program_inner_index_end
                    )
                )
                OR (
                    s.inner_swap_program_id = '27haf8L6oxUeXrHrgEgsexjSY5hbVUWEmvv9Nyxg8vQv'
                    AND ii.index BETWEEN swap_program_inner_index_start
                    AND swap_program_inner_index_end
                )
                OR (
                    s.inner_swap_program_id = '5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h'
                    AND ARRAY_SIZE(
                        e.inner_instruction :instructions [0] :accounts
                    ) IN (
                        14,
                        15,
                        21
                    )
                )
            )
        )
),
lp_actions_w_destination AS (
    SELECT
        s.block_id,
        s.block_timestamp,
        s.tx_id,
        s.index,
        s.inner_index,
        s.liquidity_provider,
        COALESCE(
            m1.owner,
            s.tx_from
        ) AS tx_from,
        COALESCE(
            m2.owner,
            s.tx_to
        ) AS tx_to,
        s.amount,
        s.mint,
        s.action,
        s.succeeded,
        s._inserted_timestamp,
        s.program_id,
        s.liquidity_pool_address,
        s.lp_mint_address,
        s.lp_amount
    FROM
        lp_actions_w_amounts_1 s
        LEFT OUTER JOIN account_mappings m1
        ON s.tx_id = m1.tx_id
        AND s.tx_from = m1.associated_account
        AND s.tx_to <> m1.owner
        LEFT OUTER JOIN account_mappings m2
        ON s.tx_id = m2.tx_id
        AND s.tx_to = m2.associated_account
        AND s.tx_from <> m2.owner
    WHERE
        action IN (
            'withdrawpnl',
            'deposit'
        )
        OR (
            action = 'withdraw'
            AND tx_to <> temp_authority
            AND tx_from = temp_authority
        )
    UNION
    SELECT
        s.block_id,
        s.block_timestamp,
        s.tx_id,
        s.index,
        s.inner_index,
        s.liquidity_provider,
        COALESCE(
            m1.owner,
            s.tx_from
        ) AS tx_from,
        COALESCE(
            m2.owner,
            s.tx_to
        ) AS tx_to,
        s.amount,
        s.mint,
        s.action,
        s.succeeded,
        s._inserted_timestamp,
        s.program_id,
        s.liquidity_pool_address,
        s.lp_mint_address,
        s.lp_amount
    FROM
        lp_actions_w_amounts_2 s
        LEFT OUTER JOIN account_mappings m1
        ON s.tx_id = m1.tx_id
        AND s.tx_from = m1.associated_account
        AND s.tx_to <> m1.owner
        LEFT OUTER JOIN account_mappings m2
        ON s.tx_id = m2.tx_id
        AND s.tx_to = m2.associated_account
        AND s.tx_from <> m2.owner
    WHERE
        action = 'deposit'
        OR (
            action = 'withdraw'
            AND tx_to <> temp_authority
            AND tx_from = temp_authority
        )
),
temp_final AS(
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
        INDEX,
        inner_index,
        _inserted_timestamp
    FROM
        lp_actions_w_destination
    UNION
    SELECT
        l.block_id,
        l.block_timestamp,
        l.tx_id,
        l.succeeded,
        l.program_id,
        CASE
            WHEN action = 'deposit' THEN 'mint_LP_tokens'
            WHEN action = 'withdraw' THEN 'burn_LP_tokens'
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
        lp_actions_w_destination l
        LEFT OUTER JOIN {{ ref('silver__token_metadata') }}
        m
        ON l.lp_mint_address = m.token_address
    WHERE
        action <> 'withdrawpnl'
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
