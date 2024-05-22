{{ config(
    materialized = 'incremental',
    unique_key = ["block_id","tx_id","action_index"],
    merge_predicates = ["DBT_INTERNAL_DEST.block_timestamp::date >= LEAST(current_date-7,(select min(block_timestamp)::date from {{ this }}__dbt_tmp))"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    tags = ['scheduled_non_core']
) }}

WITH base_events AS(

    SELECT
        *
    FROM
        {{ ref('silver__events') }}
    WHERE(
            program_id IN (
                'SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ',
                'DecZY86MU5Gj7kppfUCEmd4LbXXuyZH1yHaP2NTqdiZB',
                'Crt7UoUR6QgrFrN7j8rmSQpUTNWNSitSwWvsWGf1qZ5t',
                --program ids for acct mapping
                'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL',
                'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
            )
            OR ARRAY_CONTAINS(
                'SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ' :: variant,
                inner_instruction_program_ids
            )
            OR ARRAY_CONTAINS(
                'DecZY86MU5Gj7kppfUCEmd4LbXXuyZH1yHaP2NTqdiZB' :: variant,
                inner_instruction_program_ids
            )
            OR ARRAY_CONTAINS(
                'Crt7UoUR6QgrFrN7j8rmSQpUTNWNSitSwWvsWGf1qZ5t' :: variant,
                inner_instruction_program_ids
            )
        )
        AND block_id > 80172009 -- first appearance of Saber LP action

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2021-05-27'
{% endif %}
),
dex_lp_txs AS (
    SELECT
        e.*,
        IFF(ARRAY_SIZE(signers) = 1, signers [0] :: STRING, NULL) AS liquidity_provider,
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
    block_timestamp :: DATE >= '2021-05-27'
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
),
account_mappings AS (
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
-- actions where saber programs are within instructions
lp_actions_with_amounts_1 AS(
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
        e.liquidity_provider AS tmp_liquidity_provider,
        e.program_id,
        e.signers,
        ii.value :parsed :type :: STRING AS action,
        s.inner_swap_program_id,
        CASE
            WHEN e.program_id = 'DecZY86MU5Gj7kppfUCEmd4LbXXuyZH1yHaP2NTqdiZB' THEN e.instruction :accounts [1] :: STRING
        END AS temp_wrapped_mint,
        CASE
            WHEN e.program_id = 'SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ'
            AND action = 'burn' THEN e.instruction :accounts [3] :: STRING
            WHEN e.program_id = 'SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ'
            AND action = 'mintTo' THEN e.instruction :accounts [7] :: STRING
        END AS lp_mint_address,
        CASE
            WHEN e.program_id = 'SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ' THEN e.instruction :accounts [0] :: STRING
        END AS liquidity_pool_address,
        ii.value :parsed :info :amount :: INT AS lp_amount
    FROM
        lp_action_w_inner_program_id s
        INNER JOIN dex_lp_txs e
        ON s.tx_id = e.tx_id
        AND s.index = e.index
        LEFT JOIN TABLE(FLATTEN(inner_instruction :instructions)) ii
    WHERE
        ii.value :parsed :type :: STRING IN(
            'burn',
            'mintTo'
        )
        AND(
            (
                e.program_id = 'SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ'
                AND ARRAY_SIZE(
                    e.instruction :accounts
                ) IN (
                    10,
                    11,
                    12,
                    13
                )
                AND e.instruction :accounts [9] :: STRING <> 'SysvarC1ock11111111111111111111111111111111'
                AND e.instruction :accounts [8] :: STRING <> 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
                AND e.instruction :accounts [1] :: STRING <> 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
            )
            OR (
                e.program_id = 'DecZY86MU5Gj7kppfUCEmd4LbXXuyZH1yHaP2NTqdiZB'
            )
        )
),
-- actions where saber programs are within inner_instructions
lp_actions_with_amounts_2 AS(
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
        e.liquidity_provider AS tmp_liquidity_provider,
        e.signers,
        COALESCE(
            s.inner_swap_program_id,
            e.program_id
        ) AS program_id,
        ii.value :parsed :type :: STRING AS action,
        s.inner_swap_program_id,
        CASE
            WHEN s.inner_swap_program_id = 'DecZY86MU5Gj7kppfUCEmd4LbXXuyZH1yHaP2NTqdiZB' THEN e.inner_instruction :instructions [0] :accounts [1] :: STRING
        END AS temp_wrapped_mint,
        CASE
            WHEN s.inner_swap_program_id = 'SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ'
            AND action = 'burn' THEN e.inner_instruction :instructions [0] :accounts [3] :: STRING
            WHEN s.inner_swap_program_id = 'SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ'
            AND action = 'mintTo' THEN e.inner_instruction :instructions [0] :accounts [7] :: STRING
        END AS lp_mint_address,
        CASE
            WHEN s.inner_swap_program_id = 'SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ' THEN e.inner_instruction :instructions [0] :accounts [0] :: STRING
        END AS liquidity_pool_address,
        ii.value :parsed :info :amount :: INT AS lp_amount
    FROM
        lp_action_w_inner_program_id s
        INNER JOIN dex_lp_txs e
        ON s.tx_id = e.tx_id
        AND s.index = e.index
        LEFT JOIN TABLE(FLATTEN(inner_instruction :instructions)) ii
    WHERE
        ii.value :parsed :type :: STRING IN(
            'burn',
            'mintTo'
        )
        AND(
            (
                s.inner_swap_program_id = 'SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ'
                AND ARRAY_SIZE(
                    e.inner_instruction :instructions [0] :accounts
                ) IN (
                    10,
                    11,
                    12,
                    13
                )
                AND e.inner_instruction :instructions [0] :accounts [9] :: STRING <> 'SysvarC1ock11111111111111111111111111111111'
                AND e.inner_instruction :instructions [0] :accounts [8] :: STRING <> 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
                AND ii.index BETWEEN swap_program_inner_index_start
                AND swap_program_inner_index_end
                and s.inner_index < ii.index
            )
            OR (
                s.inner_swap_program_id = 'DecZY86MU5Gj7kppfUCEmd4LbXXuyZH1yHaP2NTqdiZB'
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
        s.tmp_liquidity_provider,
        s.liquidity_pool_address,
        s.lp_mint_address,
        s.lp_amount,
        s.temp_wrapped_mint,
        s.signers,
        s.program_id,
        ROUND(
            amount,
            2
        ) AS temp_round_amt
    FROM
        lp_actions_with_amounts_1 s
        LEFT OUTER JOIN account_mappings m1
        ON s.tx_id = m1.tx_id
        AND s.tx_from = m1.associated_account
        AND s.tx_to <> m1.owner
        LEFT OUTER JOIN account_mappings m2
        ON s.tx_id = m2.tx_id
        AND s.tx_to = m2.associated_account
        AND s.tx_from <> m2.owner
    UNION
    SELECT
        s.block_id,
        s.block_timestamp,
        s.tx_id,
        s.index,
        s.inner_index,
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
        s.tmp_liquidity_provider,
        s.liquidity_pool_address,
        s.lp_mint_address,
        s.lp_amount,
        s.temp_wrapped_mint,
        s.signers,
        s.program_id,
        ROUND(
            amount,
            2
        ) AS temp_round_amt
    FROM
        lp_actions_with_amounts_2 s
        LEFT OUTER JOIN account_mappings m1
        ON s.tx_id = m1.tx_id
        AND s.tx_from = m1.associated_account
        AND s.tx_to <> m1.owner
        LEFT OUTER JOIN account_mappings m2
        ON s.tx_id = m2.tx_id
        AND s.tx_to = m2.associated_account
        AND s.tx_from <> m2.owner
),
multi_signer_swapper AS (
    SELECT
        tx_id,
        silver.udf_get_multi_signers_swapper(ARRAY_AGG(tx_from), ARRAY_AGG(tx_to), ARRAY_AGG(signers) [0]) AS liquidity_provider
    FROM
        lp_actions_w_destination
    WHERE
        succeeded
        AND ARRAY_SIZE(signers) > 1
        AND tmp_liquidity_provider IS NULL
    GROUP BY
        1
),
lp_actions_w_liq_provider AS (
    SELECT
        l.*,
        COALESCE(
            l.tmp_liquidity_provider,
            m.liquidity_provider
        ) AS liquidity_provider
    FROM
        lp_actions_w_destination l
        LEFT OUTER JOIN multi_signer_swapper m
        ON l.tx_id = m.tx_id
),
lp_actions_w_unwrapped_tokens AS (
    SELECT
        l1.block_id,
        l1.block_timestamp,
        l1.tx_id,
        l1.index,
        l1.inner_index,
        l1.tx_from,
        l1.tx_to,
        COALESCE(
            l3.amount,
            l2.amount,
            l1.amount
        ) AS amount,
        COALESCE(
            l3.mint,
            l2.mint,
            l1.mint
        ) AS mint,
        l1.action,
        l1.succeeded,
        l1._inserted_timestamp,
        l1.liquidity_provider,
        l1.liquidity_pool_address,
        l1.lp_mint_address,
        l1.lp_amount,
        l1.temp_wrapped_mint,
        l1.program_id
    FROM
        lp_actions_w_liq_provider l1
        LEFT JOIN lp_actions_w_destination l2
        ON l1.tx_id = l2.tx_id
        AND l1.lp_mint_address IS NOT NULL
        AND l2.lp_mint_address IS NULL
        AND l1.mint = l2.temp_wrapped_mint
        LEFT JOIN lp_actions_w_liq_provider l3
        ON l1.tx_id = l3.tx_id
        AND l1.mint <> l3.mint
        AND l3.lp_mint_address IS NULL
        AND l3.program_id <> 'DecZY86MU5Gj7kppfUCEmd4LbXXuyZH1yHaP2NTqdiZB'
        AND l1.temp_round_amt = l3.temp_round_amt
    WHERE
        l1.program_id <> 'DecZY86MU5Gj7kppfUCEmd4LbXXuyZH1yHaP2NTqdiZB'
        AND l1.lp_mint_address IS NOT NULL
        AND l1.amount <> 0
),
temp_final AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        succeeded,
        program_id,
        CASE
            WHEN action = 'burn' THEN 'withdraw'
            WHEN action = 'mintTo' THEN 'deposit'
            ELSE NULL
        END AS action,
        liquidity_provider,
        liquidity_pool_address,
        amount,
        mint,
        INDEX,
        inner_index,
        _inserted_timestamp
    FROM
        lp_actions_w_unwrapped_tokens
    UNION
        -- retrieving the lp mint actions as separate rows
    SELECT
        l.block_id,
        l.block_timestamp,
        l.tx_id,
        l.succeeded,
        l.program_id,
        CASE
            WHEN l.action in ('mintTo','burn') THEN l.action
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
        lp_actions_w_unwrapped_tokens l
        LEFT OUTER JOIN {{ ref('silver__complete_token_asset_metadata') }}
        m
        ON l.lp_mint_address = m.token_address
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
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_id', 'tx_id','action_index']
    ) }} AS liquidity_pool_actions_saber_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    temp_final
WHERE
    COALESCE(
        amount,
        0
    ) > 0