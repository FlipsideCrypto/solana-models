{{ config(
    materialized = 'incremental',
    unique_key = ["block_id","tx_id","swap_index"],
    merge_predicates = ["DBT_INTERNAL_DEST.block_timestamp::date >= LEAST(current_date-7,(select min(block_timestamp)::date from {{ this }}__dbt_tmp))"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
) }}

WITH base_events AS(

    SELECT
        *
    FROM
        {{ ref('silver__events') }}
    WHERE
        program_id IN (
            -- Orca
            'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc',
            '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP',
            'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1',
            --program ids for acct mapping
            'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL',
            'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
            --program ids that identify the swapper in certain tx
            '8rGFmebhhTikfJP5bUe2uLHcejSiukdJhiLEKoit962a',
            'E16pm4Z4jiFxVEeBcSuYfJHy6TQYfYRAhGYt7cEUYfEV'
        )
        AND block_id > 111442741 -- token balances owner field not guaranteed to be populated before this slot

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2021-12-14'
{% endif %}
),
dex_lp_txs AS (
    SELECT
        e.*,
        IFF(ARRAY_SIZE(signers) = 1, signers [0] :: STRING, NULL) AS liquidity_provider,
        CASE
            -- whirl
            -- WHEN ARRAY_SIZE(e.instruction:accounts) = 7 and program_id = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc' THEN 'CollectReward'
            -- WHEN ARRAY_SIZE(e.instruction:accounts) = 9 and program_id = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc' THEN 'CollectFee'
            WHEN e.instruction :accounts [1] :: STRING = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
            AND program_id = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc' THEN 'lp_action' -- 9w95
            WHEN ARRAY_SIZE(
                e.instruction :accounts
            ) = 10
            AND program_id = '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP' THEN 'deposit'
            WHEN ARRAY_SIZE(
                e.instruction :accounts
            ) = 11
            AND program_id = '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP' THEN 'withdraw' -- dje
            WHEN ARRAY_SIZE(
                e.instruction :accounts
            ) = 11
            AND program_id = 'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1' THEN 'withdraw'
            ELSE NULL
        END AS action,
        signers
    FROM
        base_events e
        INNER JOIN {{ ref('silver__transactions') }}
        t
        ON t.tx_id = e.tx_id
        AND t.block_timestamp :: DATE = e.block_timestamp :: DATE
    WHERE
        program_id IN (
            'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc',
            '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP',
            'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1'
        )
        AND inner_instruction_program_ids [0] <> 'DecZY86MU5Gj7kppfUCEmd4LbXXuyZH1yHaP2NTqdiZB'
        AND action IS NOT NULL

{% if is_incremental() %}
AND t._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND t.block_timestamp :: DATE >= '2021-12-14'
{% endif %}
),
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
    block_timestamp :: DATE >= '2021-12-14'
{% endif %}
),
base_post_token_balances AS (
    SELECT
        pb.*
    FROM
        {{ ref('silver___post_token_balances') }}
        pb
        INNER JOIN (
            SELECT
                DISTINCT tx_id
            FROM
                dex_lp_txs
        ) d
        ON d.tx_id = pb.tx_id

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
    block_timestamp :: DATE >= '2021-12-14'
{% endif %}
),
lp_transfers_temp AS(
    SELECT
        A.block_id,
        A.block_timestamp,
        A.tx_id,
        COALESCE(SPLIT_PART(INDEX :: text, '.', 1) :: INT, INDEX :: INT) AS INDEX,
        COALESCE(SPLIT_PART(INDEX :: text, '.', 2), NULL) AS inner_index,
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
liquidity_provider_map_temp AS(
    SELECT
        e.tx_id,
        e.instruction :accounts [0] :: STRING AS liquidity_provider
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
        program_id IN (
            '8rGFmebhhTikfJP5bUe2uLHcejSiukdJhiLEKoit962a',
            'E16pm4Z4jiFxVEeBcSuYfJHy6TQYfYRAhGYt7cEUYfEV'
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
account_mappings AS (
    SELECT
        *
    FROM
        multisig_account_mapping
    UNION
    SELECT
        tx_id,
        account AS associated_account,
        owner
    FROM
        base_post_token_balances
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
    UNION
    SELECT
        e.tx_id,
        e.instruction :parsed :info :delegate :: STRING AS associated_account,
        e.instruction :parsed :info :owner :: STRING AS owner
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
            e.program_id = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
            AND e.event_type = 'approve'
        )
),
lp_transfers_with_amounts AS(
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
        COALESCE(
            sm.liquidity_provider,
            e.liquidity_provider
        ) AS tmp_liquidity_provider,
        CASE
            WHEN e.action = 'lp_action' THEN e.instruction :accounts [0] :: STRING
            WHEN e.program_id IN (
                '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP',
                'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1'
            ) THEN e.instruction :accounts [0] :: STRING
            ELSE NULL
        END AS liquidity_pool_address,
        -- lp token address
        CASE
            -- 9w95
            WHEN action = 'deposit'
            AND e.program_id = '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP' THEN e.instruction :accounts [7] :: STRING
            WHEN action = 'withdraw'
            AND e.program_id = '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP' THEN e.instruction :accounts [4] :: STRING -- dje
            -- for withdrawals but need examples for deposits...
            WHEN e.program_id = 'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1' THEN e.instruction :accounts [3] :: STRING
            ELSE NULL
        END AS lp_mint_address,
        -- lp amount
        CASE
            -- 9w95
            WHEN e.program_id IN (
                'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1',
                '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP'
            ) THEN ii.value :parsed :info :amount :: INT
            WHEN e.program_id = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc' THEN 1
            ELSE NULL
        END AS lp_amount,
        e.signers,
        e.program_id,
        e.action -- ii.*
    FROM
        lp_transfers_temp s
        INNER JOIN dex_lp_txs e
        ON s.tx_id = e.tx_id
        AND s.index = e.index
        LEFT JOIN TABLE(FLATTEN(inner_instruction :instructions)) ii -- on s.inner_index = ii.index
        LEFT OUTER JOIN liquidity_provider_map_temp sm
        ON s.tx_id = sm.tx_id
    WHERE
        ii.value :parsed :type :: STRING IN(
            'burn',
            'mintTo'
        )
        OR (
            e.program_id = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc'
            AND s.inner_index = ii.index
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
        s.signers,
        s.program_id
    FROM
        lp_transfers_with_amounts s
        LEFT OUTER JOIN account_mappings m1
        ON s.tx_id = m1.tx_id
        AND s.tx_from = m1.associated_account
        AND s.tx_to <> m1.owner
        LEFT OUTER JOIN account_mappings m2
        ON s.tx_id = m2.tx_id
        AND s.tx_to = m2.associated_account
        AND s.tx_from <> m2.owner
),
cnt_distinct_tx_from AS (
    SELECT
        COUNT(
            DISTINCT tx_from
        ) AS cnt_tx_from,
        tx_id
    FROM
        lp_actions_w_destination
    GROUP BY
        tx_id
),
lp_actions_filtered AS(
    SELECT
        l.*
    FROM
        lp_actions_w_destination l
        INNER JOIN cnt_distinct_tx_from C
        ON l.tx_id = C.tx_id
        AND C.cnt_tx_from = 1
),
multi_signer_liquidity_provider AS (
    SELECT
        tx_id,
        silver.udf_get_multi_signers_swapper(ARRAY_AGG(tx_from), ARRAY_AGG(tx_to), ARRAY_AGG(signers) [0]) AS liquidity_provider
    FROM
        lp_actions_filtered
    WHERE
        succeeded
        AND ARRAY_SIZE(signers) > 1
        AND tmp_liquidity_provider IS NULL
    GROUP BY
        1
),
lp_actions_with_liquidity_provider AS(
    SELECT
        s.*,
        COALESCE(
            s.tmp_liquidity_provider,
            m.liquidity_provider
        ) AS liquidity_provider
    FROM
        lp_actions_filtered s
        LEFT OUTER JOIN multi_signer_liquidity_provider m
        ON s.tx_id = m.tx_id
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    program_id,
    CASE
        WHEN action = 'lp_action'
        AND tx_to = liquidity_provider THEN 'withdraw'
        WHEN action = 'lp_action'
        AND tx_from = liquidity_provider THEN 'deposit'
        ELSE action
    END AS action,
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
    lp_actions_with_liquidity_provider
UNION
    -- get the lp mint/amount as separate records
SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    program_id,
    CASE
        WHEN action = 'deposit' THEN 'mint_LP_tokens'
        WHEN action = 'withdraw' THEN 'burn_LP_tokens'
        WHEN action = 'lp_action'
        AND tx_from = liquidity_provider THEN 'mint_LP_tokens'
        WHEN action = 'lp_action'
        AND tx_to = liquidity_provider THEN 'burn_LP_tokens'
        ELSE NULL
    END AS action,
    liquidity_provider,
    liquidity_pool_address,
    lp_amount AS amount,
    lp_mint_address AS mint,
    3 AS action_index,
    _inserted_timestamp
FROM
    lp_actions_with_liquidity_provider
ORDER BY
    tx_id,
    action_index
