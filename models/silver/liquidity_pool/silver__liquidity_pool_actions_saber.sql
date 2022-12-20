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
        program_id IN (
            'SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ',
            'DecZY86MU5Gj7kppfUCEmd4LbXXuyZH1yHaP2NTqdiZB',
            --program ids for acct mapping
            'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL',
            'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
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
        signers [0] :: STRING AS liquidity_provider,
        signers
    FROM
        base_events e
        INNER JOIN {{ ref('silver__transactions') }}
        t
        ON t.tx_id = e.tx_id
        AND t.block_timestamp :: DATE = e.block_timestamp :: DATE
    WHERE
        (
            program_id = 'DecZY86MU5Gj7kppfUCEmd4LbXXuyZH1yHaP2NTqdiZB'
            OR (
                program_id = 'SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ'
                AND ARRAY_SIZE(
                    e.instruction :accounts
                ) IN (
                    10,
                    11,
                    12
                )
                AND e.instruction :accounts [9] :: STRING <> 'SysvarC1ock11111111111111111111111111111111'
            )
        )

{% if is_incremental() %}
AND t._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND t.block_timestamp :: DATE >= '2021-05-27'
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
    block_timestamp :: DATE >= '2021-05-27'
{% endif %}
),
-- base_post_token_balances AS (
--     SELECT
--         pb.*
--     FROM
--         {{ ref('silver___post_token_balances') }}
--         pb
--         INNER JOIN (
--             SELECT
--                 DISTINCT tx_id
--             FROM
--                 dex_lp_txs
--         ) d
--         ON d.tx_id = pb.tx_id

-- {% if is_incremental() %}
-- WHERE
--     _inserted_timestamp >= (
--         SELECT
--             MAX(_inserted_timestamp)
--         FROM
--             {{ this }}
--     )
-- {% else %}
-- WHERE
--     block_timestamp :: DATE >= '2022-01-24'
-- {% endif %}
-- ),
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
-- delegates_mappings AS (
--     SELECT
--         e.tx_id,
--         e.instruction :parsed :info :delegate :: STRING AS associated_account,
--         e.instruction :parsed :info :owner :: STRING AS owner
--     FROM
--         base_events e
--         INNER JOIN (
--             SELECT
--                 DISTINCT tx_id
--             FROM
--                 dex_lp_txs
--             WHERE
--                 program_id = 'SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ'
--         ) d
--         ON d.tx_id = e.tx_id
--     WHERE
--         (
--             e.program_id = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
--             AND e.event_type = 'approve'
--         )
-- ),
account_mappings AS (
    -- SELECT
    --     tx_id,
    --     account AS associated_account,
    --     owner
    -- FROM
    --     base_post_token_balances
    -- UNION
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
    -- UNION
    -- SELECT
    --     dm.*
    -- FROM
    --     delegates_mappings dm
    --     INNER JOIN dex_lp_txs dt
    --     ON dm.tx_id = dm.tx_id
    --     AND dt.instruction :accounts [2] :: STRING = dm.associated_account
    -- WHERE
    --     dt.program_id = 'SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ'
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
        e.liquidity_provider,
        e.program_id,
        ii.value :parsed :type :: STRING AS action,
        CASE
            WHEN e.program_id = 'DecZY86MU5Gj7kppfUCEmd4LbXXuyZH1yHaP2NTqdiZB' THEN e.instruction :accounts [1] :: STRING
            ELSE NULL
        END AS temp_wrapped_mint,
        CASE
            -- 9w95
            WHEN e.program_id = 'SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ'
            AND action = 'burn' THEN e.instruction :accounts [3] :: STRING
            WHEN e.program_id = 'SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ'
            AND action = 'mintTo' THEN e.instruction :accounts [7] :: STRING
            ELSE NULL
        END AS lp_mint_address,
        e.instruction :accounts [0] :: STRING AS liquidity_pool_address,
        ii.value :parsed :info :amount :: INT AS lp_amount
    FROM
        lp_transfers_temp s
        INNER JOIN dex_lp_txs e
        ON s.tx_id = e.tx_id
        AND s.index = e.index
        LEFT JOIN TABLE(FLATTEN(inner_instruction :instructions)) ii
    WHERE
        ii.value :parsed :type :: STRING IN(
            'burn',
            'mintTo'
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
        s.liquidity_provider,
        s.liquidity_pool_address,
        s.lp_mint_address,
        s.lp_amount,
        s.temp_wrapped_mint,
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
    WHERE
        (
            action = 'burn'
            AND liquidity_provider = tx_to
        )
        OR (
            action = 'mintTo'
            AND liquidity_provider = tx_from
        )
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
            l2.amount,
            l1.amount
        ) AS amount,
        COALESCE(
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
        lp_actions_w_destination l1
        LEFT JOIN lp_actions_w_destination l2
        ON l1.tx_id = l2.tx_id
        AND l1.lp_mint_address IS NOT NULL
        AND l2.lp_mint_address IS NULL
        AND l1.mint = l2.temp_wrapped_mint
    WHERE
        l1.program_id <> 'DecZY86MU5Gj7kppfUCEmd4LbXXuyZH1yHaP2NTqdiZB'
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
        -- retrieving the lp mint/amount as separate records
    SELECT
        l.block_id,
        l.block_timestamp,
        l.tx_id,
        l.succeeded,
        l.program_id,
        CASE
            WHEN l.action = 'mintTo' THEN 'mint_LP_tokens'
            WHEN l.action = 'burn' THEN 'burn_LP_tokens'
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
        LEFT OUTER JOIN {{ ref('silver__token_metadata') }}
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
    _inserted_timestamp
FROM
    temp_final
WHERE
    COALESCE(
        amount,
        0
    ) > 0
