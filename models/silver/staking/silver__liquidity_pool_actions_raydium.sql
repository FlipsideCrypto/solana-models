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
            '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
            --program ids for acct mapping
            'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL',
            'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
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
        signers [0] :: STRING AS swapper,
        IFF(ARRAY_SIZE(instruction :accounts) > 21, 'withdraw', 'deposit') AS action
    FROM
        base_events e
        INNER JOIN {{ ref('silver__transactions') }}
        t
        ON t.tx_id = e.tx_id
        AND t.block_timestamp :: DATE = e.block_timestamp :: DATE
    WHERE
        (
            --deposits/add liquidity
            program_id = '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8'
            AND (
                instruction :accounts [2] :: STRING = '5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1'
                AND ARRAY_SIZE(
                    instruction :accounts
                ) IN (
                    12,
                    13
                )
            )
        ) -- withdraws/remove liquidity
        OR (
            (
                program_id = '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8'
                AND instruction :accounts [0] :: STRING = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
                AND instruction :accounts [2] :: STRING = '5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1'
                AND instruction :accounts [10] :: STRING = '9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin'
                AND ARRAY_SIZE(
                    instruction :accounts
                ) > 21
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
        s.succeeded,
        s._inserted_timestamp,
        e.action,
        e.program_id,
        CASE
            WHEN action = 'deposit' THEN e.instruction :accounts [8] :: STRING
            WHEN action = 'withdraw' THEN e.instruction :accounts [11] :: STRING
            ELSE NULL
        END AS stake_pool,
        e.instruction :accounts [5] :: STRING AS address,
        ii.value :parsed :info :amount :: INT AS lp_amount
    FROM
        lp_transfers_temp s
        LEFT OUTER JOIN dex_lp_txs e
        ON s.tx_id = e.tx_id
        AND s.index = e.index
        LEFT OUTER JOIN TABLE(FLATTEN(inner_instruction :instructions)) ii
        LEFT OUTER JOIN account_mappings m1
        ON s.tx_id = m1.tx_id
        AND s.tx_from = m1.associated_account
        AND s.tx_to <> m1.owner
        LEFT OUTER JOIN account_mappings m2
        ON s.tx_id = m2.tx_id
        AND s.tx_to = m2.associated_account
        AND s.tx_from <> m2.owner
    WHERE
        ii.value :parsed :type :: STRING IN(
            'burn',
            'mintTo'
        )
        AND s.program_id <> '11111111111111111111111111111111'
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    program_id,
    inner_index,
    action,
    stake_pool,
    address,
    lp_amount,
    amount,
    mint,
    -- ROW_NUMBER() over (
    --     PARTITION BY tx_id
    --     ORDER BY
    --         rn
    -- ) AS swap_index
     _inserted_timestamp
FROM
    lp_actions_w_destination
WHERE
    COALESCE(
        amount,
        0
    ) > 0
