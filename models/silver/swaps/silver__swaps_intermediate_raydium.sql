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
            --raydium program_ids
            '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
            '5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h',
            '93BgeoLHo5AdNbpqy9bD12dtfxtA5M2fh3rj72bE35Y3',
            'routeUGWgWzqBWFcrCfv8tritsqukccJPu3q5GPP3xS',
            --program ids for acct mapping
            'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL',
            'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
        )
        AND block_id > 111442741 -- token balances owner field not guaranteed to be populated before this slot

{% if is_incremental() %}
-- AND block_timestamp :: DATE = '2022-11-01'
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
dex_txs AS (
    SELECT
        e.*,
        signers [0] :: STRING AS swapper
    FROM
        base_events e
        INNER JOIN {{ ref('silver__transactions') }}
        t
        ON t.tx_id = e.tx_id
        AND t.block_timestamp :: DATE = e.block_timestamp :: DATE
    WHERE
        (
            (
                (
                    program_id = '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8'
                    AND instruction :accounts [2] :: STRING = '5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1'
                    AND ARRAY_SIZE(
                        instruction :accounts
                    ) >= 17
                    AND (
                        instruction :accounts [6] :: STRING = '9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin'
                        OR instruction :accounts [7] :: STRING = '9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin'
                    )
                )
                OR (
                    program_id = '5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h'
                    AND instruction :accounts [2] :: STRING = '3uaZBfHPfmpAHW7dsimC1SnyR61X4bJqQZKWmRSCXJxv'
                    AND ARRAY_SIZE(
                        instruction :accounts
                    ) >= 17
                    AND (
                        instruction :accounts [6] :: STRING = '9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin'
                        OR instruction :accounts [7] :: STRING = '9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin'
                    )
                )
                OR program_id IN (
                    '93BgeoLHo5AdNbpqy9bD12dtfxtA5M2fh3rj72bE35Y3',
                    'routeUGWgWzqBWFcrCfv8tritsqukccJPu3q5GPP3xS'
                )
            )
        )

{% if is_incremental() %}
-- AND t.block_timestamp :: DATE = '2022-11-01'
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
        {{ ref('silver__transfers2') }}
        tr
        INNER JOIN (
            SELECT
                DISTINCT tx_id
            FROM
                dex_txs
        ) d
        ON d.tx_id = tr.tx_id

{% if is_incremental() %}
WHERE
    -- block_timestamp :: DATE = '2022-11-01'
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
                dex_txs
        ) d
        ON d.tx_id = pb.tx_id

{% if is_incremental() %}
WHERE
    -- block_timestamp :: DATE = '2022-11-01'
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
swaps_temp AS(
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
                dex_txs
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
        dex_txs AS d
        LEFT OUTER JOIN TABLE(FLATTEN(inner_instruction :instructions)) ii
    WHERE
        d.program_id IN (
            '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
            '5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h',
            '93BgeoLHo5AdNbpqy9bD12dtfxtA5M2fh3rj72bE35Y3',
            'routeUGWgWzqBWFcrCfv8tritsqukccJPu3q5GPP3xS'
        )
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
        tx_to AS associated_account,
        tx_from AS owner
    FROM
        swaps_temp
    WHERE
        amount = 0.00203928
        AND mint = 'So11111111111111111111111111111111111111112'
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
                dex_txs
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
                dex_txs
        ) d
        ON d.tx_id = e.tx_id
    WHERE
        (
            e.program_id = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
            AND e.event_type = 'approve'
        )
),
swaps_w_destination AS (
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
        e.swapper,
        e.program_id
    FROM
        swaps_temp s
        LEFT OUTER JOIN dex_txs e
        ON s.tx_id = e.tx_id
        AND s.index = e.index
        LEFT OUTER JOIN account_mappings m1
        ON s.tx_id = m1.tx_id
        AND s.tx_from = m1.associated_account
        AND s.tx_to <> m1.owner
        LEFT OUTER JOIN account_mappings m2
        ON s.tx_id = m2.tx_id
        AND s.tx_to = m2.associated_account
        AND s.tx_from <> m2.owner
    WHERE
        s.program_id <> '11111111111111111111111111111111'
),
min_inner_index_of_swapper AS(
    SELECT
        tx_id,
        INDEX,
        MIN(inner_index) AS min_inner_index_swapper
    FROM
        swaps_w_destination
    WHERE
        tx_from = swapper
    GROUP BY
        1,
        2
),
swaps AS(
    SELECT
        d.*,
        m.min_inner_index_swapper,
        ROW_NUMBER() over (
            PARTITION BY d.tx_id
            ORDER BY
                d.index,
                d.inner_index
        ) AS rn,
        ROW_NUMBER() over (
            PARTITION BY d.tx_id,
            d.index
            ORDER BY
                d.inner_index
        ) AS inner_rn
    FROM
        swaps_w_destination d
        LEFT JOIN min_inner_index_of_swapper m
        ON m.tx_id = d.tx_id
        AND m.index = d.index
    WHERE
        d.swapper IS NOT NULL
),
final_temp AS (
    SELECT
        s1.block_id,
        s1.block_timestamp,
        s1.tx_id,
        s1.succeeded,
        s1.program_id,
        s1.swapper,
        s1.mint,
        s1.amount,
        s1.rn,
        s1._inserted_timestamp,
        s2.mint AS to_mint,
        s2.amount AS to_amt
    FROM
        swaps s1
        LEFT OUTER JOIN swaps s2
        ON s1.tx_id = s2.tx_id
        AND s1.index = s2.index
        AND s1.inner_index <> s2.inner_index
    WHERE
        s1.inner_index = s1.min_inner_index_swapper
        AND s1.swapper = s2.tx_to
        AND s1.mint <> s2.mint
    UNION
    SELECT
        s1.block_id,
        s1.block_timestamp,
        s1.tx_id,
        s1.succeeded,
        s1.program_id,
        s1.swapper,
        s1.mint,
        s1.amount,
        s1.rn,
        s1._inserted_timestamp,
        NULL,
        NULL
    FROM
        swaps s1
    WHERE
        s1.inner_index <> s1.min_inner_index_swapper
        AND s1.tx_from = s1.swapper
    UNION
    SELECT
        s1.block_id,
        s1.block_timestamp,
        s1.tx_id,
        s1.succeeded,
        s1.program_id,
        s1.swapper,
        NULL AS mint,
        NULL AS amount,
        s1.rn,
        s1._inserted_timestamp,
        s1.mint AS to_mint,
        s1.amount AS to_amt
    FROM
        swaps s1
        LEFT OUTER JOIN swaps s2
        ON s1.tx_id = s2.tx_id
        AND s1.index = s2.index
        AND s2.inner_index = s2.min_inner_index_swapper
    WHERE(
            (
                s1.inner_index <> s1.min_inner_index_swapper
                AND s1.tx_to = s1.swapper
                AND s2.mint = s1.mint
            )
            OR s1.min_inner_index_swapper IS NULL
        )
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    program_id,
    swapper,
    mint AS from_mint,
    amount AS from_amt,
    to_mint,
    to_amt,
    _inserted_timestamp,
    ROW_NUMBER() over (
        PARTITION BY tx_id
        ORDER BY
            rn
    ) AS swap_index
FROM
    final_temp
WHERE
    COALESCE(
        to_amt,
        0
    ) > 0
    OR COALESCE(
        from_amt,
        0
    ) > 0
