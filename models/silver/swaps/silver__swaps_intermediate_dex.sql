{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', block_id, tx_id)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH dex_txs AS (

    SELECT
        e.*,
        CASE
            WHEN program_id = 'JUP2jxvXaqu7NQY1GmNF4m1vodw12LVXYxbFL2uJvfo' THEN COALESCE(
                signers [1],
                signers [0]
            ) :: STRING
            ELSE signers [0] :: STRING
        END AS swapper
    FROM
        {{ ref('silver__events') }}
        e
        INNER JOIN {{ ref('silver__transactions') }}
        t
        ON t.tx_id = e.tx_id
        AND t.block_timestamp :: DATE = e.block_timestamp :: DATE
    WHERE
        (
            program_id IN (
                -- jupiter v2,v3,v4
                'JUP2jxvXaqu7NQY1GmNF4m1vodw12LVXYxbFL2uJvfo',
                'JUP3c2Uh3WA4Ng34tw6kPd2G4C5BB21Xo36Je1s32Ph',
                -- 'JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB',
                -- Orca
                'MEV1HDn99aybER3U3oa9MySSXqoEZNDEQ4miAimTjaW',
                '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP',
                'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1',
                -- saber
                'Crt7UoUR6QgrFrN7j8rmSQpUTNWNSitSwWvsWGf1qZ5t'
            ) -- raydium
            OR (
                program_id = '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8'
                AND instruction :accounts [2] :: STRING = '5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1'
            )
            OR (
                program_id = '5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h'
                AND instruction :accounts [1] :: STRING = '2EXiumdi14E9b8Fy62QcA5Uh6WdHS2b38wtSxp72Mibj'
            )
            OR program_id IN (
                '93BgeoLHo5AdNbpqy9bD12dtfxtA5M2fh3rj72bE35Y3',
                'routeUGWgWzqBWFcrCfv8tritsqukccJPu3q5GPP3xS'
            )
        )
        AND ARRAY_SIZE(
            e.instruction :accounts
        ) > 6
        AND e.block_id > 111442741
        AND inner_instruction_program_ids [0] <> 'DecZY86MU5Gj7kppfUCEmd4LbXXuyZH1yHaP2NTqdiZB' --associated with wrapping of tokens

{% if is_incremental() %}
AND e.block_timestamp :: DATE = '2022-07-27' -- AND e._inserted_timestamp >= (
--     SELECT
--         MAX(_inserted_timestamp)
--     FROM
--         {{ this }}
-- )
-- AND t._inserted_timestamp >= (
--     SELECT
--         MAX(_inserted_timestamp)
--     FROM
--         {{ this }}
-- )
{% else %}
    AND e.block_timestamp :: DATE >= '2021-12-14'
{% endif %}
),
base_transfers AS (
    SELECT
        *
    FROM
        {{ ref('silver__transfers2') }}
        tr

{% if is_incremental() %}
WHERE
    block_timestamp :: DATE = '2022-07-27' -- WHERE _inserted_timestamp >= (
    --     SELECT
    --         MAX(_inserted_timestamp)
    --     FROM
    --         {{ this }}
    -- )
{% else %}
WHERE
    block_timestamp :: DATE >= '2021-12-14'
{% endif %}
),
base_post_token_balances AS (
    SELECT
        *
    FROM
        {{ ref('silver___post_token_balances') }}

{% if is_incremental() %}
WHERE
    block_timestamp :: DATE = '2022-07-27' -- WHERE _inserted_timestamp >= (
    --     SELECT
    --         MAX(_inserted_timestamp)
    --     FROM
    --         {{ this }}
    -- )
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
raydium_account_mapping as(
        select 
        tx_id,
        ii.value:parsed:info:account::string as associated_account,
        coalesce(
            ii.value:parsed:info:source::string,
            ii.value:parsed:info:owner::string) as owner
    from dex_txs as d
    LEFT OUTER JOIN TABLE(FLATTEN(inner_instruction :instructions)) ii
    WHERE
        d.program_id IN (
            '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
            '5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h',
            '93BgeoLHo5AdNbpqy9bD12dtfxtA5M2fh3rj72bE35Y3',
            'routeUGWgWzqBWFcrCfv8tritsqukccJPu3q5GPP3xS'
        )

    and associated_account is not null
    
),
account_mappings AS (
        
    SELECT
        * 
    FROM 
        raydium_account_mapping
    union
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
        tx_id,
        instruction :parsed :info :account :: STRING AS associated_account,
        COALESCE(
            instruction :parsed :info :source :: STRING,
            instruction :parsed :info :owner :: STRING
        ) AS owner
    FROM
        solana_dev.silver.events
    WHERE
        (
            program_id = 'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL'
            AND event_type = 'create'
        )
        OR (
            program_id = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
            AND event_type = 'closeAccount'
        )


{% if is_incremental() %}
AND block_timestamp :: DATE = '2022-07-27' -- AND _inserted_timestamp >= (
--     SELECT
--         MAX(_inserted_timestamp)
--     FROM
--         {{ this }}
-- )
{% else %}
    AND block_timestamp :: DATE >= '2021-12-14'
{% endif %}
),
delegate_mappings AS(
    SELECT
        tx_id,
        instruction :parsed :info :delegate :: STRING AS delegate,
        instruction :parsed :info :owner :: STRING AS delegate_owner
    FROM
        solana_dev.silver.events
    WHERE
        (
            program_id = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
            AND event_type = 'approve'
        )

{% if is_incremental() %}
AND block_timestamp :: DATE = '2022-07-27' -- AND _inserted_timestamp >= (
--     SELECT
--         MAX(_inserted_timestamp)
--     FROM
--         {{ this }}
-- )
{% else %}
    AND block_timestamp :: DATE >= '2021-12-14'
{% endif %}
),
swaps_w_destination AS (
    SELECT
        s.*,
        e.swapper,
        ii.value :parsed :info :destination :: STRING AS destination,
        ii.value :parsed :info :authority :: STRING AS authority,
        ii.value :parsed :info :source :: STRING AS source
    FROM
        swaps_temp s
        LEFT OUTER JOIN dex_txs e
        ON s.tx_id = e.tx_id
        AND s.index = e.index
        LEFT OUTER JOIN TABLE(FLATTEN(inner_instruction :instructions)) ii
    WHERE
        destination IS NOT NULL
        AND s.inner_index = ii.index
        AND COALESCE(
            ii.value :programId :: STRING,
            ''
        ) <> '11111111111111111111111111111111'
),
swaps_w_tx_from_owner AS (
    SELECT
        s.*,
        COALESCE(
            m.delegate_owner,
            s.tx_from
        ) AS tx_from_owner
    FROM
        swaps_w_destination AS s
        LEFT OUTER JOIN delegate_mappings AS m
        ON s.tx_id = m.tx_id
        AND s.tx_from = m.delegate
),
min_idx_of_swapper AS(
    SELECT
        tx_id,
        INDEX,
        MIN(inner_index) AS min_index_swapper
    FROM
        swaps_w_tx_from_owner
    WHERE
        tx_from_owner = swapper
    GROUP BY
        1,
        2
),
swaps AS(
    SELECT
        d.*,
        m.min_index_swapper,
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
        LEFT JOIN min_idx_of_swapper m
        ON m.tx_id = d.tx_id
        AND m.index = d.index
),
final_temp AS (
    SELECT
        s1.*,
        s2.mint AS to_mint,
        s2.amount AS to_amt
    FROM
        swaps s1
        LEFT OUTER JOIN swaps s2
        ON s1.tx_id = s2.tx_id
        AND s1.index = s2.index
        AND s1.inner_index <> s2.inner_index
        LEFT OUTER JOIN account_mappings m
        ON m.tx_id = s2.tx_id
        AND m.associated_account = s2.destination
    WHERE
        s1.inner_index = s1.min_index_swapper
        AND s1.swapper IN (
            s2.destination,
            m.owner
        )
        AND s1.mint <> s2.mint
    UNION
    SELECT
        s1.*,
        NULL,
        NULL
    FROM
        swaps s1
    WHERE
        s1.inner_index <> s1.min_index_swapper
        AND s1.tx_from = s1.swapper
    UNION
    SELECT
        s1.block_id,
        s1.block_timestamp,
        s1.tx_id,
        s1.index,
        s1.inner_index,
        s1.tx_from,
        s1.tx_to,
        NULL AS amount,
        NULL AS mint,
        s1.succeeded,
        s1._inserted_timestamp,
        s1.swapper,
        s1.destination,
        s1.authority,
        s1.source,
        s1.min_index_swapper,
        s1.rn,
        s1.inner_rn,
        s1.mint AS to_mint,
        s1.amount AS to_amt
    FROM
        swaps s1
        LEFT OUTER JOIN swaps s2
        ON s1.tx_id = s2.tx_id
        AND s1.index = s2.index
        AND s2.inner_index = s2.min_index_swapper
        LEFT OUTER JOIN account_mappings m
        ON m.tx_id = s1.tx_id
        AND m.associated_account = s1.tx_to
    WHERE
        s1.inner_index <> s1.min_index_swapper
        AND (
            m.owner = s1.swapper
            OR s1.tx_to = s1.swapper
        )
        AND s2.mint = s1.mint
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
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
