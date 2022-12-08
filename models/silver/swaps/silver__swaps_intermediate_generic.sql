{{ config(
    materialized = 'incremental',
    unique_key = ["block_id","tx_id","swap_index"],
    merge_predicates = ["DBT_INTERNAL_DEST.block_timestamp::date >= LEAST(current_date-7,(select min(block_timestamp)::date from {{ this }}__dbt_tmp))"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

WITH base_events AS(

    SELECT
        *
    FROM
        {{ ref('silver__events') }}
    WHERE
        (
            program_id IN (
                --jupiter v2/v3 program_ids
                'JUP2jxvXaqu7NQY1GmNF4m1vodw12LVXYxbFL2uJvfo',
                'JUP3c2Uh3WA4Ng34tw6kPd2G4C5BB21Xo36Je1s32Ph',
                -- saber
                'Crt7UoUR6QgrFrN7j8rmSQpUTNWNSitSwWvsWGf1qZ5t',
                'SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ',
                --program ids for acct mapping
                'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL',
                'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
                --solrise program id (small program but for completenesss...)
                'SLrSmK5ykEhdLkZH8mEsrZsGtDvzrQLKYSwy7PVKQoj'
            )
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
        IFF(array_size(signers) = 1, signers[0]::STRING, NULL) AS swapper,
        signers
    FROM
        base_events e
        INNER JOIN {{ ref('silver__transactions') }}
        t
        ON t.tx_id = e.tx_id
        AND t.block_timestamp :: DATE = e.block_timestamp :: DATE
    WHERE
        (
            program_id IN (
                -- saber
                'Crt7UoUR6QgrFrN7j8rmSQpUTNWNSitSwWvsWGf1qZ5t',
                'SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ'
            ) -- jupiter v2,v3
            OR (
                program_id IN (
                    'JUP2jxvXaqu7NQY1GmNF4m1vodw12LVXYxbFL2uJvfo',
                    'JUP3c2Uh3WA4Ng34tw6kPd2G4C5BB21Xo36Je1s32Ph'
                )
                AND ARRAY_SIZE(
                    e.instruction :accounts
                ) > 6
            )
        )
        AND inner_instruction_program_ids [0] <> 'DecZY86MU5Gj7kppfUCEmd4LbXXuyZH1yHaP2NTqdiZB' --associated with wrapping of tokens

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
        {{ ref('silver__transfers') }}
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
                dex_txs
        )
),
delegates_mappings as (
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
            WHERE program_id = 'SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ'
        ) d
        ON d.tx_id = e.tx_id
    WHERE
        (
            e.program_id = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
            AND e.event_type = 'approve'
        )
),
account_mappings AS (
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
        dm.*
    FROM
        delegates_mappings dm
        INNER JOIN dex_txs dt
        ON dm.tx_id = dm.tx_id
        AND dt.instruction :accounts [2] :: STRING = dm.associated_account
    WHERE
        dt.program_id = 'SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ'
    UNION
    SELECT
        e.tx_id,
        e.instruction :accounts[3] :: STRING AS associated_account,
        e.instruction :accounts[0] :: STRING AS owner
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
        e.program_id = 'SLrSmK5ykEhdLkZH8mEsrZsGtDvzrQLKYSwy7PVKQoj'
    AND instruction:data::string = 't'
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
        e.swapper as tmp_swapper,
        e.signers,
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
multi_signer_swapper as (
    select 
        tx_id,
        silver.udf_get_multi_signers_swapper(array_agg(tx_from), array_agg(tx_to), array_agg(signers)[0]) as swapper
    from swaps_w_destination
    where succeeded
    and array_size(signers) > 1
    and tmp_swapper is null
    group by 1
),
unique_tx_from_and_to AS (
    SELECT
        tx_id,
        INDEX,
        COUNT(DISTINCT(tx_from)) AS num_tx_from,
        COUNT(DISTINCT(tx_to)) AS num_tx_to
    FROM
        swaps_w_destination
    GROUP BY
        1,
        2
    HAVING
        num_tx_from > 1
        AND num_tx_to > 1
),
swaps_filtered_temp AS(
    SELECT
        s.*,
        coalesce(s.tmp_swapper,m.swapper) as swapper
    FROM
        swaps_w_destination s
        INNER JOIN unique_tx_from_and_to u
        ON s.tx_id = u.tx_id
        AND s.index = u.index
        LEFT OUTER JOIN multi_signer_swapper m
        ON s.tx_id = m.tx_id
),
min_inner_index_of_swapper AS(
    SELECT
        tx_id,
        INDEX,
        MIN(inner_index) AS min_inner_index_swapper
    FROM
        swaps_filtered_temp
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
        swaps_filtered_temp d
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
    (COALESCE(
        to_amt,
        0
    ) > 0
    OR COALESCE(
        from_amt,
        0
    ) > 0)
AND program_id is not null
