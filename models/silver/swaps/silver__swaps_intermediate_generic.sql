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
                'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
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
        CASE
            WHEN program_id = 'JUP2jxvXaqu7NQY1GmNF4m1vodw12LVXYxbFL2uJvfo' THEN COALESCE(
                signers [1],
                signers [0]
            ) :: STRING
            ELSE signers [0] :: STRING
        END AS swapper
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
        {{ ref('silver__transfers2') }}
        tr
    INNER JOIN (select distinct tx_id from dex_txs) d on d.tx_id = tr.tx_id

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
        {{ ref('silver___post_token_balances') }} pb
    INNER JOIN (select distinct tx_id from dex_txs) d on d.tx_id = pb.tx_id

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
        INNER JOIN (select distinct tx_id from dex_txs) d on d.tx_id = e.tx_id
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
    INNER JOIN (select distinct tx_id from dex_txs) d on d.tx_id = e.tx_id
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
        s.INDEX,
        s.inner_index,
        COALESCE(m1.owner, s.tx_from) as tx_from,
        COALESCE(m2.owner, s.tx_to) as tx_to,
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
        LEFT OUTER JOIN account_mappings m1 on s.tx_id = m1.tx_id and s.tx_from = m1.associated_account and s.tx_to <> m1.owner
        LEFT OUTER JOIN account_mappings m2 on s.tx_id = m2.tx_id and s.tx_to = m2.associated_account and s.tx_from <> m2.owner
    WHERE
        s.program_id <> '11111111111111111111111111111111'
),

unique_tx_from_and_to as(
    SELECT
        tx_id,
        index,
        count(distinct(tx_from)) as num_tx_from,
        count(distinct(tx_to)) as num_tx_to
from 
    swaps_w_destination
group by 1,2),

swaps_filtered_temp as(
    SELECT s.* FROM
swaps_w_destination as s
    inner join
    unique_tx_from_and_to as u
    on s.tx_id = u.tx_id and s.index = u.index
    where num_tx_from >1 or num_tx_to >1
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
        d.swapper is not null
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
