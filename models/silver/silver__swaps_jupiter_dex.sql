{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', block_id, tx_id)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH jupiter_dex_txs AS (

    SELECT
        DISTINCT i.block_id,
        i.block_timestamp,
        i.tx_id,
        t.fee,
        t.succeeded,
        t.signers
    FROM
        {{ ref('silver___instructions') }}
        i
        INNER JOIN {{ ref('silver__transactions') }}
        t
        ON t.tx_id = i.tx_id
    WHERE
        i.value :programId :: STRING = 'JUP2jxvXaqu7NQY1GmNF4m1vodw12LVXYxbFL2uJvfo' -- jupiter aggregator v2
        AND i.block_id > 111442741 -- token balances owner field not guaranteed to populated bofore this slot

{% if is_incremental() %}
AND i._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
AND t._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
signers AS (
    SELECT
        t.tx_id,
        s.value :: STRING AS acct,
        s.index
    FROM
        jupiter_dex_txs t,
        TABLE(FLATTEN(t.signers)) s qualify(ROW_NUMBER() over (PARTITION BY t.tx_id
    ORDER BY
        s.index DESC)) = 1
),
post_balances_acct_map AS (
    SELECT
        b.tx_id,
        b.account AS middle_acct,
        b.owner,
        b.mint,
        b.decimal,
        b.amount
    FROM
        {{ ref('silver___post_token_balances') }}
        b
        INNER JOIN jupiter_dex_txs t
        ON t.tx_id = b.tx_id

{% if is_incremental() %}
WHERE
    b._inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
destinations AS (
    SELECT
        e.block_id,
        e.block_timestamp,
        e.tx_id,
        t.succeeded,
        e.index,
        ii.index AS inner_index,
        ii.value :parsed :info :destination :: STRING AS destination,
        ii.value :parsed :info :authority :: STRING AS authority,
        ii.value :parsed :info :source :: STRING AS source,
        ii.value :parsed :info :amount AS amount,
        ROW_NUMBER() over (
            PARTITION BY e.tx_id
            ORDER BY
                e.index,
                inner_index
        ) AS rn,
        e._inserted_timestamp
    FROM
        {{ ref('silver__events') }}
        e
        INNER JOIN jupiter_dex_txs t
        ON t.tx_id = e.tx_id
        LEFT OUTER JOIN TABLE(FLATTEN(inner_instruction :instructions)) ii
    WHERE
        destination IS NOT NULL
        AND COALESCE(
            ii.value :programId :: STRING,
            ''
        ) <> '11111111111111111111111111111111'
        AND e.program_id = 'JUP2jxvXaqu7NQY1GmNF4m1vodw12LVXYxbFL2uJvfo'

{% if is_incremental() %}
AND e._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
destination_acct_map AS (
    SELECT
        tx_id,
        authority,
        source
    FROM
        destinations
    GROUP BY
        1,
        2,
        3
),
swaps_tmp_1 AS (
    SELECT
        s.acct AS swapper,
        COALESCE(
            p1.owner,
            d2.authority
        ) AS destination_owner,
        COALESCE(
            p1.mint,
            p2.mint
        ) AS mint,
        COALESCE(
            p1.decimal,
            p2.decimal
        ) AS DECIMAL,
        d.*
    FROM
        destinations d
        LEFT OUTER JOIN signers s
        ON s.acct = d.authority
        AND s.tx_id = d.tx_id
        LEFT OUTER JOIN post_balances_acct_map p1
        ON p1.middle_acct = d.destination
        AND p1.tx_id = d.tx_id
        LEFT OUTER JOIN post_balances_acct_map p2
        ON p2.middle_acct = d.source
        AND p2.tx_id = d.tx_id
        LEFT OUTER JOIN destination_acct_map d2
        ON d2.source = d.destination
        AND d2.tx_id = d.tx_id
),
swapper_min_rn AS (
    SELECT
        DISTINCT s.tx_id,
        MIN(rn) over (
            PARTITION BY s.tx_id
        ) AS min_swapper_rn
    FROM
        swaps_tmp_1 s
    WHERE
        swapper IS NOT NULL
),
swaps_tmp AS (
    SELECT
        s.block_id,
        s.block_timestamp,
        s.tx_id,
        s.succeeded,
        s.swapper,
        s.destination_owner,
        s.mint,
        s.decimal,
        s.index,
        s.inner_index,
        s.destination,
        s.authority,
        s.source,
        s.amount,
        s._inserted_timestamp,
        ROW_NUMBER() over (
            PARTITION BY s.tx_id
            ORDER BY
                s.index,
                s.inner_index
        ) AS rn
    FROM
        swaps_tmp_1 s
        INNER JOIN swapper_min_rn m
        ON s.tx_id = m.tx_id
    WHERE
        s.rn >= m.min_swapper_rn
),
mint_acct_map AS (
    SELECT
        tx_id,
        source,
        mint,
        DECIMAL
    FROM
        swaps_tmp
    GROUP BY
        1,
        2,
        3,
        4
),
swap_actions AS (
    SELECT
        s1.block_id,
        s1.block_timestamp,
        s1.tx_id,
        s1.index,
        s1.succeeded,
        s1.swapper,
        s1.destination_owner,
        s1.destination,
        s1.source,
        COALESCE(
            s1.mint,
            s2.mint
        ) AS mint,
        COALESCE(
            s1.decimal,
            s2.decimal
        ) AS DECIMAL,
        s1.amount :: bigint AS amount,
        s1.rn,
        s1._inserted_timestamp
    FROM
        swaps_tmp s1
        LEFT OUTER JOIN mint_acct_map s2
        ON s1.destination = s2.source
        AND s1.tx_id = s2.tx_id
),
swap_actions_with_refund AS (
    SELECT
        s1.*,
        s3.mint AS originating_mint,
        CASE
            WHEN s2.amount < s1.amount THEN s2.amount
            ELSE NULL
        END AS refund,
        MAX(refund) over (
            PARTITION BY s1.tx_id
        ) AS max_refund,
        s1.amount - COALESCE(
            refund,
            0
        ) AS final_amt
    FROM
        swap_actions s1
        LEFT OUTER JOIN swap_actions s2
        ON s1.tx_id = s2.tx_id
        AND s1.swapper = s2.destination_owner
        AND s1.mint = s2.mint
        AND s1.rn = 1
        LEFT OUTER JOIN swap_actions s3
        ON s1.tx_id = s3.tx_id
        AND s3.rn = 1
),
lifinity_swap_extra_fee AS (
    SELECT
        tx_id,
        INDEX,
        instruction :accounts [14] :: STRING AS fee_collector,
        instruction :accounts [15] :: STRING AS fee_collector2
    FROM
        {{ ref('silver__events') }}
    WHERE
        program_id = 'JUP2jxvXaqu7NQY1GmNF4m1vodw12LVXYxbFL2uJvfo'
        AND instruction :accounts [0] :: STRING = 'EewxydAPCCVuNEyrVN68PuSYdQ7wKn27V9Gjeoi8dy3S'
        AND (
            instruction :data :: STRING LIKE '1M6zJqvfqkr%'
            OR instruction :data :: STRING LIKE '1UEN8S1i2c%'
        )
        AND ARRAY_SIZE(
            instruction :accounts
        ) > 14

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
swap_actions_final AS (
    SELECT
        s.*,
        f.fee_collector,
        f2.fee_collector2
    FROM
        swap_actions_with_refund s
        LEFT OUTER JOIN lifinity_swap_extra_fee f
        ON s.tx_id = f.tx_id
        AND s.index = f.index
        AND s.destination = f.fee_collector
        LEFT OUTER JOIN lifinity_swap_extra_fee f2
        ON s.tx_id = f2.tx_id
        AND s.index = f2.index
        AND s.destination = f2.fee_collector2
    WHERE
        (
            swapper IS NOT NULL
            OR mint <> originating_mint
            OR (
                originating_mint = mint
                AND max_refund IS NULL
            ) -- need to do this for situations where it appears the user swaps back to the same mint...
        )
        AND f.fee_collector IS NULL
        AND f2.fee_collector2 IS NULL
),
agg_tmp AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        succeeded,
        swapper,
        mint,
        DECIMAL,
        _inserted_timestamp,
        SUM(final_amt) AS amt,
        MIN(rn) AS rn
    FROM
        swap_actions_final
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8
),
agg AS (
    SELECT
        *,
        MAX(rn) over (
            PARTITION BY tx_id
        ) AS max_rn
    FROM
        agg_tmp
    WHERE
        amt <> 0
)
SELECT
    a1.block_id,
    a1.block_timestamp,
    a1.tx_id,
    a1.succeeded,
    a1.swapper,
    a1.mint AS from_mint,
    CASE
        WHEN a1.succeeded THEN a1.amt * pow(
            10,- a1.decimal
        )
        ELSE 0
    END AS from_amt,
    a2.mint AS to_mint,
    CASE
        WHEN a1.succeeded THEN a2.amt * pow(
            10,- a2.decimal
        )
        ELSE 0
    END AS to_amt,
    a1._inserted_timestamp
FROM
    agg a1
    LEFT OUTER JOIN agg a2
    ON a1.tx_id = a2.tx_id
    AND a1.rn <> a2.rn
    AND a2.rn = a2.max_rn
WHERE
    a1.rn = 1
    AND to_amt IS NOT NULL
