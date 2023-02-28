{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, mint)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH base_events AS (

    SELECT
        *
    FROM
        solana.silver.events
    WHERE
        program_id = 'mmm3XBJg5gk8XJxEKBvdgptZz6SgK4tXvn36sodowMc'
        AND block_timestamp :: DATE > '2022-10-14'
),
base_token_balance AS (
    SELECT
        *
    FROM
        solana.silver._post_token_balances
    WHERE
        block_timestamp :: DATE > '2022-10-14'
),
base_transfers AS (
    SELECT
        *
    FROM
        solana.silver.transfers
    WHERE
        mint = 'So11111111111111111111111111111111111111112'
        AND block_timestamp :: DATE > '2022-10-14'
),
coral_cube_sales AS(
    SELECT
        A.*,
        CASE
            WHEN b1.owner IS NOT NULL THEN b1.owner
            WHEN b2.owner IS NOT NULL THEN b2.owner
            ELSE A.instruction :accounts [1] :: STRING
        END AS purchaser,
        CASE
            WHEN b1.owner IS NOT NULL THEN A.instruction :accounts [4] :: STRING
            ELSE A.instruction :accounts [0] :: STRING
        END AS seller,
        CASE
            WHEN b1.owner IS NOT NULL THEN 'sell'
            ELSE 'buy'
        END AS nft_sale_type,
        'Coral Cube' AS marketplace,
        A.instruction :accounts [8] :: STRING AS mint
    FROM
        base_events A
        LEFT JOIN base_token_balance b1
        ON A.tx_id = b1.tx_id
        AND A.instruction :accounts [0] = b1.owner
        LEFT JOIN base_token_balance b2
        ON A.tx_id = b2.tx_id
        AND A.instruction :accounts [4] = b2.owner
    WHERE
        A.signers [1] = '7RpRDUZBdu5hfmqWvobPazbNeVCagRk5E3Rb8Bm8qRmD'
        AND A.signers [0] <> 'AwQqQ1Xo9VxY64fcjw2toXZQnk94o5pFkxnGRngnqb1u' --filters out sell_withdraw actions
        AND ARRAY_SIZE(
            instruction :accounts
        ) > 16
),
mev2_sales AS(
    SELECT
        A.*,
        instruction :accounts [1] :: STRING AS purchaser,
        instruction :accounts [0] :: STRING AS seller,
        'buy' AS nft_sale_type,
        'Magic Eden' AS marketplace,
        CASE
            WHEN ARRAY_SIZE(
                instruction :accounts
            ) > 19 THEN instruction :accounts [7] :: STRING
            WHEN ARRAY_SIZE(
                instruction :accounts
            ) = 18 THEN instruction :accounts [8] :: STRING
        END AS mint
    FROM
        base_events A
    WHERE
        signers [1] = 'NTYeYJ1wr4bpM5xo6zx5En44SvJFAd35zTxxNoERYqd'
        AND ARRAY_SIZE(
            instruction :accounts
        ) > 16
),
mev2_nft_sale_amount AS (
    SELECT
        A.tx_id,
        b.mint,
        SUM(
            b.amount
        ) AS sales_amount
    FROM
        mev2_sales A
        LEFT OUTER JOIN base_transfers b
        ON A.tx_id = b.tx_id
    WHERE
        A.instruction :accounts [5] = b.tx_from
    GROUP BY
        1,
        2
),
coral_cube_nft_sale_amount AS (
    SELECT
        A.tx_id,
        b.mint,
        SUM(
            b.amount
        ) AS sales_amount
    FROM
        coral_cube_sales A
        LEFT OUTER JOIN base_transfers b
        ON A.tx_id = b.tx_id
    WHERE
        (
            A.nft_sale_type = 'sell'
            AND A.purchaser = b.tx_from
        )
        OR (
            A.nft_sale_type = 'buy'
            AND A.instruction :accounts [5] = b.tx_from
        )
    GROUP BY
        1,
        2
)
SELECT
    A.block_timestamp,
    A.block_id,
    A.tx_id,
    A.succeeded,
    A.program_id,
    A.mint,
    A.purchaser,
    A.seller,
    b.sales_amount,
    A.marketplace,
    A._inserted_timestamp
FROM
    coral_cube_sales A
    LEFT JOIN coral_cube_nft_sale_amount b
    ON A.tx_id = b.tx_id
UNION
SELECT
    A.block_timestamp,
    A.block_id,
    A.tx_id,
    A.succeeded,
    A.program_id,
    A.mint,
    A.purchaser,
    A.seller,
    b.sales_amount,
    A.marketplace,
    A._inserted_timestamp
FROM
    mev2_sales A
    LEFT JOIN mev2_nft_sale_amount b
    ON A.tx_id = b.tx_id
