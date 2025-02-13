{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
    full_refresh = false,
    enabled = false,
) }}

WITH base_table AS (

    SELECT
        e.block_timestamp,
        e.block_id,
        e.tx_id,
        e.index,
        null as inner_index,
        t.succeeded,
        e.program_id,
        CASE
            WHEN t.log_messages [1] :: STRING LIKE 'Program log: Instruction: Accept bid' THEN 'bid'
            ELSE 'direct buy'
        END AS sale_type,
        CASE
            WHEN sale_type = 'bid' THEN instruction :accounts [1] :: STRING
            ELSE instruction :accounts [0] :: STRING
        END AS purchaser,
        CASE
            WHEN sale_type = 'bid' THEN instruction :accounts [0] :: STRING
            ELSE instruction :accounts [1] :: STRING
        END AS seller,
        CASE
            WHEN sale_type = 'bid' THEN instruction :accounts [10] :: STRING
            ELSE instruction :accounts [9] :: STRING
        END AS acct_1,
        instruction :accounts [2] :: STRING AS acct_2,
        CASE
            WHEN sale_type = 'bid' THEN instruction :accounts [8] :: STRING
            ELSE instruction :accounts [7] :: STRING
        END AS mint,
        l.value :: STRING AS log_messages,
        e._inserted_timestamp
    FROM
        {{ ref('silver__events') }}
        e
        INNER JOIN {{ ref('silver__transactions') }}
        t
        ON t.tx_id = e.tx_id
        LEFT JOIN TABLE(FLATTEN(t.log_messages)) l
    WHERE
        program_id = '5SKmrbAxnHV2sgqyDXkGrLrokZYtWWVEEk5Soed7VLVN' -- yawww program ID
        AND (
            l.value :: STRING ILIKE 'Program log: Instruction: Accept bid'
            OR l.value :: STRING ILIKE 'Program log: Instruction: Buy listed item'
        )

{% if is_incremental() %}
AND e._inserted_timestamp >= (
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
{% else %}
AND 
    e.block_timestamp :: date >= '2022-07-12'
AND 
    t.block_timestamp :: date >= '2022-07-12'
{% endif %}
),
price_buys AS (
    SELECT
        b.tx_id,
        SUM(
            i.value :parsed :info :lamports
        ) / pow(
            10,
            9
        ) :: NUMBER AS sales_amount -- sales amount, but only for buys
    FROM
        base_table b
        INNER JOIN {{ ref('silver__events') }}
        e
        ON e.tx_id = b.tx_id
        LEFT JOIN TABLE(FLATTEN(inner_instruction :instructions)) i
    WHERE
        i.value :parsed :type :: STRING = 'transfer'
        AND i.value :program :: STRING = 'system'

{% if is_incremental() %}
AND e._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
AND 
    e.block_timestamp :: date >= '2022-07-12'
{% endif %}
GROUP BY
    b.tx_id
),
price_bids AS (
    SELECT
        bidder,
        acct_2,
        bid_amount
    FROM
        {{ source(
            'solana_silver',
            'nft_bids_yawww'
        ) }}
        qualify(ROW_NUMBER() over (PARTITION BY bidder, acct_2
    ORDER BY
        block_timestamp DESC)) = 1
)
SELECT
    b.block_timestamp,
    b.block_id,
    b.tx_id,
    b.succeeded,
    b.index,
    b.inner_index,
    b.program_id,
    b.mint,
    b.purchaser,
    b.seller,
    COALESCE(
        sales_amount,
        bid_amount
    ) AS sales_amount,
    sale_type,
    b._inserted_timestamp
FROM
    base_table b
    LEFT OUTER JOIN price_buys p
    ON b.tx_id = p.tx_id
    LEFT OUTER JOIN price_bids bd
    ON b.acct_2 = bd.acct_2
    AND b.purchaser = bd.bidder
