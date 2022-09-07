{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, mint)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH sales_inner_instructions AS (

    SELECT
        e.block_timestamp,
        e.block_id,
        e.tx_id,
        t.succeeded,
        e.program_id,
        e.index,
        COALESCE(
            i.value :parsed :info :lamports :: NUMBER,
            0
        ) AS amount,
        e.instruction :accounts [0] :: STRING AS purchaser,
        e.instruction :accounts [1] :: STRING AS seller,
        e.instruction :accounts [2] :: STRING AS nft_account,
        e.ingested_at,
        e._inserted_timestamp
    FROM
        {{ ref('silver__events') }}
        e
        INNER JOIN {{ ref('silver__transactions') }}
        t
        ON t.tx_id = e.tx_id
        LEFT OUTER JOIN TABLE(FLATTEN(inner_instruction :instructions)) i
    WHERE
        program_id = 'hausS13jsjafwWwGqZTUQRmWyvyxn9EQpqMwV1PBBmk' -- Programid used by OpenSea to execute sale, other non-opensea markets also use this
        AND instruction :data :: STRING LIKE '63LNsZWnP5%'
        AND e.instruction :accounts [10] :: STRING = '3o9d13qUvEuuauhFrVom1vuCzgNsJifeaBYDPquaT73Y'

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
    AND e.block_timestamp :: DATE >= '2022-04-03' -- no Opensea sales before this date
{% endif %}
),
post_token_balances AS (
    SELECT
        DISTINCT tx_id,
        account,
        mint
    FROM
        {{ ref('silver___post_token_balances') }}
        p

{% if is_incremental() %}
WHERE
    p._inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% else %}
    WHERE block_timestamp :: DATE >= '2022-04-03' -- no Opensea sales before this date
{% endif %}
),
pre_final AS (
    SELECT
        s.block_timestamp,
        s.block_id,
        s.tx_id,
        s.succeeded,
        s.program_id,
        p.mint,
        s.purchaser,
        s.seller,
        SUM(
            s.amount
        ) / pow(
            10,
            9
        ) AS sales_amount,
        s.ingested_at,
        s._inserted_timestamp
    FROM
        sales_inner_instructions s
        LEFT OUTER JOIN post_token_balances p
        ON p.tx_id = s.tx_id
        AND p.account = s.nft_account
    WHERE
        s.block_timestamp :: DATE >= '2022-04-03' -- transactions before this time are not opensea
    GROUP BY
        s.block_timestamp,
        s.block_id,
        s.tx_id,
        s.succeeded,
        s.program_id,
        p.mint,
        s.purchaser,
        s.seller,
        s.ingested_at,
        s._inserted_timestamp
)
SELECT
    *
FROM
    pre_final
WHERE
    sales_amount > 0 -- ignore very small amount of txs are actual 0 sales or not sold in SOL (~100 out of >360k)
