{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, mint)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
    full_refresh = false,
    enabled = false,
) }}

WITH sales_inner_instructions AS (

    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        program_id,
        e.index,
        COALESCE(
            i.value :parsed :info :lamports :: NUMBER,
            0
        ) AS amount,
        instruction :accounts [0] :: STRING AS purchaser,
        instruction :accounts [1] :: STRING AS seller,
        instruction :accounts [2] :: STRING AS nft_account,
        _inserted_timestamp
    FROM
        {{ ref('silver__events') }} e
         LEFT OUTER JOIN TABLE(FLATTEN(inner_instruction :instructions)) i

    WHERE 
        program_id = 'hausS13jsjafwWwGqZTUQRmWyvyxn9EQpqMwV1PBBmk' -- Programid used by OpenSea to execute sale, other non-opensea markets also use this
        AND instruction :data :: STRING LIKE '63LNsZWnP5%'
        AND instruction :accounts [10] :: STRING = '3o9d13qUvEuuauhFrVom1vuCzgNsJifeaBYDPquaT73Y'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)

{% else %}
AND
    block_timestamp :: DATE >= '2022-04-03' -- no Opensea sales before this date

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
        s.index,
        null as inner_index,
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
        s._inserted_timestamp
    FROM
        sales_inner_instructions s
        LEFT OUTER JOIN post_token_balances p
        ON p.tx_id = s.tx_id
        AND p.account = s.nft_account
    GROUP BY
        s.block_timestamp,
        s.block_id,
        s.tx_id,
        s.succeeded,
        s.program_id,
        s.index,
        p.mint,
        s.purchaser,
        s.seller,
        s._inserted_timestamp
)
SELECT
    *
FROM
    pre_final
WHERE
    sales_amount > 0 -- ignore very small amount of txs are actual 0 sales or not sold in SOL (~100 out of >360k)
