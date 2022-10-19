{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, mint)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
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
        CASE
            WHEN program_id = '6U2LkBQ6Bqd1VFt7H76343vpSwS5Tb1rNyXSNnjkf9VL' THEN instruction :accounts [3] :: STRING
            ELSE instruction :accounts [2] :: STRING
        END AS nft_account,
        _inserted_timestamp
    FROM
        {{ ref('silver__events') }}
        e
        LEFT OUTER JOIN TABLE(FLATTEN(inner_instruction :instructions)) i
    WHERE
        instruction :data :: STRING LIKE '63LNsZWnP5%'
        AND (
            (
                program_id = 'hausS13jsjafwWwGqZTUQRmWyvyxn9EQpqMwV1PBBmk' -- Programid used BY Coral Cube V1 TO EXECUTE sale
                AND instruction :accounts [10] :: STRING = '29xtkHHFLUHXiLoxTzbC7U8kekTwN3mVQSkfXnB1sQ6e'
            )
            OR (
                program_id = '6U2LkBQ6Bqd1VFt7H76343vpSwS5Tb1rNyXSNnjkf9VL' -- Coral Cube V2
                AND instruction :accounts [10] :: STRING = 'Ex9xNf2ocrM9hmtKhkpQG4i4XeXWrhFxsusSc1wHz3X8'
            )
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2022-02-02' -- no Coral Cube sales before this DATE
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
WHERE
    block_timestamp :: DATE >= '2022-02-02' -- no Coral Cube sales before this DATE
{% endif %}
),
pre_final AS (
    SELECT
        s.block_timestamp,
        s.block_id,
        s.tx_id,
        s.succeeded,
        s.program_id,
        COALESCE(
            p.mint,
            s.nft_account
        ) AS mint,
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
        COALESCE(
            p.mint,
            s.nft_account
        ),
        s.purchaser,
        s.seller,
        s._inserted_timestamp
)
SELECT
    *
FROM
    pre_final
WHERE
    sales_amount > 0 -- ignore very small amount OF txs are actual 0 sales
