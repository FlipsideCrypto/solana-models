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
        AND ARRAY_SIZE(
            inner_instruction :instructions
        ) >= 3 -- at least 3 inner_instruction on a sale...1) nft itself 2) sale instruction 3) royalty instruction
        AND event_type IS NULL
        AND e.instruction :accounts [5] :: STRING = 'So11111111111111111111111111111111111111112'
        AND (
            ARRAY_CONTAINS(
                'pAHAKoTJsAAe2ZcvTZUxoYzuygVAFAmbYmJYdWT886r' :: variant,  -- This is definitely an opensea signer
                t.signers
            )
            OR ARRAY_CONTAINS(
                '71kwsvqZ5hTzQUB8piTRUBbCaoUWGMyN5Gb8vAtt9ZYV' :: variant,  -- This seems like it is an opensea signer?
                t.signers
            )
        )
        AND (
            ARRAY_POSITION(
                'pAHAKoTJsAAe2ZcvTZUxoYzuygVAFAmbYmJYdWT886r' :: variant,
                t.signers
            ) > 0
            OR ARRAY_POSITION(
                '71kwsvqZ5hTzQUB8piTRUBbCaoUWGMyN5Gb8vAtt9ZYV' :: variant,
                t.signers
            ) > 0
        )

{% if is_incremental() %}
AND e.ingested_at :: DATE >= CURRENT_DATE - 2
AND t.ingested_at :: DATE >= CURRENT_DATE - 2
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
    p.ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
)
SELECT
    s.block_timestamp,
    s.block_id,
    s.tx_id,
    s.succeeded,
    s.program_id,
    p.mint,
    s.purchaser,
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
    s.ingested_at,
    s._inserted_timestamp
