{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH txs AS (

    SELECT
        e.tx_id,
        t.succeeded,
        MAX(INDEX) AS max_event_index
    FROM
        {{ ref('silver__events') }}
        e
        INNER JOIN {{ ref('silver__transactions') }}
        t
        ON t.tx_id = e.tx_id
    WHERE
        program_id = 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K' -- Magic Eden V2 Program ID

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
{% endif %}
GROUP BY
    1,
    2
HAVING
    COUNT(
        e.tx_id
    ) >= 2
),
base_tmp AS (
    SELECT
        e.block_timestamp,
        e.block_id,
        e.tx_id,
        t.succeeded,
        e.index AS event_index,
        i.index AS inner_index,
        e.program_id,
        COALESCE(
            i.value :parsed :info :lamports :: NUMBER,
            0
        ) AS amount,
        instruction :accounts [7] :: STRING AS nft_account,
        instruction :accounts [0] :: STRING AS purchaser,
        i.value :parsed :type :: STRING AS inner_instruction_type,
        LAG(inner_instruction_type) over (
            PARTITION BY e.tx_id
            ORDER BY
                inner_index
        ) AS preceding_inner_instruction_type,
        -- some mints do not map to a token account because of post purchase transfers within same transaction...need to use this when it is available
        LAST_VALUE(
            i.value :parsed :info :mint :: STRING ignore nulls
        ) over (
            PARTITION BY e.tx_id
            ORDER BY
                inner_index
        ) AS nft_account_mint,
        ingested_at,
        _inserted_timestamp
    FROM
        {{ ref('silver__events') }}
        e
        INNER JOIN txs t
        ON t.tx_id = e.tx_id
        AND t.max_event_index = e.index
        AND ARRAY_SIZE(
            e.inner_instruction :instructions
        ) > 1
        LEFT OUTER JOIN TABLE(FLATTEN(inner_instruction :instructions)) i
    WHERE
        (
            (
                amount <> 0
                AND inner_instruction_type = 'transfer'
            )
            OR inner_instruction_type = 'create'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
base AS (
    SELECT
        *
    FROM
        base_tmp
    WHERE
        inner_instruction_type = 'transfer'
        AND COALESCE(
            preceding_inner_instruction_type,
            ''
        ) <> 'create'
),
post_token_balances AS (
    SELECT
        DISTINCT tx_id,
        account,
        mint
    FROM
        {{ ref('silver___post_token_balances') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    b.block_timestamp,
    b.block_id,
    b.tx_id,
    b.succeeded,
    b.program_id,
    COALESCE(
        b.nft_account_mint,
        p.mint
    ) AS mint,
    b.purchaser,
    SUM(
        b.amount
    ) / pow(
        10,
        9
    ) AS sales_amount,
    b.ingested_at,
    b._inserted_timestamp
FROM
    base b
    LEFT OUTER JOIN post_token_balances p
    ON p.tx_id = b.tx_id
    AND p.account = b.nft_account
GROUP BY
    b.block_timestamp,
    b.block_id,
    b.tx_id,
    b.succeeded,
    b.program_id,
    COALESCE(
        b.nft_account_mint,
        p.mint
    ),
    b.purchaser,
    b.ingested_at,
    b._inserted_timestamp
