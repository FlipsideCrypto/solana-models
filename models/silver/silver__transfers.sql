{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', block_id, tx_id, index)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['ingested_at::DATE'],
) }}

WITH base_transfers AS (

    SELECT
        e.block_id,
        e.block_timestamp,
        e.tx_id,
        e.index,
        e.instruction,
        e.ingested_at
    FROM
        {{ ref('silver__events') }}
        e
        INNER JOIN {{ ref('silver__transactions') }}
        t
        ON t.tx_id = e.tx_id

{% if is_incremental() %}
AND t.ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
WHERE
    e.event_type IN (
        'transfer',
        'transferChecked'
    )
    AND t.succeeded = TRUE

{% if is_incremental() %}
AND e.ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
),
base_post_token_balances AS (
    SELECT
        tx_id,
        owner,
        account,
        mint,
        DECIMAL
    FROM
        {{ ref('silver___post_token_balances') }}

{% if is_incremental() %}
WHERE
    ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
),
base_pre_token_balances AS (
    SELECT
        tx_id,
        owner,
        account,
        mint,
        DECIMAL
    FROM
        {{ ref('silver___pre_token_balances') }}

{% if is_incremental() %}
WHERE
    ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
),
spl_transfers AS (
    SELECT
        e.block_id,
        e.block_timestamp,
        e.tx_id,
        e.index,
        COALESCE(
            p.owner,
            e.instruction :parsed :info :authority :: STRING
        ) AS tx_from,
        COALESCE(
            p2.owner,
            instruction :parsed :info :destination :: STRING
        ) AS tx_to,
        COALESCE(
            e.instruction :parsed :info :tokenAmount: decimals,
            p.decimal,
            p2.decimal,
            p3.decimal,
            p4.decimal
        ) AS decimal_adj,
        COALESCE (
            e.instruction :parsed :info :amount :: INTEGER,
            e.instruction :parsed :info :tokenAmount :amount :: INTEGER
        ) / pow(
            10,
            decimal_adj
        ) AS amount,
        COALESCE(
            p.mint,
            p2.mint,
            p3.mint,
            p4.mint
        ) AS mint,
        e.ingested_at
    FROM
        base_transfers e
        LEFT OUTER JOIN base_pre_token_balances p
        ON e.tx_id = p.tx_id
        AND e.instruction :parsed :info :source :: STRING = p.account
        LEFT OUTER JOIN base_post_token_balances p2
        ON e.tx_id = p2.tx_id
        AND e.instruction :parsed :info :destination :: STRING = p2.account
        LEFT OUTER JOIN base_post_token_balances p3
        ON e.tx_id = p3.tx_id
        AND e.instruction :parsed :info :source :: STRING = p3.account
        LEFT OUTER JOIN base_pre_token_balances p4
        ON e.tx_id = p4.tx_id
        AND e.instruction :parsed :info :destination :: STRING = p4.account
    WHERE
        e.instruction :parsed :info :authority :: STRING IS NOT NULL
),
sol_transfers AS (
    SELECT
        e.block_id,
        e.block_timestamp,
        e.tx_id,
        e.index,
        instruction :parsed :info :source :: STRING AS tx_from,
        instruction :parsed :info :destination :: STRING AS tx_to,
        instruction :parsed :info :lamports / pow(
            10,
            9
        ) AS amount,
        'So11111111111111111111111111111111111111112' AS mint,
        e.ingested_at
    FROM
        base_transfers e
    WHERE
        instruction :parsed :info :lamports :: STRING IS NOT NULL
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    INDEX,
    tx_from,
    tx_to,
    amount,
    mint,
    ingested_at
FROM
    spl_transfers
UNION
SELECT
    block_id,
    block_timestamp,
    tx_id,
    INDEX,
    tx_from,
    tx_to,
    amount,
    mint,
    ingested_at
FROM
    sol_transfers
