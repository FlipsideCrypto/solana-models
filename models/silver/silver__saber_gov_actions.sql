{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
) }}

WITH post_token_balances AS (

    SELECT
        tx_id,
        account,
        mint,
        DECIMAL
    FROM
        {{ ref('silver___post_token_balances') }}
    WHERE
        mint = 'Saber2gLauYim4Mvftnrasomsv6NvAuncvMEZwcLpD1'

{% if is_incremental() %}
AND ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
),
saber_gov_lock_events AS (
    SELECT
        e.block_timestamp,
        e.block_id,
        e.tx_id,
        e.index,
        e.instruction :accounts [3] :: STRING AS lock_signer,
        e.instruction :accounts [2] :: STRING AS exit_signer,
        ii.value :parsed :info :destination :: STRING AS destination,
        ii.value :parsed :info :source :: STRING AS source,
        ii.value :parsed :info :amount :: NUMBER AS amount
    FROM
        {{ ref('silver__events') }}
        e,
        TABLE(FLATTEN(inner_instruction :instructions)) ii
    WHERE
        program_id = 'LocktDzaV1W2Bm9DeZeiyz4J9zs4fRqNiYqQyracRXw'

{% if is_incremental() %}
AND ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
),
tx_logs AS (
    SELECT
        t.tx_id,
        t.succeeded,
        l.value :: STRING AS message,
        CASE
            WHEN l.value LIKE 'Program log: Instruction: Exit%' THEN 'EXIT'
            WHEN l.value LIKE 'Program log: Instruction: Lock%' THEN 'LOCK'
            ELSE NULL
        END AS action,
        conditional_true_event(
            l.value LIKE 'Program log: Instruction:%'
        ) over (
            PARTITION BY t.tx_id
            ORDER BY
                l.index
        ) - 1 AS event_index
    FROM
        {{ ref('silver__transactions') }}
        t
        INNER JOIN (
            SELECT
                DISTINCT tx_id
            FROM
                saber_gov_lock_events
        ) g
        ON t.tx_id = g.tx_id
        LEFT OUTER JOIN TABLE(FLATTEN(t.log_messages)) l
    WHERE
        l.value :: STRING LIKE 'Program log: Instruction: %'

{% if is_incremental() %}
AND ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
)
SELECT
    e.block_timestamp,
    e.block_id,
    e.tx_id,
    l.succeeded,
    CASE
        WHEN l.action = 'EXIT' THEN e.exit_signer
        ELSE e.lock_signer
    END AS signer,
    CASE
        WHEN l.action = 'EXIT' THEN e.source
        ELSE e.destination
    END AS locker_account,
    p.mint,
    l.action,
    e.amount / pow(
        10,
        p.decimal
    ) AS amount
FROM
    saber_gov_lock_events e
    INNER JOIN post_token_balances p
    ON p.tx_id = e.tx_id
    AND p.account = e.destination
    LEFT OUTER JOIN tx_logs l
    ON l.tx_id = e.tx_id
    AND l.event_index = e.index
