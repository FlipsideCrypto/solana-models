{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, voter, gauge)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH all_saber_gauges_events AS (

    SELECT
        block_timestamp,
        block_id,
        tx_id,
        INDEX,
        instruction,
        _inserted_timestamp
    FROM
        {{ ref('silver__events') }}
    WHERE
        program_id = 'GaugesLJrnVjNNWLReiw3Q7xQhycSBRgeHGTMDUaX231'

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
            WHEN l.value LIKE 'Program log: Instruction:%' THEN 'instruction_name'
            ELSE 'vote'
        END AS log_type,
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
                all_saber_gauges_events
        ) g
        ON t.tx_id = g.tx_id
        LEFT OUTER JOIN TABLE(FLATTEN(t.log_messages)) l
    WHERE
        (
            l.value :: STRING LIKE 'Program log: Instruction:%'
            OR l.value :: STRING LIKE 'Program log: power:%'
        )

{% if is_incremental() %}
AND ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
)
SELECT
    e.block_timestamp,
    e.block_id,
    e.tx_id,
    l.succeeded,
    e.instruction :accounts [7] :: STRING AS voter,
    e.instruction :accounts [1] :: STRING AS gauge,
    REPLACE(
        REGEXP_SUBSTR(
            l.message,
            'Program log: power: \\d+'
        ),
        'Program log: power: '
    ) :: NUMBER AS power,
    REPLACE(
        REGEXP_SUBSTR(
            l.message,
            'shares: \\d+'
        ),
        'shares: '
    ) :: NUMBER AS delegated_shares,
    e._inserted_timestamp
FROM
    all_saber_gauges_events e
    LEFT OUTER JOIN tx_logs l
    ON l.tx_id = e.tx_id
    AND e.index = l.event_index
WHERE
    l.log_type = 'vote'
AND 
    e.instruction :accounts [0] :: STRING = '28ZDtf6d2wsYhBvabTxUHTRT6MDxqjmqR7RMCp348tyU' -- this is saber gaugemeister
