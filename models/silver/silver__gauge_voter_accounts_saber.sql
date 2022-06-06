{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, voter, signer, gauge_voter_account)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH potential_create_gauge_voter_events AS (

    SELECT
        e.block_timestamp,
        e.block_id,
        e.tx_id,
        e.index,
        e.instruction :accounts [0] :: STRING AS gauge_voter_account,
        e.instruction :accounts [2] :: STRING AS escrow_account,
        e.instruction :accounts [3] :: STRING AS signer
    FROM
        {{ ref('silver__events') }}
        e,
        TABLE(FLATTEN(inner_instruction :instructions)) ii
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
            WHEN l.value LIKE 'Program log: Instruction: CreateGaugeVoter%' THEN 'CREATE_GAUGE_VOTER'
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
                potential_create_gauge_voter_events
        ) g
        ON t.tx_id = g.tx_id
        LEFT OUTER JOIN TABLE(FLATTEN(t.log_messages)) l
    WHERE
        l.value :: STRING LIKE 'Program log: Instruction: %'

{% if is_incremental() %}
AND ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
),
escrow_token_account_link AS (
    SELECT
        DISTINCT locker_account,
        escrow_account
    FROM
        {{ ref('silver__gov_actions_saber') }}

{% if is_incremental() %}
WHERE
    ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
)
SELECT
    e.block_id,
    e.block_timestamp,
    e.tx_id,
    l.succeeded,
    l.action,
    e.signer,
    e.gauge_voter_account,
    e.escrow_account
FROM
    potential_create_gauge_voter_events AS e
    INNER JOIN tx_logs l
    ON l.tx_id = e.tx_id
    AND l.event_index = e.index
    AND l.action IS NOT NULL
    INNER JOIN escrow_token_account_link A
    ON A.escrow_account = e.escrow_account
