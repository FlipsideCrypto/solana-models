{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, voter, gauge)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH marinade_vote_txs AS (

    SELECT
        DISTINCT tx_id
    FROM
        {{ ref('silver__events') }}
        e,
        TABLE(
            FLATTEN(
                input => inner_instruction :instructions,
                outer => TRUE
            )
        ) ii
    WHERE
        program_id = 'tovt1VkTE2T4caWoeFP6a2xSFoew5mNpd7FWidyyMuk'

{% if is_incremental() %}
AND e.ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
INTERSECT
SELECT
    DISTINCT tx_id
FROM
    {{ ref('silver__events') }}
    e,
    TABLE(
        FLATTEN(
            input => inner_instruction :instructions,
            outer => TRUE
        )
    ) ii
WHERE
    program_id = 'Govz1VyoyLD5BL6CSCxUJLVLsQHRwjfFj1prNsdNg5Jw' -- ignore votes

{% if is_incremental() %}
AND e.ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
),
b AS (
    SELECT
        t.tx_id,
        t.succeeded,
        l.index,
        l.value :: STRING AS log_message,
        CASE
            WHEN l.value :: STRING LIKE '%invoke%' THEN 1
            WHEN l.value :: STRING LIKE '%success' THEN -1
            ELSE 0
        END AS cnt,
        SUM(cnt) over (
            PARTITION BY t.tx_id
            ORDER BY
                l.index rows BETWEEN unbounded preceding
                AND CURRENT ROW
        ) AS event_cumsum
    FROM
        {{ ref('silver__transactions') }}
        t
        INNER JOIN marinade_vote_txs d
        ON t.tx_id = d.tx_id
        LEFT OUTER JOIN TABLE(FLATTEN(t.log_messages)) l

{% if is_incremental() %}
WHERE
    t.ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
),
C AS (
    SELECT
        b.*,
        LAG(
            event_cumsum,
            1
        ) over (
            PARTITION BY tx_id
            ORDER BY
                INDEX
        ) AS prev_event_cumsum
    FROM
        b
),
tx_logs AS (
    SELECT
        C.tx_id,
        C.succeeded,
        C.index AS log_index,
        C.log_message,
        CASE
            WHEN C.log_message = 'Program log: Instruction: CastVote' THEN 'VOTE'
            ELSE NULL
        END AS action,
        conditional_true_event(
            prev_event_cumsum = 0
        ) over (
            PARTITION BY tx_id
            ORDER BY
                INDEX
        ) AS event_index
    FROM
        C
)
SELECT
    e.block_timestamp,
    e.block_id,
    e.tx_id,
    l.succeeded,
    e.index,
    e.instruction :accounts [3] :: STRING AS voter,
    e.instruction :accounts [1] :: STRING AS voter_nft,
    e.instruction :accounts [5] :: STRING AS voter_account,
    NULL AS proposal_id
FROM
    {{ ref('silver__events') }} e
    INNER JOIN marinade_vote_txs m
    ON m.tx_id = e.tx_id
    LEFT OUTER JOIN tx_logs l
    ON e.tx_id = l.tx_id
    AND e.index = l.event_index
    AND l.action IS NOT NULL
WHERE
    l.action IS NOT NULL

{% if is_incremental() %}
AND e.ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
