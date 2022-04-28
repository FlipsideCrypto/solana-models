{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, voter_nft, proposal)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH marinade_vote_txs AS (

    SELECT
        DISTINCT e.tx_id,
        succeeded
    FROM
        {{ ref('silver__events') }}
        e
        INNER JOIN {{ ref('silver__transactions') }}
        t
        ON e.tx_id = t.tx_id
        LEFT OUTER JOIN TABLE(
            FLATTEN(
                input => inner_instruction :instructions,
                outer => TRUE
            )
        ) ii
    WHERE
        program_id = 'tovt1VkTE2T4caWoeFP6a2xSFoew5mNpd7FWidyyMuk'

{% if is_incremental() %}
AND e.ingested_at :: DATE >= CURRENT_DATE - 2
AND t.ingested_at :: DATE >= CURRENT_DATE - 2
{% else %}
    AND e.ingested_at :: DATE >= '2022-04-01'
    AND t.ingested_at :: DATE >= '2022-04-01'
{% endif %}
INTERSECT
SELECT
    DISTINCT e.tx_id,
    succeeded
FROM
    {{ ref('silver__events') }}
    e
    INNER JOIN {{ ref('silver__transactions') }}
    t
    ON e.tx_id = t.tx_id
    LEFT OUTER JOIN TABLE(
        FLATTEN(
            input => inner_instruction :instructions,
            outer => TRUE
        )
    ) ii
WHERE
    (
        program_id = 'Govz1VyoyLD5BL6CSCxUJLVLsQHRwjfFj1prNsdNg5Jw' -- ignore votes
        OR COALESCE(
            ii.value :programId :: STRING,
            ''
        ) = 'Govz1VyoyLD5BL6CSCxUJLVLsQHRwjfFj1prNsdNg5Jw'
    )

{% if is_incremental() %}
AND e.ingested_at :: DATE >= CURRENT_DATE - 2
AND t.ingested_at :: DATE >= CURRENT_DATE - 2
{% else %}
    AND e.ingested_at :: DATE >= '2022-04-01'
    AND t.ingested_at :: DATE >= '2022-04-01'
{% endif %}
),
b AS (
    SELECT
        t.tx_id,
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
{% else %}
WHERE
    t.ingested_at :: DATE >= '2022-04-01'
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
    m.succeeded,
    e.instruction :accounts [3] :: STRING AS voter,
    e.instruction :accounts [1] :: STRING AS voter_nft,
    e.instruction :accounts [5] :: STRING AS voter_account,
    e.instruction :accounts [6] :: STRING AS proposal
FROM
    {{ ref('silver__events') }}
    e
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
{% else %}
    AND e.ingested_at :: DATE >= '2022-04-01'
{% endif %}