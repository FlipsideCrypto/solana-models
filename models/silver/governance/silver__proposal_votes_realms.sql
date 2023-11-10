{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', tx_id, index)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['scheduled_non_core']
) }}

WITH vote_programs AS (
    SELECT 
        address
    FROM 
    {{ ref('silver__labels') }} 
    WHERE 
        project_name = 'realms'
),  
vote_txs AS (
    SELECT
        block_timestamp, 
        block_id, 
        tx_id,
        succeeded, 
        e.index, 
        program_id, 
        instruction :accounts[0] :: STRING AS realms_id, 
        instruction :accounts[2] :: STRING AS proposal, 
        instruction :accounts[5] :: STRING AS voter, 
        instruction :accounts[6] :: STRING AS vote_account, 
        _inserted_timestamp
    FROM 
        {{ ref('silver__events') }}  e 
  
    INNER JOIN vote_programs v
    ON e.program_id = v.address

    WHERE 
        block_timestamp :: date >= '2022-04-28'
        AND instruction :data :: STRING <> 'Q'
    
    {% if is_incremental() %}
    AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
    )
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
        INNER JOIN (
            SELECT
                DISTINCT tx_id
            FROM
                vote_txs 
        ) v
        ON t.tx_id = v.tx_id
        LEFT OUTER JOIN TABLE(FLATTEN(t.log_messages)) l

{% if is_incremental() %}
WHERE _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
WHERE 
    t.block_timestamp :: date >= '2022-04-28'
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
        conditional_true_event(
            prev_event_cumsum = 0
        ) over (
            PARTITION BY tx_id
            ORDER BY
                INDEX
        ) AS event_index
    FROM
        C
), 
create_vote_logs AS (
    SELECT 
        * 
    FROM 
        tx_logs
    WHERE 
        log_message LIKE 'Program log: GOVERNANCE-INSTRUCTION: CastVote %'
)

SELECT 
    v.block_timestamp, 
    v.block_id,
    v.tx_id, 
    v.succeeded,
    v.index, 
    v.program_id, 
    realms_id, 
    proposal, 
    voter, 
    vote_account, 
    CASE WHEN (log_message LIKE 'Program log: GOVERNANCE-INSTRUCTION: CastVote { vote: Deny }')
    OR (log_message LIKE 'Program log: GOVERNANCE-INSTRUCTION: CastVote { vote: No }') THEN 
        'NO'
    WHEN (log_message LIKE 'Program log: GOVERNANCE-INSTRUCTION: CastVote { vote: Approve%') 
    OR (log_message LIKE 'Program log: GOVERNANCE-INSTRUCTION: CastVote { vote: Yes }') THEN 
        'YES'
    ELSE 
        'ABSTAIN'
    END AS vote_choice,
    CASE WHEN log_message LIKE 'Program log: GOVERNANCE-INSTRUCTION: CastVote { vote: Approve%' THEN 
        TRY_CAST(split_part(split_part(split_part(split_part(log_message :: STRING, '{', 3), '}', 1), ',', 1), ':', 2) AS INTEGER)
    ELSE 
        0
    END AS vote_rank, 
    CASE WHEN log_message LIKE 'Program log: GOVERNANCE-INSTRUCTION: CastVote { vote: Approve%' THEN 
        TRY_CAST(split_part(split_part(split_part(split_part(log_message:: STRING, '{', 3), '}', 1), ',', 2), ':', 2) AS INTEGER)
    WHEN log_message LIKE 'Program log: GOVERNANCE-INSTRUCTION: CastVote { vote: Yes }' THEN 
        100
    ELSE 
        0
    END AS vote_weight,
    _inserted_timestamp
FROM vote_txs v 

INNER JOIN create_vote_logs l 
ON l.tx_id = v.tx_id
AND l.event_index = v.index

    
