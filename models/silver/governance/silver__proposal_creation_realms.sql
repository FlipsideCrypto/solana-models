{{ config(
  materialized = 'incremental',
  unique_key = "tx_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['daily']
) }}

WITH vote_programs AS (
    SELECT 
        address 
    FROM 
        {{ ref('silver__labels') }} 
    WHERE 
    project_name = 'realms'
), 
prop_txs AS (
    SELECT 
        block_timestamp, 
        block_id, 
        tx_id, 
        succeeded, 
        e.index, 
        program_id, 
        instruction :accounts[0] :: STRING AS realms_id, 
        instruction :accounts[1] :: STRING AS proposal, 
        instruction :accounts[5] :: STRING AS proposal_writer,
        _inserted_timestamp 
    FROM 
        {{ ref('silver__events') }} e
  
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
                prop_txs 
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
        log_message LIKE 'Program log: GOVERNANCE-INSTRUCTION: CreateProposal %'
)
SELECT 
    p.block_timestamp, 
    p.block_id, 
    p.tx_id, 
    p.succeeded, 
    p.index, 
    p.program_id, 
    realms_id, 
    proposal, 
    proposal_writer, 
    SPLIT_PART(SPLIT_PART(log_message, 'name:', 2), ', description_link:', 0) AS proposal_name, 
    SPLIT_PART(SPLIT_PART(log_message, 'vote_type:', 2), ', options:', 0) AS vote_type,
    SPLIT_PART(SPLIT_PART(log_message, 'options: ', 2), ', use_deny_option:', 0) AS vote_options,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['p.tx_id']
    ) }} AS proposal_creation_realms_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM prop_txs p

INNER JOIN create_vote_logs l 
ON l.tx_id = p.tx_id 
AND l.event_index = p.index
