{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', tx_id, index)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
) }}

WITH vote_programs AS (
    SELECT 
        address
    FROM {{ source(
        'crosschain',
        'address_labels'
    ) }} 
    WHERE 
        blockchain = 'solana'
        AND project_name = 'realms'
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
        i.value :parsed :info :source :: STRING AS voter, 
        instruction :accounts[6] :: STRING AS vote_account, 
        _inserted_timestamp
    FROM 
        {{ ref('silver__events') }}  e 
  
    INNER JOIN vote_programs v
    ON e.program_id = v.address

    LEFT OUTER JOIN TABLE(FLATTEN(inner_instruction :instructions)) i

    WHERE 
        instruction :data :: STRING <> 'Q'
    
    {% if is_incremental() %}
    AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
    )
    {% endif %}
)

SELECT 
    v.block_timestamp, 
    v.block_id,
    v.tx_id, 
    v.succeeded,
    v.index, 
    l.index AS _index, 
    v.program_id, 
    realms_id, 
    proposal, 
    voter, 
    vote_account, 
    CASE WHEN (l.value :: STRING LIKE 'Program log: GOVERNANCE-INSTRUCTION: CastVote { vote: Deny }')
    OR (l.value :: STRING LIKE 'Program log: GOVERNANCE-INSTRUCTION: CastVote { vote: No }') THEN 
        'NO'
    WHEN (l.value :: STRING LIKE 'Program log: GOVERNANCE-INSTRUCTION: CastVote { vote: Approve%') 
    OR (l.value :: STRING LIKE 'Program log: GOVERNANCE-INSTRUCTION: CastVote { vote: Yes }') THEN 
        'YES'
    ELSE 
        'ABSTAIN'
    END AS vote_choice,
    CASE WHEN l.value :: STRING LIKE 'Program log: GOVERNANCE-INSTRUCTION: CastVote { vote: Approve%' THEN 
        split_part(split_part(split_part(split_part(l.value :: STRING, '{', 3), '}', 1), ',', 1), ':', 2)
    ELSE 
        0
    END AS vote_rank, 
    CASE WHEN l.value :: STRING LIKE 'Program log: GOVERNANCE-INSTRUCTION: CastVote { vote: Approve%' THEN 
        split_part(split_part(split_part(split_part(l.value :: STRING, '{', 3), '}', 1), ',', 2), ':', 2) :: INTEGER
    WHEN l.value :: STRING LIKE 'Program log: GOVERNANCE-INSTRUCTION: CastVote { vote: Yes }' THEN 
        100
    ELSE 
        0
    END AS vote_weight,
    t._inserted_timestamp
FROM vote_txs v 

INNER JOIN {{ ref('silver__transactions') }} t
ON v.tx_id = t.tx_id

INNER JOIN TABLE(FLATTEN(t.log_messages)) l
    
WHERE 
    (l.value :: STRING LIKE 'Program log: GOVERNANCE-INSTRUCTION: CastVote %' 
    AND v.index = 0 
    AND _index <= 6)
    OR (
    l.value :: STRING LIKE 'Program log: GOVERNANCE-INSTRUCTION: CastVote %'
    AND v.index = 1 
    AND _index >= 7) 

{% if is_incremental() %}
AND t._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
    