{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', tx_id, index)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
) }}

WITH vote_txs AS (
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
        _inserted_timestamp
    FROM 
        {{ ref('silver__events') }}  e, 
  
    LATERAL FLATTEN (input => inner_instruction :instructions) i 
  
    WHERE 
        program_id = 'GovER5Lthms3bLBqWub97yVrMmEogzX7xNjdXpPPCVZw' -- General Realms contract. Mango and Serum have different IDs, maybe more.

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
    program_id, 
    realms_id, 
    proposal, 
    voter, 
    split_part(split_part(split_part(split_part(l.value :: STRING, '{', 3), '}', 1), ',', 1), ':', 2) AS vote_choice, 
    split_part(split_part(split_part(split_part(l.value :: STRING, '{', 3), '}', 1), ',', 2), ':', 2) AS vote_weight, 
    t._inserted_timestamp
FROM vote_txs v 
    
LEFT OUTER JOIN {{ ref('silver__transactions') }} t
ON v.tx_id = t.tx_id

LEFT OUTER JOIN TABLE(FLATTEN(t.log_messages)) l
    
WHERE 
    v.block_timestamp :: date = '2022-07-19' 
    AND l.value :: STRING LIKE 'Program log: GOVERNANCE-INSTRUCTION: CastVote %'  

    {% if is_incremental() %}
    AND t._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
    )
    {% endif %}
    