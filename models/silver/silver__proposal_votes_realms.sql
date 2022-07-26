WITH vote_txs AS (
    SELECT
        tx_id
    FROM 
        {{ ref('silver__events') }} 
    WHERE 
        program_id = 'GovER5Lthms3bLBqWub97yVrMmEogzX7xNjdXpPPCVZw' -- General Realms contract. Mango and Serum have different IDs, maybe more.
)

SELECT 
    tx_id, 
    split_part(split_part(split_part(split_part(l.value :: STRING, '{', 3), '}', 1), ',', 1), ':', 2) :: INTEGER AS rank, 
    split_part(split_part(split_part(split_part(l.value :: STRING, '{', 3), '}', 1), ',', 2), ':', 2) :: INTEGER AS vote_weight
FROM 
    {{ ref('silver__transactions') }} t
LEFT OUTER JOIN TABLE(FLATTEN(t.log_messages)) l
    
WHERE block_timestamp :: date = '2022-07-19' 
AND l.value :: STRING LIKE 'Program log: GOVERNANCE-INSTRUCTION: CastVote %'  
AND tx_id = '4T3aZ7o4TSFq9h1WAxDopmF8YJkGVHGnWdQ4hUiA2dx2rvvhnkkFNgqAUds75wBqz1NfPY1PKFsJsRAv7tYAUCRi'