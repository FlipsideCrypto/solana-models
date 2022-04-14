SELECT 
    DISTINCT 
        block_timestamp::date, 
        block_id as slot 
FROM {{ ref('silver___inner_instructions') }} 
WHERE value:instructions[0]:programIdIndex::number IS NOT NULL 
GROUP BY block_timestamp::date, block_id 
ORDER BY block_timestamp::date DESC