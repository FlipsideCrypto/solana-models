{{ config(error_if = '>500', warn_if = '>400') }}

WITH tmp AS (
    SELECT  block_id, 
            block_hash, 
            previous_block_hash
    FROM {{ ref('silver__blocks') }}
), 

hash AS (
        SELECT  
            t.block_id AS missing_slot, 
            t.block_hash 
        FROM tmp t
        LEFT JOIN tmp t2
        ON t.previous_block_hash = t2.block_hash
        WHERE t2.block_hash is null 
) 

SELECT 
    *
FROM hash
