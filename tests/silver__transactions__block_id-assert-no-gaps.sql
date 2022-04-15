{{ config(error_if = '>6000', warn_if = '>6000') }}

SELECT 
    block_id AS slot
FROM {{ ref('silver__blocks') }}
WHERE block_id NOT IN (
    SELECT 
        block_id 
    FROM {{ ref('silver__transactions') }}
)

AND block_id NOT IN (
    SELECT 
        block_id 
    FROM {{ ref('silver__votes') }}
)