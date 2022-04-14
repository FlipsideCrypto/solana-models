{{ config(error_if = '>5000', warn_if = '>5000') }}

SELECT 
    block_id AS slot
FROM {{ ref('silver__blocks') }}
WHERE block_id NOT IN (
    SELECT 
        block_id 
    FROM {{ ref('silver__transactions') }}
)