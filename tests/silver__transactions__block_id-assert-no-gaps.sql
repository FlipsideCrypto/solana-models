{{ config(error_if = '>500', warn_if = '>500') }}

SELECT 
    block_id AS slot
FROM {{ ref('silver__blocks') }}
WHERE block_timestamp::date <= current_date - 1
AND block_id NOT IN (
    SELECT 
        block_id 
    FROM {{ ref('silver__transactions') }}
)

AND block_id NOT IN (
    SELECT 
        block_id 
    FROM {{ ref('silver__votes') }}
)

AND tx_count > 0