{{ config(
    materialized = 'view'
) }}

SELECT
    SEQ8() AS block_id
FROM
    TABLE(GENERATOR(rowcount => 1000000000))
WHERE block_id <= 98680445
EXCEPT
SELECT
    block_id
from 
    {{ ref('streamline__complete_blocks') }}