{{ config(
    materialized = 'view'
) }}

SELECT
    SEQ8() + 98680445 AS block_id
FROM
    TABLE(GENERATOR(rowcount => 60000000))
WHERE
    block_id > 98680445
    AND block_id <= 148693779
EXCEPT
SELECT
    block_id
FROM
    {{ ref('streamline__complete_blocks') }}
