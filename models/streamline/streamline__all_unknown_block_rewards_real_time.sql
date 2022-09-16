{{ config(
    materialized = 'view'
) }}

WITH pre_final AS (

    SELECT
        SEQ8()+
        iff(
            (select max(block_id)-1000000 from {{ ref('streamline__complete_block_rewards') }}) < 148693779,
            148693779,
            (select max(block_id)-1000000 from {{ ref('streamline__complete_block_rewards') }})) 
        as block_id
    FROM
        TABLE(GENERATOR(rowcount => 5000000))
    EXCEPT
    SELECT
        block_id
    FROM
        {{ ref('streamline__complete_block_rewards') }}
)
SELECT
    block_id,
    (
        SELECT
            coalesce(MAX(_partition_id) + 1,1)
        FROM
            {{ ref('streamline__complete_block_rewards') }}
    ) AS batch_id
FROM
    pre_final