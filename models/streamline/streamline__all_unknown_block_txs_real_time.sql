{{ config(
    materialized = 'view'
) }}

WITH pre_final AS (

    SELECT
        SEQ8()+
        iff(
            (select max(block_id)-300000 from {{ ref('streamline__complete_block_txs') }}) < 148520683,
            148520683,
            (select max(block_id)-300000 from {{ ref('streamline__complete_block_txs') }})) 
        as block_id
    FROM
        TABLE(GENERATOR(rowcount => 5000000))
    EXCEPT
    SELECT
        block_id
    FROM
        {{ ref('streamline__complete_block_txs') }}
)
SELECT
    block_id,
    (
        SELECT
            coalesce(MAX(_partition_id) + 1,1)
        FROM
            {{ ref('streamline__complete_block_txs') }}
    ) AS batch_id
FROM
    pre_final
