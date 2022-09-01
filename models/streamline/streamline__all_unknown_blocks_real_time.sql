{{ config(
    materialized = 'view'
) }}

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
    {{ ref('streamline__complete_blocks') }}