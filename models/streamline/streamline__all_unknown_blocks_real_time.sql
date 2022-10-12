{{ config(
    materialized = 'view'
) }}

with tmp as (
    select
        block_id,
        previous_block_id
    from solana.silver.blocks2
),
missing as (
    select 
        t1.previous_block_id as block_id
    from tmp t1 
    left outer join tmp t2 on t1.previous_block_id = t2.block_id
    where t2.block_id is null 
    and t1.previous_block_id is not null
)
SELECT
    SEQ8()+
    iff(
        (select max(block_id)-1000000 from {{ ref('streamline__complete_block_txs') }}) < 148693779,
        148693779,
        (select max(block_id)-1000000 from {{ ref('streamline__complete_block_txs') }})) 
    as block_id
FROM
    TABLE(GENERATOR(rowcount => 5000000))
EXCEPT
SELECT
    block_id
FROM
    {{ ref('streamline__complete_blocks') }}
UNION 
SELECT 
    block_id
FROM missing
where block_id > 130000000 --15984074,16903261,1690556 these 3 blocks are missing from the node
EXCEPT 
SELECT block_id
FROM solana.streamline.complete_blocks
WHERE _inserted_date >= '2022-10-07'
AND error is null