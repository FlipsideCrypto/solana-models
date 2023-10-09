{{ config(
    materialized = 'incremental',
    tags = ['bronze_api']
) }}

{% set producer_limit_size = 4000 %}
{% set query_batch_size = 2000 %}

{% if is_incremental() %}
with next_batch as (
    select 
        *
    from 
        {{ this }}
    qualify(row_number() over (order by _inserted_timestamp desc, _id desc)) = 1
),
next_block_id as (
    select 
        d.value:id::number as block_id
    from 
        next_batch,
    table(flatten(data)) d 
),
{% else %}
with 
{% endif %}
block_ids as (
    select 
        a._id+b.offset as block_id
    from 
        {{ ref('silver___number_sequence') }} a,
    {% if is_incremental() %}
        (select max(block_id)+1 as offset from next_block_id) b
    {% else %}
        (select 0 as offset) b
    {% endif %}
    qualify(row_number() over (order by block_id)) <= {{ producer_limit_size }}
),
request as (
    select 
        block_id,
        'https://solana-mainnet.rpc.extrnode.com' as url,
        object_construct(
            'jsonrpc','2.0',
            'id',block_id,
            'method','getBlock',
            'params',[
              block_id,
              object_construct(
                'encoding','json',
                'maxSupportedTransactionVersion',0,
                'transactionDetails','signatures',
                'rewards',false
              )
            ]
        ) as payload,
        row_number() over (order by block_id) as rn,
        ceil(rn/{{ query_batch_size }}) as gn
    from block_ids
)
, make_requests as (
    select 
        gn,
        array_agg(payload) as requests
    from request
    group by gn
)
-- , responses as (
select
    gn,
    requests,
    streamline.udf_bulk_get_blocks_tx_count(requests) as data,
    sysdate() as _inserted_timestamp,
    concat_ws('-',_inserted_timestamp,gn) as _id
from make_requests mr
    -- ,
--     table(flatten(data)) d
-- )
-- select 
    
--     value:id as block_id,
--     array_size(value:result:signatures::array) as tx_count
-- from responses