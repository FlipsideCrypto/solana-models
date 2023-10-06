{{ config(
    materialized = 'incremental',
    tags = ['bronze_api']
) }}

{% set producer_limit_size = 100 %}
{% set query_batch_size = 50 %}

with request as (
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
        ) as payload
    from solana.silver.blocks
    qualify(row_number() over (order by block_id)) <= {{ producer_limit_size }}
)
, make_requests as ({% for item in range(1, producer_limit_size, query_batch_size) %}
    select 
        block_id,
        livequery_dev.live.udf_api('POST',url,{},payload) as responses
    from request
    qualify(row_number() over (order by block_id)) between {{ item }} and {{ item+query_batch_size-1 }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %})
select
    block_id,
    array_size(mr.responses:data:result:signatures::array) as tx_count
from make_requests mr