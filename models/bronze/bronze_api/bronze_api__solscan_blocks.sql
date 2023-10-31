{{ config(
    materialized = 'incremental',
    tags = ['bronze_api']
) }}

{% set producer_limit_size = 6000 %}
{% set query_batch_size = 1000 %}
{% set num_groups = 6 %}

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
    where 
        block_id <= (select max(block_id) from {{ ref('silver__blocks')}} )
    qualify(row_number() over (order by block_id)) <= {{ producer_limit_size }}
),
request as (
    select 
        block_id,
        -- object_construct(
        --     'headers', headers,
        --     'url', 'https://pro-api.solscan.io',
        --     'endpoint', CONCAT('/v1.0/block/',block_id)
        -- ) as payload,
        row_number() over (order by block_id) as rn,
        ceil(rn/{{ query_batch_size }}) as gn
    from block_ids
)
, make_requests as (
    select 
        gn,
        'https://pro-api.solscan.io' as url,
        '/v1.0/block/' as endpoint,
        OBJECT_CONSTRUCT(
            'Accept',
            'application/json',
            'token',
            (
                SELECT
                api_key
                FROM
                crosschain.silver.apis_keys
                WHERE
                api_name = 'solscan'
            )
        ) as headers,
        array_agg(block_id) as block_ids
    from request
    group by gn
)
{% for item in range(1,num_groups+1) %}
    select
        gn,
        block_ids,
        streamline.udf_bulk_get_solscan_blocks(url, endpoint, headers, block_ids) as data,
        sysdate() as _inserted_timestamp,
        concat_ws('-',_inserted_timestamp,gn) as _id
    from make_requests mr
    where gn = {{ item }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}
-- , responses as (

    -- ,
--     table(flatten(data)) d
-- )
-- select 
    
--     value:id as block_id,
--     array_size(value:result:signatures::array) as tx_count
-- from responses