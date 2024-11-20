{{ config(
    materialized = 'incremental',
    tags = ['bronze_api'],
    full_refresh=false,
    enabled = false
) }}
/*
DEPRECATED - keeping so that we know this table still exists in snowflake
*/

/*
{% set producer_limit_size = 1000 %}

with base as (
    select b.block_id
    from {{ ref('silver__blocks') }} b
    left outer join {{ source('solana_silver','_blocks_tx_count') }} b2 on b.block_id = b2.block_id
    where b.block_id >= 226000000
    and b2.block_id is null
    and b.block_timestamp::date <= current_date
    union all /* TEMPORARY BACKFILL PORTTION */
    select b.block_id 
    from solana.bronze_api.solscan_blocks_to_get b
    left outer join  solana.silver._blocks_tx_count b2 on b.block_id = b2.block_id
    where b2.block_id is null
),
block_ids as (
    select block_id 
    from base b
    qualify(row_number() over (order by b.block_id desc)) <= {{ producer_limit_size }}
)
, make_requests as (
    select 
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
    from block_ids
)
select
    block_ids,
    streamline.udf_bulk_get_solscan_blocks(url, endpoint, headers, block_ids) as data,
    sysdate() as _inserted_timestamp,
    concat_ws('-',_inserted_timestamp,1) as _id
from make_requests mr
where array_size(block_ids) > 0
*/