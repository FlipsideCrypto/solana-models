{{ config(
    materialized = 'incremental',
    tags = ['bronze_api'],
    full_refresh=false,
) }}

{% set producer_limit_size = 1000 %}
{% if execute %}
    {% if is_incremental() %}
        {% set max_block_id = run_query("""select max(block_id) from """ ~ target.database ~ """.silver._blocks_tx_count""").columns[0].values()[0] %}
    {% else %}
        {% set max_block_id = 225999999 %}
    {% endif %}
{% endif %}

with block_ids as (
    select 
        a._id+{{ max_block_id }}+1 as block_id
    from 
        {{ ref('silver___number_sequence') }} a
    where 
        block_id <= (select max(block_id) from {{ ref('silver__blocks')}} )
    and block_id >= 226000000
    qualify(row_number() over (order by block_id)) <= {{ producer_limit_size }}
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