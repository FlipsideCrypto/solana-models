{{ config(
    materialized = 'incremental',
    unique_key = ['block_id'],
    full_refresh = false
) }}

SELECT
    value:id as block_id,
    case when value:error:code::number = -32009 then 
        0
    else 
        array_size(value:result:signatures::array) 
    end as tx_count,
    _inserted_timestamp
FROM
    {{ ref('bronze_api__blocks_tx_count') }},
    table(flatten(data)) d

{% if is_incremental() %}
WHERE 
    _inserted_timestamp >= (select max(_inserted_timestamp) from {{ this }})
{% endif %}