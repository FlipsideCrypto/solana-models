{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ["account_address","start_block_id"],
    cluster_by = ['_inserted_timestamp::DATE'],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}'),
    full_refresh = false,
) }}

/* need to rebucket and regroup the intermediate model due to possibility of change events coming in out of order */
with last_updated_at as (
    select max(_inserted_timestamp) as _inserted_timestamp
    from {{ ref('silver__token_account_owners_intermediate') }}
    --testing
    -- where _inserted_timestamp::date < '2023-04-25'

)
, changed_addresses as (
    select distinct account_address
    from {{ ref('silver__token_account_owners_intermediate') }}
    {% if is_incremental() %}
    where _inserted_timestamp > (select max(_inserted_timestamp) from {{ this }})
    --testing
    -- and _inserted_timestamp::date < '2023-04-25'
    {% endif %}
),
rebucket as (
select 
    o.account_address, 
    o.owner, 
    o.start_block_id,
    conditional_change_event(owner) over (partition by o.account_address order by o.start_block_id) as bucket
from {{ ref('silver__token_account_owners_intermediate') }} o
inner join changed_addresses c on o.account_address = c.account_address
),
regroup as (
    select 
        account_address, 
        owner, 
        bucket, 
        min(start_block_id) as start_block_id
    from rebucket
    group by 1,2,3
),
pre_final as (
    select 
        account_address,
        owner,
        start_block_id,
        lead(start_block_id) ignore nulls over (
                    PARTITION BY account_address
                    ORDER BY bucket
                ) as end_block_id,
        _inserted_timestamp
    from regroup 
    join last_updated_at
)
select *
from pre_final
where start_block_id <> end_block_id 
or end_block_id is null