{{ config(
    materialized = 'table',
    unique_key = ["account_address"],
    cluster_by = ['_inserted_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
) }}

/* need to rebucket and regroup the intermediate model due to possibility of change events coming in out of order */
with last_updated_at as (
    select max(_inserted_timestamp) as _inserted_timestamp
    from {{ ref('silver__token_account_owners_intermediate') }}
)
, changed_addresses as (
    select distinct account_address
    from {{ ref('silver__token_account_owners_intermediate') }}
    {% if is_incremental() %}
    where _inserted_timestamp > (select max(_inserted_timestamp) from {{ this }})
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
)
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