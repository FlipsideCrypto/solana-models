{{ config(
    materialized = 'incremental',
    unique_key = ["account_address","owner","start_block_id"],
    cluster_by = ['_inserted_timestamp::DATE'],
) }}

/*
for incrementals also select all null end date accounts and combine
join to eliminate accounts that are not in the subset
remove all accounts that have the same owner + start block + end block
*/
with last_updated_at as (
    select max(_inserted_timestamp) as _inserted_timestamp
    from {{ ref('silver__token_account_ownership_events') }}
),
base as (
    select 
        account_address, 
        owner, 
        block_id,
        case 
            when event_type in ('create','createIdempotent','createAccount','createAccountWithSeed') then 
                0
            when event_type in ('initializeAccount','initializeAccount2','initializeAccount3') then 
                1
            else 2
        end as same_block_order_index
    from {{ ref('silver__token_account_ownership_events') }}
    /* incremental condition here */
    {% if is_incremental() %}
        where _inserted_timestamp >= (select max(_inserted_timestamp) from {{ this }})
    {% endif %}
),
{% if is_incremental() %}
current_ownership as (
    select 
        t.account_address, 
        t.owner, 
        t.start_block_id as block_id,
        2 as same_block_order_index
    from {{ this }} t
    join (select distinct account_address from base) b on b.account_address = t.account_address
    where t.end_block_id is null
    union 
    select 
        *
    from base
),
bucketed as (
    select 
        *,
        conditional_change_event(owner) over (partition by account_address order by block_id, same_block_order_index) as bucket
    from current_ownership
),
{% else %}
bucketed as (
    select 
        *,
        conditional_change_event(owner) over (partition by account_address order by block_id, same_block_order_index) as bucket
    from base
),
{% endif %}
c as (
    select 
        account_address, 
        owner, 
        bucket, 
        min(block_id) as start_block_id
    from bucketed
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
from c
join last_updated_at