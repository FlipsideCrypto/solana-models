{{ config(
    materialized = 'incremental',
    unique_key = ["address","owner","start_block_id"],
    incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp::date >= LEAST(current_date-7,(select min(block_timestamp)::date from ' ~ generate_tmp_view_name(this) ~ '))'],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
) }}

/*
for incrementals also select all null end date accounts and combine
join to eliminate accounts that are not in the subset
remove all accounts that have the same owner + start block + end block
*/
with last_updated_at as (
    select max(_inserted_timestamp) as _inserted_timestamp
    from {{ ref('silver__account_ownership_events') }}
),
base as (
    select 
        account_address, 
        owner, 
        block_id,
        conditional_change_event(owner) over (partition by account_address order by block_id) as bucket
    from {{ ref('silver__account_ownership_events') }}
    /* incremental condition here */
    /* incremental union here */
),
c as (
    select 
        account_address, 
        owner, 
        bucket, 
        min(block_id) as start_block_id
    from base 
    where account_address = 'abc'
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
/* eliminate existing records here or could just re-merge them w/ a new last updated timestamp?*/;