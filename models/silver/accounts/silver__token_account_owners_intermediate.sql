{{ config(
    materialized = 'incremental',
    unique_key = ["account_address","owner","start_block_id"],
    cluster_by = ['_inserted_timestamp::DATE'],
    full_refresh = false,
    tags = ['scheduled_non_core']
) }}
-- depends_on: {{ ref('silver__token_account_ownership_events') }}
/* run incremental timestamp value first then use it as a static value */
{% if execute %}

{% if is_incremental() %}
{% set max_inserted_query %}

SELECT
    MAX(_inserted_timestamp) AS _inserted_timestamp
FROM
    silver.token_account_ownership_events

    {% endset %}
    {% set max_inserted_timestamp = run_query(max_inserted_query).columns [0].values() [0] %}
{% endif %}

{% set query = """ CREATE OR REPLACE TEMPORARY TABLE silver.token_account_ownership_events__intermediate_tmp AS SELECT account_address, owner, block_id, event_type, _inserted_timestamp  FROM """ ~ ref('silver__token_account_ownership_events') %}
{% set incr = "" %}

{% if is_incremental() %}
{% set incr = """ WHERE _inserted_timestamp >= '""" ~ max_inserted_timestamp ~ """' """ %}
{% endif %}

{% do run_query(
    query ~ incr
) %}
{% endif %}

/*
for incrementals also select all null end date accounts and combine
join to eliminate accounts that are not in the subset
remove all accounts that have the same owner + start block + end block
*/

with base as (
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
    FROM
        silver.token_account_ownership_events__intermediate_tmp
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
    '{{ max_inserted_timestamp }}' as _inserted_timestamp
from c
-- join last_updated_at