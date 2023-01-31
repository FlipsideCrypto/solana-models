{{ config(
    materialized = 'incremental',
    unique_key = ["block_id","tx_id","index","inner_index"],
    incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp::date >= LEAST(current_date-7,(select min(block_timestamp)::date from ' ~ generate_tmp_view_name(this) ~ '))'],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
) }}

with base_events as (
    select *
    from {{ ref('silver__events')}}
    where succeeded 
    and block_timestamp::date = '2023-01-01'

),
ownership_change_events as (
    select
        tx_id,
        succeeded,
        index,
        null as inner_index,
        event_type,
        instruction
    from base_events
    where event_type in ('assign','assignWithSeed','authorize','authorizeChecked',
        'authorizeWithSeed','close','closeAccount','create','createAccount')
    union 
    select 
        tx_id,
        succeeded,
        e.index,
        ii.index as inner_index,
        ii.value :parsed :type,
        ii.value as instruction
    from base_events e,
    TABLE(FLATTEN(e.inner_instruction :instructions)) ii
    WHERE
        ii.value :parsed :type :: STRING IN ('assign','assignWithSeed','authorize','authorizeChecked',
        'authorizeWithSeed','close','closeAccount','create','createAccount')
)
select 
    tx_id,
    succeeded,
    index,
    inner_index,
    event_type,
    instruction:parsed:info:account::string as account_address,
    instruction:parsed:info:owner::string as owner
from ownership_change_events 
where event_type in ('assign','assignWithSeed','closeAccount')
union 
select 
    tx_id,
    succeeded,
    index,
    inner_index,
    event_type,
    instruction:parsed:info:stakeAccount::string as account_address,
    instruction:parsed:info:newAuthority::string as owner
from ownership_change_events 
where event_type in ('authorize','authorizeChecked')
and instruction:parsed:info:authorityType::string = 'Withdrawer' /* probably handle stake accounts differently and include both stake and withdraw authorities */
union 
select 
    tx_id,
    succeeded,
    index,
    inner_index,
    event_type,
    coalesce(instruction:parsed:info:stakeAccount::string, instruction:parsed:info:voteAccount::string) as account_address,
    coalesce(instruction:parsed:info:newAuthorized::string, instruction:parsed:info:newAuthority::string) as owner,
from ownership_change_events 
where event_type in ('authorizeWithSeed')
and instruction:parsed:info:authorityType::string = 'Withdrawer' /* This seems to involve both stake and vote accounts */
union 
select 
    tx_id,
    succeeded,
    index,
    inner_index,
    event_type,
    instruction:parsed:info:account::string as account_address,
    instruction:parsed:info:authority::string as owner
from ownership_change_events 
where event_type in ('close')
union 
select 
    tx_id,
    succeeded,
    index,
    inner_index,
    event_type,
    instruction:parsed:info:account::string as account_address,
    instruction:parsed:info:wallet::string as owner
from ownership_change_events 
where event_type in ('create')
union 
select 
    tx_id,
    succeeded,
    index,
    inner_index,
    event_type,
    instruction:parsed:info:newAccount::string as account_address,
    instruction:parsed:info:owner::string as owner
from ownership_change_events 
where event_type in ('createAccount')