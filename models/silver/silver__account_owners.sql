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
    and block_timestamp::date between '2023-01-01' and '2023-01-31'

),
ownership_change_events as (
    select
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        index,
        null as inner_index,
        event_type,
        instruction,
        _inserted_timestamp
    from base_events
    where event_type in ('assign','assignWithSeed','authorize','authorizeChecked',
        'authorizeWithSeed','close','closeAccount','create','createAccount','createAccountWithSeed','createIdempotent',
        'initialize','initializeAccount','initializeAccount2','initializeAccount3','initializeChecked','revoke','setAuthority')
    union 
    select 
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        e.index,
        ii.index as inner_index,
        ii.value :parsed :type,
        ii.value as instruction,
        _inserted_timestamp
    from base_events e,
    TABLE(FLATTEN(e.inner_instruction :instructions)) ii
    WHERE
        ii.value :parsed :type :: STRING IN ('assign','assignWithSeed','authorize','authorizeChecked',
        'authorizeWithSeed','close','closeAccount','create','createAccount','createAccountWithSeed','createIdempotent',
        'initialize','initializeAccount','initializeAccount2','initializeAccount3','initializeChecked','revoke','setAuthority')
)
select 
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    event_type,
    instruction:parsed:info:account::string as account_address,
    instruction:parsed:info:owner::string as owner,
    _inserted_timestamp
from ownership_change_events 
where event_type in ('assign','assignWithSeed','initializeAccount','initializeAccount2','initializeAccount3')
union 
select 
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    event_type,
    instruction:parsed:info:stakeAccount::string as account_address,
    instruction:parsed:info:newAuthority::string as owner,
    _inserted_timestamp
from ownership_change_events 
where event_type in ('authorize','authorizeChecked')
and instruction:parsed:info:authorityType::string = 'Withdrawer' /* probably handle stake accounts differently and include both stake and withdraw authorities */
union 
select 
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    event_type,
    coalesce(instruction:parsed:info:stakeAccount::string, instruction:parsed:info:voteAccount::string) as account_address,
    coalesce(instruction:parsed:info:newAuthorized::string, instruction:parsed:info:newAuthority::string) as owner,
    _inserted_timestamp
from ownership_change_events 
where event_type in ('authorizeWithSeed')
and instruction:parsed:info:authorityType::string = 'Withdrawer' /* This seems to involve both stake and vote accounts */
union 
select 
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    event_type,
    instruction:parsed:info:account::string as account_address,
    instruction:parsed:info:authority::string as owner,
    _inserted_timestamp
from ownership_change_events 
where event_type in ('close')
union 
select 
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    event_type,
    instruction:parsed:info:account::string as account_address,
    instruction:parsed:info:wallet::string as owner,
    _inserted_timestamp
from ownership_change_events 
where event_type in ('create','createIdempotent')
union 
select 
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    event_type,
    instruction:parsed:info:newAccount::string as account_address,
    instruction:parsed:info:owner::string as owner,
    _inserted_timestamp
from ownership_change_events 
where event_type in ('createAccount','createAccountWithSeed')
union 
select 
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    event_type,
    coalesce(instruction:parsed:info:stakeAccount::string,instruction:parsed:info:voteAccount::string) as account_address,
    coalesce(instruction:parsed:info:authorized:withdrawer::string,instruction:parsed:info:authorizedWithdrawer::string) as owner,
    _inserted_timestamp
from ownership_change_events 
where event_type in ('initialize')
union 
select 
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    event_type,
    instruction:parsed:info:stakeAccount::string as account_address,
    instruction:parsed:info:withdrawer::string as owner,
    _inserted_timestamp
from ownership_change_events 
where event_type in ('initializeChecked')
union 
select 
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    event_type,
    instruction:parsed:info:source::string as account_address,
    coalesce(instruction:parsed:info:owner::string,instruction:parsed:info:multisigOwner::string) as owner,
    _inserted_timestamp
from ownership_change_events 
where event_type in ('revoke')
union 
select 
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    event_type,
    instruction:parsed:info:account::string as account_address,
    coalesce(instruction:parsed:info:authority::string,instruction:parsed:info:newAuthority::string) as owner,
    _inserted_timestamp
from ownership_change_events 
where event_type in ('setAuthority')
and (instruction:parsed:info:authorityType::string is null 
    or instruction:parsed:info:authorityType::string = 'accountOwner')
union 
select 
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    event_type,
    instruction:parsed:info:account::string as account_address,
    coalesce(instruction:parsed:info:owner::string,instruction:parsed:info:multisigOwner::string) as owner,
    _inserted_timestamp
from ownership_change_events 
where event_type in ('closeAccount')
