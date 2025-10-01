{{ config(
    materialized = 'incremental',
    unique_key = ["block_id","tx_id","index","inner_index"],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    full_refresh = false,
    tags = ['scheduled_non_core','daily_balances'],
) }}

with base_events as (
    select *
    from {{ ref('silver__events')}}
    where succeeded 
    {% if is_incremental() %}
        {% if execute %}
        {{ get_batch_load_logic(this,15,'2023-02-05') }}
        {% endif %}
    {% else %}
        and _inserted_timestamp::date between '2022-08-12' and '2022-09-01'
    {% endif %}
),
ownership_change_events as (
    select
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        index,
        -1 as inner_index,
        event_type,
        instruction,
        _inserted_timestamp
    from base_events
    where event_type in ('assign','assignWithSeed','close','closeAccount','create','createAccount','createAccountWithSeed','createIdempotent',
    'initializeAccount','initializeAccount2','initializeAccount3','revoke','setAuthority')
    union all
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
        ii.value :parsed :type :: STRING IN ('assign','assignWithSeed','close','closeAccount','create','createAccount','createAccountWithSeed','createIdempotent',
    'initializeAccount','initializeAccount2','initializeAccount3','revoke','setAuthority')
),
combined as (
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
        null as mint,
        _inserted_timestamp
    from ownership_change_events 
    where event_type in ('assign','assignWithSeed')
    union all
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
        instruction:parsed:info:mint::string as mint,
        _inserted_timestamp
    from ownership_change_events 
    where event_type in ('initializeAccount','initializeAccount2','initializeAccount3')
    union all
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
        null as mint,
        _inserted_timestamp
    from ownership_change_events 
    where event_type in ('close')
    union all
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
        instruction:parsed:info:mint::string as mint,
        _inserted_timestamp
    from ownership_change_events 
    where event_type in ('create','createIdempotent')
    union all
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
        null as mint,
        _inserted_timestamp
    from ownership_change_events 
    where event_type in ('createAccount','createAccountWithSeed')
    union all
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
        null as mint,
        _inserted_timestamp
    from ownership_change_events 
    where event_type in ('revoke')
    union all
    select 
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        index,
        inner_index,
        event_type,
        instruction:parsed:info:account::string as account_address,
        instruction:parsed:info:newAuthority::string as owner,
        null as mint,
        _inserted_timestamp
    from ownership_change_events 
    where event_type in ('setAuthority')
    and (instruction:parsed:info:authorityType::string is null 
        or instruction:parsed:info:authorityType::string = 'accountOwner')
    and owner is not null /* some events have an invalid new authority object even though tx is successful, ex: 4oHAf4fmEFmdiYG6Rchh4FoMH4de97iwnZqHEYrvQ5oo3UgwumPxkkkX6KAWCwmk4e5GzsHXqFQYVa2VyoQUYyyD */
    union all
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
        null as mint,
        _inserted_timestamp
    from ownership_change_events 
    where event_type in ('closeAccount')
)
select *
from combined 
qualify(row_number() over (partition by tx_id, account_address order by index desc, inner_index desc)) = 1