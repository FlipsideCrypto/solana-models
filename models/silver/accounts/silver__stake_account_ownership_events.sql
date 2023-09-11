{{ config(
    materialized = 'incremental',
    unique_key = ["block_id","tx_id","index","inner_index","authority_type"],
    incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp::date >= LEAST(current_date-7,(select min(block_timestamp)::date from ' ~ generate_tmp_view_name(this) ~ '))'],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    full_refresh = false,
    enabled = false,
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
    where event_type in ('authorize','authorizeChecked','authorizeWithSeed','initialize','initializeChecked')
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
        ii.value :parsed :type :: STRING IN ('authorize','authorizeChecked','authorizeWithSeed','initialize','initializeChecked')
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
        instruction:parsed:info:stakeAccount::string as account_address,
        lower(instruction:parsed:info:authorityType::string) as authority_type,
        instruction:parsed:info:newAuthority::string as authority,
        _inserted_timestamp
    from ownership_change_events 
    where event_type in ('authorize','authorizeChecked')
    and lower(instruction:parsed:info:authorityType::string) = 'withdrawer' /* probably handle stake accounts differently and include both stake and withdraw authorities */
    and instruction:parsed:info:voteAccount::string is null
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
        lower(instruction:parsed:info:authorityType::string) as authority_type,
        instruction:parsed:info:newAuthority::string as authority,
        _inserted_timestamp
    from ownership_change_events 
    where event_type in ('authorize','authorizeChecked')
    and lower(instruction:parsed:info:authorityType::string) = 'staker' /* probably handle stake accounts differently and include both stake and withdraw authorities */
    and instruction:parsed:info:voteAccount::string is null
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
        lower(instruction:parsed:info:authorityType::string) as authority_type,
        coalesce(instruction:parsed:info:newAuthorized::string, instruction:parsed:info:newAuthority::string),
        _inserted_timestamp
    from ownership_change_events 
    where event_type in ('authorizeWithSeed')
    and instruction:parsed:info:voteAccount::string is null /* handle vote account changes elsewhere? */
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
        'withdrawer' as authority_type,
        coalesce(instruction:parsed:info:authorized:withdrawer::string,instruction:parsed:info:authorizedWithdrawer::string),
        _inserted_timestamp
    from ownership_change_events 
    where event_type in ('initialize')
    and instruction:parsed:info:voteAccount::string is null /* handle vote account changes elsewhere? */
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
        'staker' as authority_type,
        coalesce(instruction:parsed:info:authorized:staker::string,instruction:parsed:info:authorizedStaker::string),
        _inserted_timestamp
    from ownership_change_events 
    where event_type in ('initialize')
    and instruction:parsed:info:voteAccount::string is null /* handle vote account changes elsewhere? */
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
        'withdrawer' as authority_type,
        instruction:parsed:info:withdrawer::string,
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
        instruction:parsed:info:stakeAccount::string as account_address,
        'staker' as authority_type,
        instruction:parsed:info:staker::string,
        _inserted_timestamp
    from ownership_change_events 
    where event_type in ('initializeChecked')
)
select *
from combined 
qualify(row_number() over (partition by tx_id, account_address, authority_type order by index desc, inner_index desc)) = 1