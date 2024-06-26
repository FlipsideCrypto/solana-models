{{ config(
    materialized = 'incremental',
    unique_key = ['tx_id','swap_index','program_id'],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    enabled=false
) }}

WITH base_events AS(

    SELECT
        *
    FROM
        {{ ref('silver__events') }}
    WHERE
        program_id = 'JUP5cHjnnCx2DppVsufsLrXs8EBZeEZzGtEK9Gdz6ow'
    AND 
        succeeded 
{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2023-06-06' -- first tx with v5
{% endif %}
),
base_decoded as (
    select 
        *
    from 
        {{ ref('silver__decoded_instructions') }}
    WHERE
        program_id = 'JUP5cHjnnCx2DppVsufsLrXs8EBZeEZzGtEK9Gdz6ow'
    
{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2023-06-06' -- first tx with v5
{% endif %}
),
base_transfers as (
    SELECT
        *,
        split(index,'.') as split_index,
        concat_ws('.',split_index[0],lpad(split_index[1],2,'0')) as padded_index
    FROM
        {{ ref('silver__transfers') }}
    WHERE
        succeeded
{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2023-06-06' -- first tx with v5
{% endif %}
),
base_token_mint_actions as (
    SELECT
        tma.*,
        ma.token_account
    FROM
        {{ ref('silver__mint_actions') }} ma
    join {{ ref('silver__token_mint_actions') }} tma
        on ma.block_timestamp::date = tma.block_timestamp::date
        and ma.tx_id = tma.tx_id
        and ma.index = tma.index
        and ma.inner_index = tma.inner_index
    WHERE
        ma.succeeded
    and tma.event_type IN ('mintToChecked', 'mintTo')

{% if is_incremental() %}
AND ma._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND ma.block_timestamp :: DATE >= '2023-06-06' -- first tx with v5
{% endif %}
),
pre_final as (
    select
        e.block_id,
        e.block_timestamp,
        e.tx_id,
        e.index,
        e.program_id,
        e.succeeded,
        silver.udf_get_account_pubkey_by_name('userTransferAuthority', d.decoded_instruction:accounts) as swapper,
        silver.udf_get_account_pubkey_by_name('destinationTokenAccount', d.decoded_instruction:accounts) as dest_token_account,
        silver.udf_get_account_pubkey_by_name('destinationMint', d.decoded_instruction:accounts) as destination_mint,
        iff(OBJECT_KEYS(decoded_instruction:args:routePlan:"0":swap)[0]::string = 'heliumTreasuryManagementRedeemV0', true, false) as is_helium_redeem,
        iff(OBJECT_KEYS(decoded_instruction:args:routePlan:"0":swap)[0]::string = 'stakeDexSwapViaStake', true, false)as is_stake_dex_swap,
        iff(OBJECT_KEYS(decoded_instruction:args:routePlan:"0":swap)[0]::string = 'saberAddDecimalsWithdraw', true, false)as is_saber_withdraw,
        decoded_instruction:args:inAmount::number as args_in_amount,
        e._inserted_timestamp
    from base_events e
    left outer join base_decoded d
        on e.block_timestamp::date = d.block_timestamp::date 
        and e.tx_id = d.tx_id
        and e.index = d.index
    where decoded_instruction:error::string is null /* a few non-swap instructions resulted in errors in idl parsing */
    and swapper is not null /* eliminate non-swap instructions */
    and not is_saber_withdraw
),
swaps_using_burns as (
    select 
        tx_id,
        index,
        iff(is_helium_redeem,'iotEVVZLEywoTn1QdwNPddxPWszn3zFhEot3MfL9fns','7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj') as mint,
        args_in_amount * pow(10,iff(is_helium_redeem,-6,-9)) as amount
    from pre_final
    where 
        (is_helium_redeem or is_stake_dex_swap)
),
source_transfers as (
    select
        pf.block_id,
        pf.block_timestamp,
        pf.tx_id,
        pf.index,
        pf.program_id,
        tr.source_token_account,
        max(tr.mint) as mint,
        sum(tr.amount) as amount,
        min(padded_index) as tr_index
    from 
        pre_final pf
    left outer join base_transfers tr 
        on pf.block_timestamp::date = tr.block_timestamp::date 
        and pf.tx_id = tr.tx_id
        and pf.index = split(tr.index,'.')[0]::number
        and pf.swapper = tr.tx_from  
    group by 1,2,3,4,5,6
    qualify(row_number() over (partition by pf.tx_id, pf.index order by tr_index)) = 1
),
find_null_source_mints as (
    select 
        st.tx_id,
        st.index,
        tr.mint
    from source_transfers st
    join base_transfers tr on st.tx_id = tr.tx_id and (st.source_token_account = tr.tx_to or st.source_token_account = tr.tx_from) and tr.mint is not null
    where st.mint is null
),
dest_transfers as (
    select
        pf.block_id,
        pf.block_timestamp,
        pf.tx_id,
        pf.index,
        pf.program_id,
        pf.dest_token_account,
        tr.mint,
        sum(tr.amount) as amount
    from 
        pre_final pf
    left outer join base_transfers tr 
        on pf.block_timestamp::date = tr.block_timestamp::date 
        and pf.tx_id = tr.tx_id
        and pf.index = split(tr.index,'.')[0]::number
        and pf.dest_token_account = coalesce(tr.dest_token_account,tr.tx_to)
    group by 1,2,3,4,5,6,7
),
find_marinade_deposits as (
    select 
        dt.tx_id,
        dt.index,
        tma.mint,
        tma.mint_amount * pow(10,-tma.decimal) as amount
    from dest_transfers dt
    join base_token_mint_actions tma
        on dt.block_timestamp::date = tma.block_timestamp::date
        and dt.tx_id = tma.tx_id
        and dt.index = tma.index
        and dt.dest_token_account = tma.token_account
    where 
        tma.mint_amount is not null
    and 
        tma.decimal is not null
)
select 
    pf.block_id,
    pf.block_timestamp,
    pf.tx_id,
    pf.index as swap_index,
    pf.program_id,
    pf.succeeded,
    pf.swapper,
    coalesce(st.amount,burns.amount) as from_amt,
    coalesce(st.mint,burns.mint,nm.mint) as from_mint,
    coalesce(dt.amount,0) + coalesce(md.amount,0) as to_amt,
    coalesce(dt.mint,md.mint) as to_mint,
    pf._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['pf.tx_id','pf.index','pf.program_id']) }} as swaps_intermediate_jupiterv5_id,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
from pre_final pf
left outer join source_transfers st
    on pf.tx_id = st.tx_id
    and pf.index = st.index
left outer join dest_transfers dt
    on pf.tx_id = dt.tx_id
    and pf.index = dt.index
left outer join swaps_using_burns burns
    on pf.tx_id = burns.tx_id
    and pf.index = burns.index
left outer join find_null_source_mints nm 
    on pf.tx_id = nm.tx_id
    and pf.index = nm.index
left outer join find_marinade_deposits md 
    on pf.tx_id = md.tx_id
    and pf.index = md.index