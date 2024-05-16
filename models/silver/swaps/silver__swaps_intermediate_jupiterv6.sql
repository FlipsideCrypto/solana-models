 -- depends_on: {{ ref('silver__decoded_instructions_combined') }}

{{ config(
    materialized = 'incremental',
    unique_key = ['tx_id','swap_index','program_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    tags = ['scheduled_non_core'],
    post_hook = enable_search_optimization(
        '{{this.schema}}',
        '{{this.identifier}}',
        'ON EQUALITY(tx_id, swapper, from_mint, to_mint)'
    ),
) }}

{% if execute %}
    {% set base_query %}
        CREATE OR REPLACE TEMPORARY TABLE silver.swaps_intermediate_jupiterv6__intermediate_tmp AS 
        SELECT 
            *
        FROM 
            {{ ref('silver__decoded_instructions_combined') }}
        WHERE
            program_id = 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4'
        AND event_type IN ('exactOutRoute','sharedAccountsExactOutRoute','sharedAccountsRoute','routeWithTokenLedger','route','sharedAccountsRouteWithTokenLedger')
        AND succeeded
        {% if is_incremental() %}
        AND _inserted_timestamp::DATE >= '2024-05-16 04:00:00'
        {% else %} 
            AND _inserted_timestamp :: DATE >= '2023-09-14'
            AND _inserted_timestamp :: DATE < '2023-09-30'
        {% endif %}
    {% endset %}
    {% do run_query(base_query) %}
    {% set between_stmts = fsc_utils.dynamic_range_predicate("silver.swaps_intermediate_jupiterv6__intermediate_tmp","block_timestamp::date") %}
{% endif %}

WITH base_transfers as (
    SELECT
        *,
        split(index,'.') as split_index,
        concat_ws('.',split_index[0],lpad(split_index[1],2,'0')) as padded_index
    FROM
        {{ ref('silver__transfers') }}
    WHERE
        {{ between_stmts }}
    AND succeeded
),
base_token_mint_actions as (
    SELECT
        tma.*,
    FROM {{ ref('silver__token_mint_actions') }} tma
    WHERE
        {{ between_stmts }}
    AND tma.succeeded
    AND tma.event_type IN ('mintToChecked', 'mintTo')
),
pre_final as (
    select
        block_id,
        block_timestamp,
        tx_id,
        index,
        program_id,
        succeeded,
        silver.udf_get_account_pubkey_by_name('userTransferAuthority', decoded_instruction:accounts) as swapper,
        silver.udf_get_account_pubkey_by_name('userSourceTokenAccount', decoded_instruction:accounts) as user_source_token_account,
        silver.udf_get_account_pubkey_by_name('sourceTokenAccount', decoded_instruction:accounts) as source_token_account,
        silver.udf_get_account_pubkey_by_name('sourceMint', decoded_instruction:accounts) as source_mint,
        silver.udf_get_account_pubkey_by_name('destinationMint', decoded_instruction:accounts) as destination_mint,
        silver.udf_get_account_pubkey_by_name('userDestinationTokenAccount', decoded_instruction:accounts) as user_destination_token_account,
        silver.udf_get_account_pubkey_by_name('destinationTokenAccount', decoded_instruction:accounts) as destination_token_account,
        silver.udf_get_account_pubkey_by_name('programDestinationTokenAccount', decoded_instruction:accounts) as program_destination_token_account,
        silver.udf_get_account_pubkey_by_name('programSourceTokenAccount', decoded_instruction:accounts) as program_source_token_account,
        iff(OBJECT_KEYS(decoded_instruction:args:routePlan:"0":swap)[0]::string = 'heliumTreasuryManagementRedeemV0', true, false) as is_helium_redeem,
        iff(OBJECT_KEYS(decoded_instruction:args:routePlan:"0":swap)[0]::string = 'stakeDexSwapViaStake', true, false)as is_stake_dex_swap,
        iff(OBJECT_KEYS(decoded_instruction:args:routePlan:"0":swap)[0]::string = 'perpsRemoveLiquidity', true, false)as is_perps_remove_liquidity,
        iff(OBJECT_KEYS(decoded_instruction:args:routePlan:"0":swap)[0]::string = 'saberAddDecimalsWithdraw', true, false)as is_saber_withdraw,
        iff(OBJECT_KEYS(decoded_instruction:args:routePlan:"0":swap)[0]::string = 'phoenix', true, false)as is_phoenix_swap,
        iff(OBJECT_KEYS(decoded_instruction:args:routePlan:"0":swap)[0]::string = 'stakeDexPrefundWithdrawStakeAndDepositStake', true, false) as is_sanctum_burn_route,
        iff(is_sanctum_burn_route, decoded_instruction:accounts[13]:pubkey::string, NULL) as sanctum_burn_mint,
        decoded_instruction:args:inAmount::number as args_in_amount,
        _inserted_timestamp
    from silver.swaps_intermediate_jupiterv6__intermediate_tmp d
    where decoded_instruction:error::string is null /* a few non-swap instructions resulted in errors in idl parsing */
    and swapper is not null /* eliminate non-swap instructions */
    and not is_saber_withdraw
),
swaps_using_burns as (
    select 
        tx_id,
        index,
        case
            when is_helium_redeem then 'iotEVVZLEywoTn1QdwNPddxPWszn3zFhEot3MfL9fns'
            when is_perps_remove_liquidity then '27G8MtK7VtTcCHkpASjSDdkWWYfoqT6ggEuKidVJidD4'
            when is_sanctum_burn_route then sanctum_burn_mint
            else '7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj'
        end as mint,
        case 
            when is_helium_redeem or is_perps_remove_liquidity then args_in_amount * pow(10,-6)
            else args_in_amount * pow(10,-9)
        end as amount
    from pre_final
    where 
        (is_helium_redeem or is_stake_dex_swap or is_perps_remove_liquidity or is_sanctum_burn_route)
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
        and coalesce(pf.user_source_token_account,pf.source_token_account) = tr.source_token_account 
        and (
                tr.mint is not null 
                or 
                (
                    tr.mint is NULL 
                    and tr.source_token_account <> tr.dest_token_account
                )
            )
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
    group by 1,2,3
),
dest_transfers as (
    select
        pf.block_id,
        pf.block_timestamp,
        pf.tx_id,
        pf.index,
        pf.program_id,
        coalesce(pf.user_destination_token_account,pf.destination_token_account) as destination_token_account,
        tr.mint,
        sum(tr.amount) as amount
    from 
        pre_final pf
    left outer join base_transfers tr 
        on pf.block_timestamp::date = tr.block_timestamp::date 
        and pf.tx_id = tr.tx_id
        and pf.index = split(tr.index,'.')[0]::number
        and coalesce(pf.user_destination_token_account,pf.destination_token_account) = coalesce(tr.dest_token_account,tr.tx_to)
        and (
                tr.mint is not null 
                or 
                (
                    tr.mint is NULL 
                    and tr.source_token_account <> tr.dest_token_account
                )
            )
    group by 1,2,3,4,5,6,7
),
last_transfers as (
    select
        pf.block_id,
        pf.block_timestamp,
        pf.tx_id,
        pf.index,
        pf.program_id,
        tr.dest_token_account as destination_token_account,
        tr.mint,
        tr.amount as amount
    from 
        pre_final pf
    left outer join base_transfers tr 
        on pf.block_timestamp::date = tr.block_timestamp::date 
        and pf.tx_id = tr.tx_id
        and pf.index = split(tr.index,'.')[0]::number
        and pf.swapper = tx_to
    qualify row_number() over (partition by pf.tx_id, pf.index order by tr.padded_index desc) = 1
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
        and dt.destination_token_account = tma.token_account
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
    case 
        when coalesce(dt.amount,0) + coalesce(md.amount,0) = 0 then coalesce(lt.amount, 0)
        else coalesce(dt.amount,0) + coalesce(md.amount,0)
    end as to_amt,
    case 
        when coalesce(dt.mint,md.mint) is null then lt.mint
        else coalesce(dt.mint,md.mint)
    end as to_mint,
    pf._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['pf.tx_id','pf.index','pf.program_id']) }} as swaps_intermediate_jupiterv6_id,
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
left outer join last_transfers lt
    on pf.tx_id = lt.tx_id
    and pf.index = lt.index
where 
    /* remove small amount of edge cases where source transfer is not there or is in another index */
    (is_phoenix_swap and from_mint is not null) 
    OR not is_phoenix_swap