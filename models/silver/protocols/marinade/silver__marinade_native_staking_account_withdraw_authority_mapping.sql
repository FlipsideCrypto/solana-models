{{
    config(
        materialized = 'table',
    )
}}

/* 
remap withdraw authority using recursion to first parent 
if stake authority AND withdraw authorityis stWirqFCf2Uts1JBL1Jsd3r6VBWhgnpdPxCTe1MFjrq 
*/
with first_parents AS (
    select distinct
        stake_account AS child_stake_account,
        stake_account AS parent_stake_account,
        withdraw_authority,
        1 AS recursion_depth  -- Start recursion depth from 1
    from 
        {{ ref('silver__staking_lp_actions_labeled_2') }}
    where stake_authority = 'stWirqFCf2Uts1JBL1Jsd3r6VBWhgnpdPxCTe1MFjrq'
        and withdraw_authority <> 'stWirqFCf2Uts1JBL1Jsd3r6VBWhgnpdPxCTe1MFjrq'
        and withdraw_authority is not null
        and succeeded
    qualify
        row_number() over (partition by stake_account order by block_timestamp) = 1
),
children AS (
    select distinct
        children.stake_account,
        children.parent_stake_account
    from
        {{ ref('silver__staking_lp_actions_labeled_2') }} AS children
    left join
        first_parents
        on first_parents.parent_stake_account = children.stake_account
    where
        first_parents.parent_stake_account IS NULL
        AND children.parent_stake_account IS NOT NULL
        AND stake_authority = 'stWirqFCf2Uts1JBL1Jsd3r6VBWhgnpdPxCTe1MFjrq'
),
ancestry AS (
    select 
        child_stake_account,
        parent_stake_account,
        recursion_depth
    from first_parents
    union all
    select 
        children.stake_account,
        ancestry.parent_stake_account,
        ancestry.recursion_depth + 1 AS recursion_depth
    from ancestry
    join
        children
        on children.parent_stake_account = ancestry.child_stake_account
    where
        ancestry.recursion_depth < 10
),
child_with_first_parent AS (
    select distinct 
        child_stake_account AS stake_account, 
        parent_stake_account
    from ancestry
),
child_with_withdraw_authority AS (
    select
        c.stake_account,
        fp.withdraw_authority
    from child_with_first_parent as c
    left join
        first_parents as fp
        ON fp.parent_stake_account = c.parent_stake_account
    group by 1,2
)
select 
    stake_account,
    withdraw_authority,
    {{ dbt_utils.generate_surrogate_key(['stake_account']) }} AS marinade_native_staking_account_withdraw_authority_mapping_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
from 
    child_with_withdraw_authority
